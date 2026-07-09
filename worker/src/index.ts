export interface Env {
  ROOMS: DurableObjectNamespace;
}

type ClientRole = "host" | "guest";

type SignalEnvelope =
  | { type: "hello"; role: ClientRole; roomCode?: string }
  | { type: "signal"; payload: unknown }
  | { type: "bye" };

function json(data: unknown, init: ResponseInit = {}): Response {
  return new Response(JSON.stringify(data), {
    ...init,
    headers: {
      "content-type": "application/json; charset=utf-8",
      ...init.headers
    }
  });
}

function createRoomCode(): string {
  const values = new Uint16Array(1);
  crypto.getRandomValues(values);
  return String(values[0] % 10000).padStart(4, "0");
}

function getRoomStub(env: Env, roomCode: string): DurableObjectStub {
  const id = env.ROOMS.idFromName(roomCode);
  return env.ROOMS.get(id);
}

const CREATE_ROOM_ATTEMPTS = 5;

function forwardToRoom(
  env: Env,
  request: Request,
  roomCode: string,
  params: Record<string, string>
): Promise<Response> {
  const target = new URL(request.url);
  target.pathname = `/rooms/${roomCode}`;
  target.searchParams.set("code", roomCode);
  for (const [key, value] of Object.entries(params)) {
    target.searchParams.set(key, value);
  }
  return getRoomStub(env, roomCode).fetch(new Request(target, request));
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);

    if (url.pathname === "/health") {
      return json({ ok: true });
    }

    if (url.pathname === "/rooms/new") {
      if (request.headers.get("upgrade")?.toLowerCase() !== "websocket") {
        return json({ error: "expected websocket upgrade" }, { status: 426 });
      }

      for (let attempt = 0; attempt < CREATE_ROOM_ATTEMPTS; attempt += 1) {
        const roomCode = createRoomCode();
        const response = await forwardToRoom(env, request, roomCode, {
          role: "host",
          create: "1"
        });

        // 409 = 房间码已被占用，换码重试
        if (response.status !== 409) {
          return response;
        }
      }

      return json({ error: "no room code available" }, { status: 503 });
    }

    if (url.pathname.startsWith("/rooms/")) {
      const [, , roomCode] = url.pathname.split("/");
      if (!roomCode) {
        return json({ error: "missing room code" }, { status: 400 });
      }

      return forwardToRoom(env, request, roomCode, {});
    }

    return json({ error: "not found" }, { status: 404 });
  }
};

const MAX_PEERS_PER_ROOM = 2;

export class RoomObject {
  private sessions = new Map<WebSocket, ClientRole>();
  private hostPresent = false;

  constructor(private readonly state: DurableObjectState, private readonly env: Env) {
    void this.state;
    void this.env;
  }

  async fetch(request: Request): Promise<Response> {
    if (request.headers.get("upgrade")?.toLowerCase() !== "websocket") {
      return json({ error: "expected websocket upgrade" }, { status: 426 });
    }

    const url = new URL(request.url);
    const create = url.searchParams.get("create") === "1";
    const roomCode = url.searchParams.get("code") ?? "";
    // 只有经 /rooms/new 创建的连接才是房主，加入已有房间的一律是访客
    const role: ClientRole = create ? "host" : "guest";

    if (create) {
      if (this.hostPresent) {
        return json({ error: "room code taken" }, { status: 409 });
      }
    } else if (!this.hostPresent) {
      return json({ error: "room not found" }, { status: 404 });
    }

    if (this.sessions.size >= MAX_PEERS_PER_ROOM) {
      return json({ error: "room full" }, { status: 409 });
    }

    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);

    server.accept();
    this.sessions.set(server, role);
    if (role === "host") {
      this.hostPresent = true;
    }

    server.addEventListener("message", (event) => this.handleMessage(server, event.data));
    server.addEventListener("close", () => this.disconnect(server));
    server.addEventListener("error", () => this.disconnect(server));

    server.send(JSON.stringify({ type: "room-ready", roomCode }));

    return new Response(null, { status: 101, webSocket: client });
  }

  private handleMessage(sender: WebSocket, data: string | ArrayBuffer): void {
    if (typeof data !== "string") {
      sender.send(JSON.stringify({ type: "error", message: "binary messages are not supported" }));
      return;
    }

    let envelope: SignalEnvelope;
    try {
      envelope = JSON.parse(data) as SignalEnvelope;
    } catch {
      sender.send(JSON.stringify({ type: "error", message: "invalid json" }));
      return;
    }

    if (!this.isValidEnvelope(envelope)) {
      sender.send(JSON.stringify({ type: "error", message: "invalid signaling envelope" }));
      return;
    }

    if (envelope.type === "hello") {
      // 角色在连接时由 URL 决定，hello 仅用于触发 peer-joined 通知
      const senderRole = this.sessions.get(sender) ?? "guest";
      for (const [peer, role] of this.sessions.entries()) {
        if (peer !== sender) {
          sender.send(JSON.stringify({ type: "peer-joined", role }));
        }
      }

      this.broadcast(sender, { type: "peer-joined", role: senderRole });
      return;
    }

    if (envelope.type === "bye") {
      this.disconnect(sender);
      return;
    }

    this.broadcast(sender, envelope);
  }

  private isValidEnvelope(envelope: SignalEnvelope): boolean {
    if (!envelope || typeof envelope !== "object" || typeof envelope.type !== "string") {
      return false;
    }

    switch (envelope.type) {
      case "hello":
        return envelope.role === "host" || envelope.role === "guest";
      case "signal":
        return "payload" in envelope;
      case "bye":
        return true;
      default:
        return false;
    }
  }

  private broadcast(sender: WebSocket, message: unknown): void {
    const payload = JSON.stringify(message);

    for (const peer of this.sessions.keys()) {
      if (peer !== sender) {
        try {
          peer.send(payload);
        } catch {
          // The peer connection may already be gone; its close handler cleans up.
        }
      }
    }
  }

  private disconnect(socket: WebSocket): void {
    const role = this.sessions.get(socket);
    this.sessions.delete(socket);

    if (role) {
      this.broadcast(socket, { type: "peer-left", role });
    }

    // 房间随房主存活：房主离开即关房，踢出剩余访客；
    // 访客离开则保留房间，允许重连续传
    if (role === "host") {
      this.hostPresent = false;
      for (const peer of this.sessions.keys()) {
        this.sessions.delete(peer);
        try {
          peer.close(1000, "room closed");
        } catch {
          // The socket may already be closed by the runtime.
        }
      }
    }

    try {
      socket.close();
    } catch {
      // The socket may already be closed by the runtime.
    }
  }
}
