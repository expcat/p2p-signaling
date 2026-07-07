export interface Env {
  ROOMS: DurableObjectNamespace;
}

type ClientRole = "host" | "guest";

type SignalEnvelope =
  | { type: "hello"; role: ClientRole; roomCode?: string }
  | { type: "signal"; payload: unknown }
  | { type: "chat"; text: string }
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

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);

    if (url.pathname === "/health") {
      return json({ ok: true });
    }

    if (url.pathname === "/rooms" && request.method === "POST") {
      const roomCode = createRoomCode();
      return json({ roomCode }, { status: 201 });
    }

    if (url.pathname.startsWith("/rooms/")) {
      const [, , roomCode] = url.pathname.split("/");
      if (!roomCode) {
        return json({ error: "missing room code" }, { status: 400 });
      }

      return getRoomStub(env, roomCode).fetch(request);
    }

    return json({ error: "not found" }, { status: 404 });
  }
};

export class RoomObject {
  private sessions = new Map<WebSocket, ClientRole>();

  constructor(private readonly state: DurableObjectState, private readonly env: Env) {
    void this.state;
    void this.env;
  }

  async fetch(request: Request): Promise<Response> {
    if (request.headers.get("upgrade")?.toLowerCase() !== "websocket") {
      return json({ error: "expected websocket upgrade" }, { status: 426 });
    }

    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);

    server.accept();
    this.sessions.set(server, "guest");

    server.addEventListener("message", (event) => this.handleMessage(server, event.data));
    server.addEventListener("close", () => this.disconnect(server));
    server.addEventListener("error", () => this.disconnect(server));

    server.send(JSON.stringify({ type: "room-ready" }));

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

    if (envelope.type === "hello") {
      for (const [peer, role] of this.sessions.entries()) {
        if (peer !== sender) {
          sender.send(JSON.stringify({ type: "peer-joined", role }));
        }
      }

      this.sessions.set(sender, envelope.role);
      this.broadcast(sender, { type: "peer-joined", role: envelope.role });
      return;
    }

    if (envelope.type === "bye") {
      this.disconnect(sender);
      return;
    }

    this.broadcast(sender, envelope);
  }

  private broadcast(sender: WebSocket, message: unknown): void {
    const payload = JSON.stringify(message);

    for (const peer of this.sessions.keys()) {
      if (peer !== sender) {
        peer.send(payload);
      }
    }
  }

  private disconnect(socket: WebSocket): void {
    const role = this.sessions.get(socket);
    this.sessions.delete(socket);

    if (role) {
      this.broadcast(socket, { type: "peer-left", role });
    }

    try {
      socket.close();
    } catch {
      // The socket may already be closed by the runtime.
    }
  }
}
