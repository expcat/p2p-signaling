# p2p-signaling

P2P chat/signaling playground with a Cloudflare Worker signaling service and a Rust client workspace.

## Layout

- `worker/`: Cloudflare Workers TypeScript signaling service.
- `clients/p2p-core/`: Rust core library for signaling, chat protocol, and session state.
- `clients/p2p-gui/`: GUI-facing crate. It currently runs as a CLI shell until a concrete GUI framework is chosen.

## Local checks

```sh
cd clients
cargo check --workspace
```

For the Worker:

```sh
cd worker
npm install
npm run typecheck
```

