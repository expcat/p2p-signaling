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

## Client build and start scripts

macOS:

```sh
./scripts/build-client-macos.sh
./scripts/start-client-macos.sh --server p2p-signaling.yizhe.studio --room TESTROOM --role host
```

`scripts/start-client-macos.command` is also available for launching from Finder.

Windows PowerShell:

```powershell
.\scripts\build-client-windows.ps1
.\scripts\start-client-windows.ps1 -Server p2p-signaling.yizhe.studio -Room TESTROOM -Role host
```

Windows cmd:

```bat
scripts\build-client-windows.cmd
scripts\start-client-windows.cmd -Server p2p-signaling.yizhe.studio -Room TESTROOM -Role host
```

The client also reads these optional environment variables:

- `P2P_SIGNALING_SERVER`
- `P2P_SIGNALING_ROOM`
- `P2P_SIGNALING_ROLE`
