# Aero-Relay

A modular IBC relayer for the Cosmos ecosystem, built in Rust. Unlike monolithic relayers (Hermes, Go Relayer), Aero-Relay is a pipeline of independent services connected via TCP buffers. Each component can be restarted, replaced, or customized without touching the rest of the system.

Supports: **Neutron · CosmosHub · Osmosis · Noble**

---

## Architecture

```
                    ┌─────────────────────────────────┐
                    │  web  (gRPC proxy to nodes)      │
                    │  :22001 :22002 :22003 :22004     │
                    └────────────┬────────────────────┘
                                 │ gRPC (H/2)
                                 ▼
[poller] ──:22750──▶ [parser] ──:22751──▶ [filter] ──:22752──▶ [mapper] ──:22753──┐
  WS→TCP              parse                prefix +                channel→         │
                       events              incentivized             client_id        │
                                           filter                   (LRU cache)     │
                                                                                    │
                     ┌──────────────────────────────────────────────────────────────┘
                     │
                     ├──:22754──▶ [queryer]  — Merkle proofs (ABCI)
                     │                │
                     │                └──:22754──┐
                     │                           ▼
                     └──:22756──▶ [updater]  — MsgUpdateClient   ──▶ [batcher] ──:22755──▶ broadcast
                                   leader/follower                        │
                                   dedup                              [logger] :22666
```

### Port map

| Component   | Listens on | Reads from              |
|-------------|-----------|--------------------------|
| web         | :22001–22004 | external nodes (gRPC) |
| poller      | :22750    | WebSocket nodes          |
| parser      | :22751    | :22750                   |
| filter      | :22752    | :22751                   |
| mapper      | :22753    | :22752                   |
| queryer     | :22754    | :22753                   |
| updater     | :22756    | :22753                   |
| batcher     | :22755    | :22754, :22756           |
| logger      | :22666    | :22754, :22756           |

---

## Components

### web
gRPC reverse proxy. Maintains persistent H/2 connections to blockchain nodes and multiplexes requests from all pipeline components through a single connection pool per chain. Implements exponential backoff with jitter on reconnect.

### poller
Subscribes to `tm.event='Tx'` on each chain via WebSocket. Pre-filters at the wire level — only events containing `send_packet`, `write_acknowledgement`, or `incentivized_packet` are forwarded downstream.

### parser
Parses raw Tendermint events into structured JSON. Handles both `send_packet` (recv path) and `write_acknowledgement` (ack path). Includes a global dedup cache with 24h TTL to suppress duplicate events at the source.

### filter
Filters packets by address prefix and incentivized status. Configurable via `allowed_prefixes` in `Config.toml`. Stateless — trivially replaceable with custom routing logic.

### mapper
Resolves `channel_id → client_id` for each chain pair. Queries gRPC once per unknown channel, then caches in an LRU (16384 entries). The full map is persisted to `clients.json` every 10 minutes and on shutdown, so restarts don't require re-querying known channels.

### queryer
Fetches Merkle proofs via ABCI for each packet. Produces `msg_bytes` — the serialized `MsgRecvPacket` or `MsgAcknowledgement` — ready for the batcher.

### updater
Fetches light client headers (`MsgUpdateClient`) for each destination chain. Implements a **leader/follower** dedup pattern: when multiple packets arrive for the same client height simultaneously, only one goroutine fetches the header — others receive it via `oneshot::channel` and are marked `FOLLOW_LEADER`. This eliminates redundant RPC calls and duplicate update messages in the same batch.

### batcher
The execution core. Receives `MsgUpdateClient` from updater and `MsgRecvPacket`/`MsgAcknowledgement` from queryer, pairs them by `(client_id, type, sequence)`, and broadcasts as a single transaction.

Key behaviors:
- Gas simulation via REST before every broadcast
- Automatic gas adjustment on `out_of_gas` (code 11)
- Sequence resync on `sequence mismatch` (code 32)
- Skips redundant packets (code 22) without retrying
- Up to 20 broadcast attempts per transaction
- Per-chain workers, fully independent sequence management

### logger
Taps the output of queryer and updater, prints to stdout, and forwards data unchanged. Zero overhead on the hot path — can be removed from the pipeline without any other changes.

---

## Getting Started

### Prerequisites
- Rust stable (1.75+)
- Running gRPC endpoints for each supported chain (or use the included `web` proxy)

### Build

Each component is a separate Cargo workspace. Build individually:

```bash
cd batcher && cargo build --release
cd ../updater && cargo build --release
# ... repeat for each component
```

### Configuration

Every component reads its own `Config.toml` / `config.toml` from the working directory.

**Mandatory step — set your mnemonic in `batcher/config.toml`:**

```toml
relayer_mnemonic = "your twenty four word bip39 mnemonic phrase goes here replace this before running"
```

All other settings (ports, RPC URLs, chain IDs) can be left as defaults for the supported chains.

### Run order

Start components in dependency order:

```bash
# 1. gRPC proxy
cd web && ./target/release/web

# 2. Inbound pipeline
cd poller   && ./target/release/poller
cd parser_pack && ./target/release/parser_pack
cd filter   && ./target/release/filter
cd mapper   && ./target/release/mapper

# 3. Proof / header fetchers (can start in parallel)
cd queryer  && ./target/release/queryer
cd updater  && ./target/release/updater

# 4. Execution + logging
cd logger   && ./target/release/logger
cd batcher  && ./target/release/batcher
```

Each component reconnects automatically if its upstream goes down. Order matters only at initial startup.

---

## Design Notes

**Why TCP buffers instead of channels/queues?**
Each component is a separate process. TCP gives natural backpressure, allows independent restarts, and makes it trivial to insert custom components anywhere in the pipeline — just point `source_addr` and `listen_addr` to the right ports.

**Why independent Cargo workspaces per component?**
Each service can be updated, recompiled, and redeployed independently. A change in batcher's dependency tree has zero impact on poller. This is intentional — not a limitation.

**Why separate queryer and updater?**
Proof fetching (queryer) and header fetching (updater) have different latency profiles and failure modes. Decoupling them allows one to retry independently without blocking the other.

---

## Supported Chains

| Chain      | chain_id     | Prefix   |
|------------|-------------|----------|
| Neutron    | neutron-1   | neutron  |
| CosmosHub  | cosmoshub-4 | cosmos   |
| Osmosis    | osmosis-1   | osmo     |
| Noble      | noble-1     | noble    |

Adding a new chain requires entries in `web/config.toml`, `batcher/config.toml`, `updater/Config.toml`, and `queryer/Config.toml`.

---

## License

MIT
