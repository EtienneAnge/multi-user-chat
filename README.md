# Multi-User Chat

A simple command-line chat for several users, written in **Rust** with **Tokio**. It checks message integrity with a chain of hashes.

> **Practice project.** I made it to learn Rust, async programming (Tokio, actors, `mpsc` / `broadcast` / `oneshot` channels) and TCP sockets. It is not made for production: there is no encryption, and no authentication (see below).

## Project structure

```
multi-user-chat/
├── server/   # TCP server: receives, checks and broadcasts messages
├── client/   # Terminal client: asks for a nickname and sends messages
└── common/   # Shared code: Message struct amd  history actor
```

## Installation

You need [Rust](https://rustup.rs/)

```bash
git clone <repo-url>
cd multi-user-chat
cargo build --release
```

## Usage

1. **Start the server** (it listens on `127.0.0.1:9090`):
   ```bash
   cargo run -p server
   ```
2. **Start one or more clients**, each one in its own terminal:
   ```bash
   cargo run -p client
   ```
3. Type a **nickname**, then type your messages and press `Enter`.

When a client connects, it asks the server for the history and shows it. Every valid message is then sent to all connected clients, with a status:
- `Message integrity verified` (green): the message is valid.
- `Message invalide` (red): the message was rejected.

## Message integrity with hashes

Each `Message` has a `prev_hash` field: the **SHA-256 hash of the previous message** in the history. So the messages form a **chain** (like a very simple blockchain).

```
[Msg 1]                      [Msg 2]                     [Msg 3]
prev_hash = GENESIS_HASH <-- prev_hash = H(Msg 1) <-- prev_hash = H(Msg 2)
```

`GENESIS_HASH` is 64 zeros, used for the first message. `H(msg)` is the SHA-256 of the message serialized  in JSON.

How it works:

1. **Creating a message**: the client computes the hash of the last message in its local history (or `GENESIS_HASH` if the history is empty) and puts it in `prev_hash`.
2. **Check on the server**: the history actor (`common/src/history.rs`) computes the hash of the last message it knows and compares it with the received `prev_hash`:
   - if they are equal, the message is added to the history and sent to everyone;
   - if not, the message is rejected.
3. **Check on the client**: each client does the same check on every new message it receives from the server.
4. **Check of the full history**: when a client connects and gets the history from the server, it checks every link of the chain (`history_integrity`). If one link is broken, the history is refused and the client shows `Received history is not valid`.

The hash uses **all the fields** of the message (`id`, `pseudo`, `time`, `content`, `prev_hash`). If a message in the history is changed, removed or moved, its hash changes and the chain breaks. A message built from an old history (for example, two clients writing at the same time) is also rejected.

### Limit (this is a practice project)

- The chain finds changes and inconsistencies, but it does not stop an attacker (or the server) from recomputing the whole chain: there are no signatures and no authentication.
- No encryption: messages are sent in plain JSON over TCP.
- No persistence: the history is lost when the server stops.
