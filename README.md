# mini-kv-store

A local key-value store in Rust. It commits batches to a checksummed
write-ahead log (WAL), rebuilds an in-memory index on open, recovers from a
short final frame, and compacts the log by replacing it with a snapshot.

## Demo

The Linux smoke test checks exclusive locking across processes, kills a
helper after it writes part of a batch, then opens the database through the CLI.
The demo also compacts 512 updates to one key and verifies that all three
live keys survive. It uses a temporary directory.

```sh
cargo build --locked
python3 scripts/check_recovery.py
```

```text
Lock contention returned exit 3. After SIGKILL, recovered 17 trailing bytes.
Compaction: 20102 -> 148 WAL bytes, all 3 live keys verified.
A complete frame with a bad CRC was rejected. WAL bytes were preserved.
```

## Use the CLI

Requires Rust 1.89 or later and a Linux filesystem with advisory locking and
directory sync. Each command acquires an exclusive lock and opens the WAL.
The database directory is created if its parent exists. Use a dedicated
directory and let the store manage its `wal`, `.lock`, and `.wal.next` files.

```sh
db=$(mktemp -d)
kv=target/debug/mini-kv-store
"$kv" --db "$db" set mode safe
"$kv" --db "$db" get mode
printf '%s\n' '[{"op":"set","key":"rpm","value":"1200"},{"op":"delete","key":"mode"}]' |
  "$kv" --db "$db" apply -
"$kv" --db "$db" list
"$kv" --db "$db" stats
"$kv" --db "$db" compact
```

`apply FILE` accepts a JSON array from a file or from stdin with `apply -`.
A batch contains 1–1,024 mutations, with an encoded payload of at most
1 MiB. The CLI reads up to 2 MiB of JSON input before opening the store.
Keys are 1–1,024 UTF-8 bytes. Values may be empty and can be at most 64 KiB
of UTF-8. Operations in a batch run in order, so the same key may appear
more than once. Deleting a missing key succeeds.

| Exit | Meaning |
| ---: | --- |
| 0 | Success |
| 1 | `get` found no value |
| 2 | Invalid command-line arguments |
| 3 | Database lock held by another handle |
| 4 | I/O, WAL corruption, or invalid mutation |

## File format

The 8-byte file header is `4D 49 4E 49 4B 56 01 0A`. The final two bytes
are the format version and newline. Each frame describes one batch.

| Bytes | Contents |
| ---: | --- |
| 4 | Payload length as a little-endian `u32` |
| 4 | IEEE CRC-32 of the four length bytes, little-endian |
| N | UTF-8 JSON mutation array |
| 4 | IEEE CRC-32 of the payload, little-endian |

The length checksum lets recovery distinguish a truncated payload from a
damaged length field. A short final header or a valid header with too few
remaining bytes is discarded from the last verified offset. A complete
frame with a bad checksum, invalid length, or invalid payload causes open to
fail and preserves the WAL for inspection. [CRC implementation](https://docs.rs/crc32fast/1/crc32fast/)

## Write and recovery guarantees

- A batch is validated before any append. The store appends one frame,
  calls `sync_all`, then updates the in-memory index and reports success.
  A complete frame can survive an interrupted call whose result the caller
  never received. After an I/O failure during a write, reopen the store to
  learn which changes survived. [Linux fsync documentation](https://man7.org/linux/man-pages/man2/fsync.2.html)
- The lock uses a dedicated file, so it stays held while compaction replaces
  the WAL inode. The store writes live keys to a staged snapshot, syncs it,
  renames it over the WAL, then syncs the database directory. If the snapshot
  would take as much space as the original or more, the original is retained.
  On reopen, any leftover staging file is disposable. [Rust file locking](https://doc.rust-lang.org/std/fs/struct.File.html#method.try_lock) · [Linux rename semantics](https://man7.org/linux/man-pages/man2/rename.2.html)
- Replay reads one frame at a time. Memory grows with the live key and
  value set, and startup time grows with the WAL. Every CLI invocation pays
  this replay cost. Durability relies on the filesystem and storage
  honoring sync calls. The demo exercises process interruption on Linux.

## Verify

```sh
cargo fmt --all -- --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked --all-targets
```

The Rust tests truncate a two-mutation batch at every incomplete byte offset,
then reopen and check that neither mutation leaked. They also check corrupted
length and payload fields, lock ownership through compaction, and compaction
at both frame size limits.

On Linux, you can also interrupt the Rust process directly with `strace`.
The third `write` in this command is the first payload write, after the
length and its checksum have been written.

```sh
db=$(mktemp -d)
kv=target/debug/mini-kv-store
"$kv" --db "$db" set mode safe
printf '%s\n' '[{"op":"set","key":"mode","value":"changed"},{"op":"set","key":"new","value":"pending"}]' |
  strace -qq -e 'inject=write:error=EIO:signal=SIGKILL:when=3' "$kv" --db "$db" apply -
"$kv" --db "$db" stats
"$kv" --db "$db" list
```

The next open reports `recovered_bytes: 8`. Listing still returns
`{"mode":"safe"}`.
