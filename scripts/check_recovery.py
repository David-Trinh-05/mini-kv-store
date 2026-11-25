"""Exercise process locking, recovery and compaction through the Linux CLI."""

import fcntl
import json
import os
import select
import signal
import struct
import subprocess
import sys
import tempfile
import zlib
from pathlib import Path


def stage_partial_batch(db):
    changes = [
        {"op": "set", "key": "mode", "value": "unsafe"},
        {"op": "delete", "key": "serial"},
    ]
    # Independent encoder for just the first 17 bytes of a valid frame.
    payload = json.dumps(changes, separators=(",", ":")).encode()
    length = struct.pack("<I", len(payload))
    partial = length + struct.pack("<I", zlib.crc32(length)) + payload[:9]
    with (db / ".lock").open("r+b") as lock, (db / "wal").open("ab", buffering=0) as wal:
        fcntl.flock(lock, fcntl.LOCK_EX)
        assert wal.write(partial) == len(partial)
        os.fsync(wal.fileno())
        print("partial frame staged", flush=True)
        signal.pause()


def check(binary, db):
    def run(*args, code=0, data=None):
        result = subprocess.run(
            [binary, "--db", db, *args],
            input=data, text=True, capture_output=True, timeout=10,
        )
        assert result.returncode == code, (args, result.returncode, result.stderr)
        return result

    run("set", "serial", "rover-01")
    run("set", "mode", "safe")
    run("set", "temporary", "discard")
    run("delete", "temporary")
    run("get", "temporary", code=1)
    expected = {"mode": "safe", "serial": "rover-01"}
    assert json.loads(run("list").stdout) == expected

    with subprocess.Popen(
        [sys.executable, __file__, "--stage-partial", db],
        stdout=subprocess.PIPE, text=True,
    ) as writer:
        try:
            assert select.select([writer.stdout], [], [], 5)[0], "partial writer timed out"
            assert writer.stdout.readline().strip() == "partial frame staged"
            assert "database is locked" in run("stats", code=3).stderr
        finally:
            if writer.poll() is None:
                writer.kill()
        assert writer.wait(timeout=5) == -signal.SIGKILL

    recovered = run("stats")
    stats = json.loads(recovered.stdout)
    assert stats["recovered_bytes"] == 17 and stats["frames"] == 4
    assert "recovered 17 incomplete trailing bytes" in recovered.stderr
    assert json.loads(run("list").stdout) == expected
    print("Lock contention returned exit 3. After SIGKILL, recovered 17 trailing bytes.")

    changes = [{"op": "set", "key": "rpm", "value": str(rpm)} for rpm in range(512)]
    run("apply", "-", data=json.dumps(changes))
    expected["rpm"] = "511"
    assert json.loads(run("list").stdout) == expected
    compacted = json.loads(run("compact").stdout)
    before, after = compacted["before_bytes"], compacted["after_bytes"]
    assert after < before
    assert json.loads(run("list").stdout) == expected
    print(f"Compaction: {before} -> {after} WAL bytes, all {len(expected)} live keys verified.")

    log = db / "wal"
    corrupted = bytearray(log.read_bytes())
    corrupted[16] ^= 1  # First payload byte, after the file and frame headers.
    log.write_bytes(corrupted)
    assert "payload CRC mismatch" in run("stats", code=4).stderr
    assert log.read_bytes() == corrupted
    print("A complete frame with a bad CRC was rejected. WAL bytes were preserved.")


if __name__ == "__main__":
    if len(sys.argv) == 3 and sys.argv[1] == "--stage-partial":
        stage_partial_batch(Path(sys.argv[2]))
    else:
        default = Path(__file__).resolve().parents[1] / "target/debug/mini-kv-store"
        binary = Path(sys.argv[1]).resolve() if len(sys.argv) == 2 else default
        with tempfile.TemporaryDirectory() as temporary:
            check(binary, Path(temporary) / "db")
