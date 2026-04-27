#!/usr/bin/env python3
# Test that the configurable chunk_delay in serverThreadConfig.cfg
# is actually throttling chunk transfers.
#
# Spins up peer2's server (delay=0.005), requests N chunks,
# and verifies elapsed time >= N * chunk_delay.

import subprocess, socket, time, os, sys, signal

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PEER2_PORT = 4002
CHUNK_DELAY = 0.005  # must match peer2/serverThreadConfig.cfg line 3
N_CHUNKS = 10        # how many sequential chunks to request

processes = []


def cleanup(*args):
    for p in processes:
        try:
            p.terminate()
            p.wait(timeout=2)
        except Exception:
            p.kill()


signal.signal(signal.SIGINT, cleanup)
signal.signal(signal.SIGTERM, cleanup)


def request_chunk(port, filename, start, end):
    s = socket.socket()
    s.settimeout(5)
    s.connect(("127.0.0.1", port))
    s.sendall(f"GET {filename} {start} {end}\n".encode())
    data = b""
    while len(data) < (end - start):
        part = s.recv(end - start - len(data))
        if not part:
            break
        data += part
    s.close()
    return data


def check(label, passed, detail=""):
    status = "PASS" if passed else "FAIL"
    suffix = f"  ({detail})" if detail else ""
    print(f"  [{status}] {label}{suffix}")
    return passed


def main():
    print(f"\n{'='*60}")
    print(f"  Chunk Delay Test  (expected delay: {CHUNK_DELAY}s per chunk)")
    print(f"{'='*60}\n")

    # start peer2 server (serverThreadConfig.cfg has chunk_delay=0.005)
    peer2_proc = subprocess.Popen(
        [sys.executable, "peer.py"],
        cwd=os.path.join(BASE_DIR, "peer2"),
        stdin=subprocess.PIPE,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    processes.append(peer2_proc)
    time.sleep(1.5)

    largefile = os.path.join(BASE_DIR, "peer2", "shared", "largefile.bin")
    filesize = os.path.getsize(largefile)
    print(f"  largefile.bin: {filesize} bytes")
    print(f"  Requesting {N_CHUNKS} sequential chunks from peer2 (port {PEER2_PORT})...\n")

    t_start = time.time()
    received = 0
    for i in range(N_CHUNKS):
        chunk_start = i * 1024
        chunk_end = min(chunk_start + 1024, filesize)
        data = request_chunk(PEER2_PORT, "largefile.bin", chunk_start, chunk_end)
        received += len(data)
        print(f"  chunk {i+1:02d}: bytes {chunk_start}-{chunk_end}  got {len(data)} bytes")

    elapsed = time.time() - t_start
    expected_min = N_CHUNKS * CHUNK_DELAY

    print(f"\n  Elapsed:  {elapsed:.3f}s")
    print(f"  Expected minimum: {expected_min:.3f}s  ({N_CHUNKS} chunks × {CHUNK_DELAY}s)")

    all_pass = True
    all_pass &= check("all chunks received", received == N_CHUNKS * 1024,
                      f"got {received}, want {N_CHUNKS * 1024}")
    all_pass &= check(f"elapsed >= {expected_min:.3f}s (delay applied)", elapsed >= expected_min,
                      f"elapsed={elapsed:.3f}s")
    all_pass &= check("elapsed < 5s (not deadlocked)", elapsed < 5.0,
                      f"elapsed={elapsed:.3f}s")

    print()
    print(f"  {'ALL PASSED' if all_pass else 'SOME FAILED'}")
    print()

    cleanup()
    sys.exit(0 if all_pass else 1)


if __name__ == "__main__":
    main()
