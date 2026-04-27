#!/usr/bin/env python3
# final_demo.py — CS 4390 Final Demo
#
# Run via the Makefile:
#   make final-demo          # runs final-setup then this script
#   make final-setup         # setup only (generate files, write configs)
#   make clean               # wipe all generated files
#
# `make final-setup` (or `make final-demo`) handles:
#   - creating peer1/…peer13/ directories with shared/ and cache/ subdirs
#   - copying peer.py into each peer directory
#   - writing clientThreadConfig.cfg and serverThreadConfig.cfg for every peer
#   - generating peer1/shared/smallfile.txt and peer2/shared/largefile.bin
#   - clearing tracker/torrents/ so the server starts clean (spec requirement)
#
# This script handles everything that must happen at runtime:
#   - starting the tracker and seed peers
#   - driving createtracker for peer1 and peer2
#   - launching downloader peers in two waves at t=30s and t=90s
#   - terminating peer1 and peer2 at t=90s
#   - printing a final summary of downloaded files

import subprocess
import threading
import time
import socket
import os
import sys
import hashlib
import signal

# ─────────────────────────────────────────────────────────────────
# configuration — must match what Makefile final-setup writes
# ─────────────────────────────────────────────────────────────────

TRACKER_PORT   = 9090
TRACKER_IP     = "127.0.0.1"      # change to the tracker machine's IP when running across machines
BASE_PEER_PORT = 4000              # peer N listens on BASE_PEER_PORT + N (e.g. peer3 -> 4003)
BASE_DIR       = os.path.dirname(os.path.abspath(__file__))
TORRENTS_DIR   = os.path.join(BASE_DIR, "tracker", "torrents")

CHUNK_DELAY    = 0.5           # wait between sending chunks (to fix port exhaustion and make the download time 1 min 20 s)

SMALL_FILE     = "testfile.txt"     # shared by peer1 (matches Makefile final-setup)
LARGE_FILE     = "largefile.bin"    # shared by peer2 (matches Makefile final-setup)

_t0            = 0.0
processes: list[subprocess.Popen] = []
procs_lock     = threading.Lock()
shutdown_event = threading.Event()   # set by main once all downloads are done


def get_my_ip():
    # trick to get our actual ip without hardcoding it
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return "127.0.0.1"


MY_IP: str = ""   # resolved once in main() after config is confirmed


# ─────────────────────────────────────────────────────────────────
# logging helpers (kept from demo.py)
# ─────────────────────────────────────────────────────────────────

def ts() -> str:
    return f"t+{int(time.time() - _t0):>3}s"


def log(msg: str, prefix: str = "DEMO") -> None:
    print(f"\n{'='*60}", flush=True)
    print(f"  [{ts()}][{prefix}] {msg}", flush=True)
    print(f"{'='*60}", flush=True)


def step(msg: str) -> None:
    print(f"\n--- {msg} ---", flush=True)


# ─────────────────────────────────────────────────────────────────
# TCP helpers (kept from demo.py)
# ─────────────────────────────────────────────────────────────────

def send_to_tracker(message: str, label: str = "DEMO") -> str:
    """Send one protocol message to the tracker and return the full reply."""
    print(f"  {label}: >> {message}", flush=True)
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(10)
    sock.connect((TRACKER_IP, TRACKER_PORT))
    sock.sendall((message + "\n").encode())
    response = b""
    while True:
        try:
            chunk = sock.recv(4096)
            if not chunk:
                break
            response += chunk
        except socket.timeout:
            break
    sock.close()
    decoded = response.decode().strip()
    for line in decoded.split("\n"):
        print(f"  {label}: << {line}", flush=True)
    return decoded


def send_to_peer(port: int, message: str, label: str = "DEMO") -> bytes:
    """Send one message to a peer's server thread and return the raw bytes."""
    print(f"  {label}: >> peer:{port} {message}", flush=True)
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(15)
    sock.connect(("127.0.0.1", port))
    sock.sendall((message + "\n").encode())
    response = b""
    while True:
        try:
            chunk = sock.recv(4096)
            if not chunk:
                break
            response += chunk
        except socket.timeout:
            break
    sock.close()
    return response


def file_md5(filepath: str) -> str:
    h = hashlib.md5()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


# ─────────────────────────────────────────────────────────────────
# process management
# ─────────────────────────────────────────────────────────────────

def cleanup(*args) -> None:
    print("\n\nCleaning up all processes...", flush=True)
    with procs_lock:
        for p in processes:
            try:
                p.terminate()
                p.wait(timeout=2)
            except Exception:
                try:
                    p.kill()
                except Exception:
                    pass
    sys.exit(0)


signal.signal(signal.SIGINT, cleanup)


def drain(proc: subprocess.Popen, label: str) -> None:
    _noise = ("[SERVER]", "[UPDATE] Sent periodic")

    start_time = time.time()
    try:
        for line in proc.stdout:
            line = line.rstrip()

            # suppress startup noise window
            if time.time() - start_time < 2:
                continue

            if line and not any(n in line for n in _noise):
                print(f"  [{label}] {line}", flush=True)
    except Exception:
        pass


def start_proc(label: str, py_file: str, cwd: str,
               use_stdin: bool = True) -> subprocess.Popen:
    kwargs: dict = dict(
        cwd=cwd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    if use_stdin:
        kwargs["stdin"] = subprocess.PIPE
    proc = subprocess.Popen([sys.executable, py_file], **kwargs)
    threading.Thread(target=drain, args=(proc, label), daemon=True).start()
    with procs_lock:
        processes.append(proc)
    return proc


def terminate_peer(n: int, proc: subprocess.Popen) -> None:
    """Gracefully stop a peer then print the spec-required termination message."""
    try:
        proc.stdin.write("quit\n")
        proc.stdin.flush()
    except Exception:
        pass
    try:
        proc.wait(timeout=3)
    except subprocess.TimeoutExpired:
        proc.terminate()
        proc.wait()
    print(f"Peer{n} terminated", flush=True)


# ─────────────────────────────────────────────────────────────────
# downloader peer logic
# ─────────────────────────────────────────────────────────────────

def parse_tracker_content(content: str) -> tuple[int, list[dict]]:
    """Return (filesize, list-of-peer-dicts) from raw tracker file text."""
    filesize = 0
    peers: list[dict] = []
    skip_prefixes = ("Filename", "Description", "MD5")
    for line in content.split("\n"):
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("Filesize:"):
            try:
                filesize = int(line.split(":", 1)[1].strip())
            except ValueError:
                pass
        elif ":" in line and not line.startswith("<") \
                and not any(line.startswith(p) for p in skip_prefixes):
            parts = line.split(":")
            if len(parts) == 5:
                peers.append({
                    "ip":        parts[0],
                    "port":      int(parts[1]),
                    "start":     int(parts[2]),
                    "end":       int(parts[3]),
                    "timestamp": int(parts[4]),
                })
    return filesize, peers


def extract_tracker_body(response: str) -> str:
    """Strip <REP GET BEGIN> / <REP GET END …> envelope from a GET response."""
    body: list[str] = []
    in_block = False
    for line in response.split("\n"):
        if "<REP GET BEGIN>" in line:
            in_block = True
            continue
        if "<REP GET END" in line:
            in_block = False
            continue
        if in_block:
            body.append(line)
    return "\n".join(body)


def download_file_direct(n: int, filename: str, tracker_content: str) -> None:
    """
    Download a file chunk-by-chunk from the peers listed in tracker_content.
    One thread per 1024-byte chunk; retries each chunk across peers on failure.
    Saves the result to peer{n}/shared/filename and sends updatetracker on success.
    """
    label = f"Peer{n}"
    filesize, peers = parse_tracker_content(tracker_content)
    if not peers or filesize == 0:
        print(f"  [{label}] No peers or filesize for {filename}, skipping.", flush=True)
        return

    peers_sorted = sorted(peers, key=lambda p: p["timestamp"], reverse=True)
    peer_summary = ", ".join(f"{p['ip']}:{p['port']}" for p in peers_sorted)

    out_dir  = os.path.join(BASE_DIR, f"peer{n}", "shared")
    out_path = os.path.join(out_dir, filename)
    CHUNK    = 1024
    results: dict[int, bytes] = {}
    results_lock = threading.Lock()
    total_chunks = -(-filesize // CHUNK)   # ceiling division

    print(f"  [{label}] Starting download: {filename} ({filesize} bytes, "
          f"{total_chunks} chunks) from [{peer_summary}]", flush=True)
    start = time.time()
    
    def fetch_chunk(chunk_start: int, chunk_end: int) -> None:
        for peer in peers_sorted:
            try:
                # FIX: Use context manager to ensure socket is always closed
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                    sock.settimeout(15)
                    sock.connect((peer["ip"], peer["port"]))
                    sock.sendall(f"GET {filename} {chunk_start} {chunk_end}\n".encode())

                    data = b""
                    expected = chunk_end - chunk_start
                    while len(data) < expected:
                        part = sock.recv(expected - len(data))
                        if not part:
                            break
                        data += part

                if data:
                    with results_lock:
                        results[chunk_start] = data
                    return
            except Exception as e:
                print(
                    f"  [{label}] chunk {chunk_start}-{chunk_end} failed "
                    f"({peer['ip']}:{peer['port']}): {e}, trying next...",
                    flush=True
                )
        print(f"  [{label}] All peers exhausted for chunk {chunk_start}-{chunk_end}", flush=True)

    # FIX: Limit concurrent connections to prevent socket exhaustion (WinError 10055)
    # Semaphore wraps the function call (matches peer.py pattern)
    MAX_CONCURRENT = 2
    semaphore = threading.Semaphore(MAX_CONCURRENT)
    
    def throttled_fetch(chunk_start: int, chunk_end: int) -> None:
        with semaphore:
            fetch_chunk(chunk_start, chunk_end)

    # build chunk list and launch one thread per chunk
    chunks: list[tuple[int, int]] = []
    offset = 0
    while offset < filesize:
        chunk_end = min(offset + CHUNK, filesize)
        chunks.append((offset, chunk_end))
        offset = chunk_end

    threads = [threading.Thread(target=throttled_fetch, args=(s, e)) for s, e in chunks]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # write to disk, zero-filling any chunks that failed all peers
    with open(out_path, "wb") as f:
        for chunk_start, chunk_end in chunks:
            f.seek(chunk_start)
            f.write(results.get(chunk_start, b"\x00" * (chunk_end - chunk_start)))

    received = sum(len(v) for v in results.values())
    print(f"  [{label}] {filename}: saved {received}/{filesize} bytes -> {out_path}, took {time.time()-start} seconds", flush=True)

    if received == filesize:
        print(f"  [{label}] {filename}: download complete", flush=True)
        # register this peer as a seeder so the next wave can download from it too
        send_to_tracker(
            f"<updatetracker {filename} 0 {filesize} "
            f"{MY_IP} {BASE_PEER_PORT + n}>",
            label=label
        )
    else:
        print(f"  [{label}] WARNING: {filename} incomplete ({received}/{filesize})", flush=True)


def run_downloader(n: int, done_event: threading.Event) -> None:
    """
    Full download sequence for one downloader peer.
    Starts peer.py as a subprocess so its server thread stays live as a seeder
    for subsequent waves.  After both downloads complete, signals done_event so
    main knows this peer is finished, then waits on shutdown_event before
    sending quit — keeping the server thread alive until all waves are done.
    """
    label = f"Peer{n}"

    proc = start_proc(
        label,
        os.path.join(BASE_DIR, f"peer{n}", "peer.py"),
        cwd=os.path.join(BASE_DIR, f"peer{n}"),
    )
    time.sleep(0.5)

    # LIST
    print(f"  {label}: List", flush=True)
    send_to_tracker("<REQ LIST>", label=label)

    # GET + download small file
    print(f"  {label}: Get {SMALL_FILE}", flush=True)
    small_resp = send_to_tracker(f"<GET {SMALL_FILE}.track>", label=label)
    download_file_direct(n, SMALL_FILE, extract_tracker_body(small_resp))

    # GET + download large file
    print(f"  {label}: Get {LARGE_FILE}", flush=True)
    large_resp = send_to_tracker(f"<GET {LARGE_FILE}.track>", label=label)
    download_file_direct(n, LARGE_FILE, extract_tracker_body(large_resp))

    # signal that this peer's downloads are done, then stay alive as a seeder
    # until main tells everyone to shut down
    done_event.set()
    shutdown_event.wait()

    try:
        proc.stdin.write("quit\n")
        proc.stdin.flush()
    except Exception:
        pass
    proc.wait()


# ─────────────────────────────────────────────────────────────────
# runtime guard — abort early if final-setup wasn't run
# ─────────────────────────────────────────────────────────────────

def preflight_check() -> None:
    """
    Verify that `make final-setup` has been run.
    Only checks for what the Makefile actually creates: the shared files and
    peer.py copies.  Config files and cache dirs are handled by runtime_setup.
    """
    errors: list[str] = []

    for filename, peer_n in [(SMALL_FILE, 1), (LARGE_FILE, 2)]:
        path = os.path.join(BASE_DIR, f"peer{peer_n}", "shared", filename)
        if not os.path.exists(path):
            errors.append(f"  Missing: peer{peer_n}/shared/{filename}")

    for n in range(1, 14):
        peer_py = os.path.join(BASE_DIR, f"peer{n}", "peer.py")
        if not os.path.exists(peer_py):
            errors.append(f"  Missing: peer{n}/peer.py")

    if errors:
        print("ERROR: final-setup has not been run (or is incomplete).", flush=True)
        print("Run:  make final-setup   then retry.", flush=True)
        for e in errors:
            print(e, flush=True)
        sys.exit(1)


def runtime_setup() -> None:
    """
    Handles the gaps that the Makefile's final-setup leaves for the script:
      - cache/ directories (peer.py needs them for tracker caching)
      - clientThreadConfig.cfg and serverThreadConfig.cfg per peer
      - clearing tracker/torrents/ (spec: server must start with no .track files)
    Safe to re-run; overwrites stale config files from previous runs.
    """
    print("  [SETUP] Writing config files and clearing tracker...", flush=True)

    for n in range(1, 14):
        peer_dir = os.path.join(BASE_DIR, f"peer{n}")
        os.makedirs(os.path.join(peer_dir, "cache"), exist_ok=True)

        port = BASE_PEER_PORT + n
        with open(os.path.join(peer_dir, "clientThreadConfig.cfg"), "w") as f:
            f.write(f"{TRACKER_PORT}\n{TRACKER_IP}\n900\n")
        with open(os.path.join(peer_dir, "serverThreadConfig.cfg"), "w") as f:
            f.write(f"{port}\nshared\n{CHUNK_DELAY}")

    # spec: server must not contain any tracker files prior to start
    os.makedirs(TORRENTS_DIR, exist_ok=True)
    for f in os.listdir(TORRENTS_DIR):
        if f.endswith(".track"):
            os.remove(os.path.join(TORRENTS_DIR, f))

    print("  [SETUP] Done.", flush=True)


# ─────────────────────────────────────────────────────────────────
# timing helper
# ─────────────────────────────────────────────────────────────────

def _wait_until(target_s: float) -> None:
    remaining = target_s - (time.time() - _t0)
    if remaining > 0:
        print(
            f"\n  [{ts()}][DEMO] Waiting {remaining:.1f}s until t={int(target_s)}s...\n",
            flush=True
        )
        time.sleep(remaining)


# ─────────────────────────────────────────────────────────────────
# main
# ─────────────────────────────────────────────────────────────────

def main() -> None:
    global _t0, MY_IP

    preflight_check()
    runtime_setup()
    MY_IP = get_my_ip()
    print(f"  [DEMO] This machine's IP: {MY_IP}  Tracker: {TRACKER_IP}:{TRACKER_PORT}", flush=True)
    _t0 = time.time()

    # ── t=0 : tracker + seed peers ──────────────────────────────
    log("t=0 : seed peers", "DEMO")

    

    peer1_proc = start_proc(
        "Peer1",
        os.path.join(BASE_DIR, "peer1", "peer.py"),
        cwd=os.path.join(BASE_DIR, "peer1"),
    )
    peer2_proc = start_proc(
        "Peer2",
        os.path.join(BASE_DIR, "peer2", "peer.py"),
        cwd=os.path.join(BASE_DIR, "peer2"),
    )
    time.sleep(1)
    print(f"  [DEMO] Peer1 running (pid {peer1_proc.pid})", flush=True)
    print(f"  [DEMO] Peer2 running (pid {peer2_proc.pid})", flush=True)

    # read metadata from the files generated by final-setup
    small_path = os.path.join(BASE_DIR, "peer1", "shared", SMALL_FILE)
    large_path = os.path.join(BASE_DIR, "peer2", "shared", LARGE_FILE)
    small_size = os.path.getsize(small_path)
    large_size = os.path.getsize(large_path)
    small_md5  = file_md5(small_path)
    large_md5  = file_md5(large_path)

    # createtracker — each seed peer registers its file with the tracker
    step("Peer1: createtracker (small file)")
    print(
        f"Peer1: createtracker {SMALL_FILE} {small_size} small_shared_file "
        f"{small_md5} {MY_IP} {BASE_PEER_PORT + 1}",
        flush=True
    )
    send_to_tracker(
        f"<createtracker {SMALL_FILE} {small_size} small_shared_file "
        f"{small_md5} {MY_IP} {BASE_PEER_PORT + 1}>",
        label="Peer1"
    )

    step("Peer2: createtracker (large file)")
    print(
        f"Peer2: createtracker {LARGE_FILE} {large_size} large_shared_file "
        f"{large_md5} {MY_IP} {BASE_PEER_PORT + 2}",
        flush=True
    )
    send_to_tracker(
        f"<createtracker {LARGE_FILE} {large_size} large_shared_file "
        f"{large_md5} {MY_IP} {BASE_PEER_PORT + 2}>",
        label="Peer2"
    )

    time.sleep(1)   # let tracker write .track files before downloaders request them

    # ── t=30s : wave 1, peers 3-8 ───────────────────────────────
    _wait_until(30)
    log("t=30s : starting peers 3-8 (wave 1)", "DEMO")

    wave1: list[threading.Thread] = []
    wave1_done: list[threading.Event] = []
    for n in range(3, 9):
        done = threading.Event()
        wave1_done.append(done)
        t = threading.Thread(target=run_downloader, args=(n, done), daemon=False)
        wave1.append(t)
        t.start()
        time.sleep(0.4)

    # ── t=90s : wave 2, peers 9-13 + terminate seeds ────────────
    _wait_until(120)
    log("t=120s : 1 min 30s since last step, starting peers 9-13 (wave 2) + terminating peer1 & peer2", "DEMO")

    wave2: list[threading.Thread] = []
    wave2_done: list[threading.Event] = []
    for n in range(9, 14):
        done = threading.Event()
        wave2_done.append(done)
        t = threading.Thread(target=run_downloader, args=(n, done), daemon=False)
        wave2.append(t)
        t.start()
        time.sleep(0.4)

    threading.Thread(target=terminate_peer, args=(1, peer1_proc), daemon=True).start()
    threading.Thread(target=terminate_peer, args=(2, peer2_proc), daemon=True).start()

    # ── wait for every peer to finish downloading, then shut all down ────────
    log("All waves launched — waiting for all downloads to complete...", "DEMO")
    for e in wave1_done + wave2_done:
        e.wait()

    # all downloads done — release every peer so they can quit cleanly
    log("All downloads complete — shutting down peer processes...", "DEMO")
    shutdown_event.set()

    log("ABOUT TO JOIN ALL PEER THREADS", "DEBUG")
    for i, t in enumerate(wave1 + wave2):
        print(f"Thread {i} alive={t.is_alive()}", flush=True)

    for t in wave1 + wave2:
        t.join()

    # ── final summary ────────────────────────────────────────────
    log("DEMO COMPLETE", "DONE")

    print("  Tracker files on disk:", flush=True)
    for f in sorted(os.listdir(TORRENTS_DIR)):
        if f.endswith(".track"):
            print(f"    - {f}", flush=True)

    print("\n  Downloaded files per peer:", flush=True)
    for n in range(3, 14):
        shared = os.path.join(BASE_DIR, f"peer{n}", "shared")
        files = [
            f"{fname} ({os.path.getsize(os.path.join(shared, fname))} bytes)"
            for fname in sorted(os.listdir(shared))
            if os.path.isfile(os.path.join(shared, fname))
        ]
        print(f"    peer{n}: {', '.join(files) if files else 'none'}", flush=True)

    cleanup()


if __name__ == "__main__":
    main()