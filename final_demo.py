#!/usr/bin/env python3
# Final demo script
# t=0:   tracker + peer1 (testfile seed) + peer2 (largefile seed)
# t=30s: peers 3-8 join and download largefile.bin
# t=90s: peers 9-13 join, peer1 + peer2 are terminated
# peers 9-13 must complete download from peers 3-8's partial files only

import hashlib, os, shutil, signal, socket, subprocess, sys, threading, time

BASE_DIR    = os.path.dirname(os.path.abspath(__file__))
TRACKER_IP  = "10.255.203.4"
TRACKER_PORT = 9090
WAVE1       = list(range(3, 9))    # peers 3-8,  join at t=30s
WAVE2       = list(range(9, 14))   # peers 9-13, join at t=90s
CHUNK_DELAY = 0.05                 # 50ms per chunk — keeps wave 1 partial at t=90s

processes  = {}
temp_dirs  = []
START_TIME = 0.0


# ── helpers ────────────────────────────────────────────────────────────────

def banner(msg):
    elapsed = time.time() - START_TIME
    print(f"\n{'='*60}")
    print(f"  [t={elapsed:5.1f}s]  {msg}")
    print(f"{'='*60}")


def file_md5(path):
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def tracker_send(msg):
    try:
        s = socket.socket()
        s.settimeout(5)
        s.connect((TRACKER_IP, TRACKER_PORT))
        s.sendall((msg + "\n").encode())
        resp = b""
        while True:
            c = s.recv(4096)
            if not c:
                break
            resp += c
            d = resp.decode()
            if any(t in d for t in ["succ", "ferr", "fail", "END", "invalid"]):
                break
        s.close()
        return resp.decode().strip()
    except Exception as e:
        return f"ERROR: {e}"


def peer_dir(n):
    if n <= 3:
        return os.path.join(BASE_DIR, f"peer{n}")
    return os.path.join(BASE_DIR, f"_peer{n}")


def create_peer_dir(n):
    d = peer_dir(n)
    os.makedirs(os.path.join(d, "shared"), exist_ok=True)
    os.makedirs(os.path.join(d, "cache"), exist_ok=True)
    shutil.copy(os.path.join(BASE_DIR, "peer.py"), os.path.join(d, "peer.py"))
    with open(os.path.join(d, "clientThreadConfig.cfg"), "w") as f:
        f.write(f"{TRACKER_PORT}\n{TRACKER_IP}\n30\n")
    with open(os.path.join(d, "serverThreadConfig.cfg"), "w") as f:
        f.write(f"{4000 + n}\nshared\n{CHUNK_DELAY}\n")
    if n > 3:
        temp_dirs.append(d)


def peer_log_reader(n, stream):
    for raw in stream:
        line = raw.decode(errors="replace").rstrip()
        if not line:
            continue
        elapsed = time.time() - START_TIME
        print(f"[peer{n:>2} t={elapsed:6.1f}s] {line}", flush=True)


def start_peer(n):
    d = peer_dir(n)
    proc = subprocess.Popen(
        [sys.executable, "peer.py"],
        cwd=d,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    processes[n] = proc
    t = threading.Thread(target=peer_log_reader, args=(n, proc.stdout), daemon=True)
    t.start()
    return proc


def send_cmd(n, cmd):
    proc = processes.get(n)
    if proc and proc.poll() is None:
        try:
            proc.stdin.write((cmd + "\n").encode())
            proc.stdin.flush()
        except Exception:
            pass


def terminate_peer(n):
    proc = processes.get(n)
    if proc:
        proc.terminate()
        print(f"  peer{n} (port {4000+n}) terminated — no longer seeding")


def cleanup(*args):
    print("\n[DEMO] Shutting down all processes...")
    for proc in processes.values():
        try:
            proc.terminate()
            proc.wait(timeout=2)
        except Exception:
            try:
                proc.kill()
            except Exception:
                pass
    for d in temp_dirs:
        shutil.rmtree(d, ignore_errors=True)
    sys.exit(0)


signal.signal(signal.SIGINT, cleanup)
signal.signal(signal.SIGTERM, cleanup)


# ── main ───────────────────────────────────────────────────────────────────

def main():
    global START_TIME

    # pre-flight checks
    testfile  = os.path.join(BASE_DIR, "peer1", "shared", "testfile.txt")
    largefile = os.path.join(BASE_DIR, "peer2", "shared", "largefile.bin")
    if not os.path.exists(testfile) or not os.path.exists(largefile):
        print("ERROR: run 'make setup' before starting the demo.")
        sys.exit(1)

    # wipe stale state
    for f in os.listdir(os.path.join(BASE_DIR, "tracker", "torrents")):
        if f.endswith(".track"):
            os.remove(os.path.join(BASE_DIR, "tracker", "torrents", f))
    for n in WAVE1 + WAVE2:
        d = peer_dir(n)
        if n > 3 and os.path.exists(d):
            shutil.rmtree(d)
    for sub in ("cache", "shared"):
        d = os.path.join(BASE_DIR, "peer3", sub)
        if os.path.exists(d):
            shutil.rmtree(d)
        os.makedirs(d)

    print("=" * 60)
    print("  FINAL DEMO — P2P File Sharing")
    print(f"  chunk delay : {CHUNK_DELAY}s  |  file size : {os.path.getsize(largefile)//1024} KB")
    print("=" * 60)

    START_TIME = time.time()

    # ── t=0 ──────────────────────────────────────────────────────
    banner("t=0  Starting tracker + seed peers (peer1, peer2)")

    tracker_proc = subprocess.Popen(
        [sys.executable, "tracker.py"],
        cwd=os.path.join(BASE_DIR, "tracker"),
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    processes["tracker"] = tracker_proc
    time.sleep(1)

    for n in (1, 2):
        create_peer_dir(n)
        start_peer(n)
    time.sleep(1.5)

    tf_size = os.path.getsize(testfile)
    lf_size = os.path.getsize(largefile)

    r = tracker_send(f"<createtracker testfile.txt {tf_size} testfile {file_md5(testfile)} 127.0.0.1 4001>")
    print(f"  createtracker testfile.txt  → {r}")
    r = tracker_send(f"<createtracker largefile.bin {lf_size} largefile {file_md5(largefile)} 127.0.0.1 4002>")
    print(f"  createtracker largefile.bin → {r}")
    print(f"\n  peer1 (port 4001) seeding testfile.txt  ({tf_size} bytes)")
    print(f"  peer2 (port 4002) seeding largefile.bin ({lf_size} bytes)")

    # ── t=30s ─────────────────────────────────────────────────────
    banner("Waiting 30s for wave 1...")
    time.sleep(30)
    banner("t=30s  Starting peers 3-8 — downloading testfile.txt + largefile.bin")

    for n in WAVE1:
        create_peer_dir(n)
        start_peer(n)
        print(f"  peer{n} started on port {4000+n}")
    time.sleep(1.5)

    for n in WAVE1:
        send_cmd(n, "download testfile.txt")
        send_cmd(n, "download largefile.bin")
        print(f"  peer{n}: issued download testfile.txt + largefile.bin")

    # ── t=90s ─────────────────────────────────────────────────────
    banner("Waiting 60s for wave 2...")
    time.sleep(60)
    banner("t=90s  Terminating seed peers + starting peers 9-13")

    terminate_peer(1)
    terminate_peer(2)
    print()

    for n in WAVE2:
        create_peer_dir(n)
        start_peer(n)
        print(f"  peer{n} started on port {4000+n}")
    time.sleep(1.5)

    for n in WAVE2:
        send_cmd(n, "download testfile.txt")
        send_cmd(n, "download largefile.bin")
        print(f"  peer{n}: issued download testfile.txt + largefile.bin")

    print("\n  Seed peers are gone — peers 9-13 rely on peers 3-8 partial files only")
    print("  testfile.txt must come from wave-1 peers (peer1 is terminated)")

    # ── monitor ───────────────────────────────────────────────────
    banner("Monitoring downloads — Ctrl+C to stop")
    try:
        while True:
            time.sleep(15)
            elapsed = time.time() - START_TIME
            alive = [n for n in WAVE1 + WAVE2 if processes[n].poll() is None]
            lf_done, tf_done = [], []
            for n in WAVE1 + WAVE2:
                lp = os.path.join(peer_dir(n), "shared", "largefile.bin")
                tp = os.path.join(peer_dir(n), "shared", "testfile.txt")
                if os.path.exists(lp) and os.path.getsize(lp) == lf_size:
                    lf_done.append(n)
                if os.path.exists(tp) and os.path.getsize(tp) == tf_size:
                    tf_done.append(n)
            print(f"[t={elapsed:.0f}s]  alive: {len(alive)} peers  |  largefile done: {lf_done}  |  testfile done: {tf_done}")
    except KeyboardInterrupt:
        pass

    cleanup()


if __name__ == "__main__":
    main()
