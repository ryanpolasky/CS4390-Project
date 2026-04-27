#!/usr/bin/env python3
# Tests for parallel chunk download and resume.
# Spins up the real tracker + peer1 + peer2, then exercises cmd_download
# with resume_from=0 (full parallel) and resume_from>0 (resume path).

import subprocess, time, os, sys, hashlib, socket, shutil, signal

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TRACKER_PORT = 9090
PEER1_PORT = 4001
PEER2_PORT = 4002

processes = []
PASS = 0
FAIL = 0


def file_md5(path):
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def send_to_tracker(msg):
    s = socket.socket()
    s.settimeout(5)
    s.connect(("127.0.0.1", TRACKER_PORT))
    s.sendall((msg + "\n").encode())
    resp = b""
    while True:
        try:
            c = s.recv(4096)
            if not c:
                break
            resp += c
            dec = resp.decode()
            if any(tok in dec for tok in [">\n", ">", "REP LIST END", "REP GET END"]):
                break
        except socket.timeout:
            break
    s.close()
    return resp.decode()


def cleanup(*args):
    for p in processes:
        try:
            p.terminate()
            p.wait(timeout=2)
        except Exception:
            p.kill()


signal.signal(signal.SIGINT, cleanup)
signal.signal(signal.SIGTERM, cleanup)


def check(label, passed, detail=""):
    global PASS, FAIL
    status = "PASS" if passed else "FAIL"
    suffix = f"  ({detail})" if detail else ""
    print(f"  [{status}] {label}{suffix}")
    if passed:
        PASS += 1
    else:
        FAIL += 1


def section(title):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}")


def main():
    section("Setup: starting tracker and peers")

    # wipe stale tracker files so createtracker starts fresh
    torrents_dir = os.path.join(BASE_DIR, "tracker", "torrents")
    for f in os.listdir(torrents_dir):
        if f.endswith(".track"):
            os.remove(os.path.join(torrents_dir, f))

    # clean peer3 shared and cache dirs
    peer3_shared = os.path.join(BASE_DIR, "peer3", "shared")
    peer3_cache = os.path.join(BASE_DIR, "peer3", "cache")
    for d in (peer3_shared, peer3_cache):
        if os.path.exists(d):
            shutil.rmtree(d)
        os.makedirs(d)

    tracker_proc = subprocess.Popen(
        [sys.executable, "tracker.py"],
        cwd=os.path.join(BASE_DIR, "tracker"),
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    processes.append(tracker_proc)

    peer1_proc = subprocess.Popen(
        [sys.executable, "peer.py"],
        cwd=os.path.join(BASE_DIR, "peer1"),
        stdin=subprocess.PIPE, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    processes.append(peer1_proc)

    peer2_proc = subprocess.Popen(
        [sys.executable, "peer.py"],
        cwd=os.path.join(BASE_DIR, "peer2"),
        stdin=subprocess.PIPE, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    processes.append(peer2_proc)

    time.sleep(1.5)

    testfile_path = os.path.join(BASE_DIR, "peer1", "shared", "testfile.txt")
    testfile_size = os.path.getsize(testfile_path)
    testfile_md5 = file_md5(testfile_path)
    send_to_tracker(f"<createtracker testfile.txt {testfile_size} testfile {testfile_md5} 127.0.0.1 {PEER1_PORT}>")

    largefile_path = os.path.join(BASE_DIR, "peer2", "shared", "largefile.bin")
    largefile_size = os.path.getsize(largefile_path)
    largefile_md5 = file_md5(largefile_path)
    send_to_tracker(f"<createtracker largefile.bin {largefile_size} largefile {largefile_md5} 127.0.0.1 {PEER2_PORT}>")

    print(f"  tracker up, peer1 (port {PEER1_PORT}) and peer2 (port {PEER2_PORT}) registered")
    print(f"  testfile.txt: {testfile_size} bytes  |  largefile.bin: {largefile_size} bytes ({largefile_size // 1024} chunks)")

    sys.path.insert(0, os.path.join(BASE_DIR, "peer3"))
    from peer import cmd_download

    client_cfg = {"tracker_ip": "127.0.0.1", "tracker_port": TRACKER_PORT, "update_interval": 900}
    server_cfg = {"listen_port": 4003, "shared_folder": peer3_shared}

    # ------------------------------------------------------------------ #
    section("Test 1: parallel full download of testfile.txt (1 chunk)")
    # ------------------------------------------------------------------ #
    cmd_download(client_cfg, server_cfg, "testfile.txt")

    out = os.path.join(peer3_shared, "testfile.txt")
    check("file exists after download", os.path.exists(out))
    if os.path.exists(out):
        got_size = os.path.getsize(out)
        got_md5 = file_md5(out)
        check("file size correct", got_size == testfile_size,
              f"got {got_size}, want {testfile_size}")
        check("MD5 matches original", got_md5 == testfile_md5,
              f"got {got_md5}, want {testfile_md5}")
        os.remove(out)

    # ------------------------------------------------------------------ #
    section("Test 2: parallel full download of largefile.bin (100 chunks)")
    # ------------------------------------------------------------------ #
    for f in os.listdir(peer3_cache):
        os.remove(os.path.join(peer3_cache, f))

    cmd_download(client_cfg, server_cfg, "largefile.bin")

    out = os.path.join(peer3_shared, "largefile.bin")
    check("file exists after download", os.path.exists(out))
    if os.path.exists(out):
        got_size = os.path.getsize(out)
        got_md5 = file_md5(out)
        check("file size correct", got_size == largefile_size,
              f"got {got_size}, want {largefile_size}")
        check("MD5 matches original", got_md5 == largefile_md5,
              f"got {got_md5}, want {largefile_md5}")

    # ------------------------------------------------------------------ #
    section("Test 3: resume download (first 50 KB already present)")
    # ------------------------------------------------------------------ #
    resume_from = 50 * 1024
    with open(largefile_path, "rb") as src, open(out, "wb") as dst:
        dst.write(src.read(resume_from))
    check("partial file written", os.path.getsize(out) == resume_from)

    for f in os.listdir(peer3_cache):
        os.remove(os.path.join(peer3_cache, f))

    cmd_download(client_cfg, server_cfg, "largefile.bin", resume_from=resume_from)

    if os.path.exists(out):
        got_size = os.path.getsize(out)
        got_md5 = file_md5(out)
        check("file size correct after resume", got_size == largefile_size,
              f"got {got_size}, want {largefile_size}")
        check("MD5 matches original after resume", got_md5 == largefile_md5,
              f"got {got_md5}, want {largefile_md5}")
    else:
        check("file exists after resume", False)

    # ------------------------------------------------------------------ #
    section("Test 4: resume_from=0 gives same result as fresh download")
    # ------------------------------------------------------------------ #
    resumed_md5 = file_md5(out) if os.path.exists(out) else None
    for f in os.listdir(peer3_cache):
        os.remove(os.path.join(peer3_cache, f))
    if os.path.exists(out):
        os.remove(out)

    cmd_download(client_cfg, server_cfg, "largefile.bin", resume_from=0)

    if os.path.exists(out) and resumed_md5:
        fresh_md5 = file_md5(out)
        check("resume_from=0 matches resumed download MD5", fresh_md5 == resumed_md5)
        check("resume_from=0 matches original MD5", fresh_md5 == largefile_md5)
    else:
        check("second full download succeeded", False)

    # ------------------------------------------------------------------ #
    section("Results")
    # ------------------------------------------------------------------ #
    total = PASS + FAIL
    print(f"\n  {PASS}/{total} tests passed" + ("  — ALL PASSED" if FAIL == 0 else f"  — {FAIL} FAILED"))
    print()

    cleanup()
    sys.exit(0 if FAIL == 0 else 1)


if __name__ == "__main__":
    main()
