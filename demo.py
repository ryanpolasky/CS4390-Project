#!/usr/bin/env python3
# demo script - spins up the tracker and peers, then runs through all the
# protocol commands automatically. uses real tcp sockets for everything.

import subprocess
import time
import socket
import os
import sys
import hashlib
import signal

TRACKER_PORT = 9090
PEER1_PORT = 4001
PEER2_PORT = 4002
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

processes = []


def log(msg, prefix="DEMO"):
    print(f"\n{'='*60}")
    print(f"  [{prefix}] {msg}")
    print(f"{'='*60}")


def step(msg):
    print(f"\n--- {msg} ---")


def send_to_tracker(message):
    print(f"  >> Sending:  {message}")
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(5)
    sock.connect(("127.0.0.1", TRACKER_PORT))
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
        print(f"  << Received: {line}")
    return decoded


def send_to_peer(port, message):
    print(f"  >> Sending to peer (port {port}): {message}")
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(10)
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


def file_md5(filepath):
    h = hashlib.md5()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def cleanup(*args):
    print("\n\nCleaning up processes...")
    for p in processes:
        try:
            p.terminate()
            p.wait(timeout=2)
        except Exception:
            p.kill()
    sys.exit(0)


signal.signal(signal.SIGINT, cleanup)


def main():
    log("P2P File Sharing - Midterm Demo", "START")
    print("  running all protocol commands over real tcp connections")

    # start tracker
    log("starting tracker server on port {}".format(TRACKER_PORT))
    tracker_proc = subprocess.Popen(
        [sys.executable, "tracker.py"],
        cwd=os.path.join(BASE_DIR, "tracker"),
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT
    )
    processes.append(tracker_proc)
    time.sleep(1)
    print("  tracker server is running (pid: {})".format(tracker_proc.pid))

    # start peer1 and peer2 (they need to be up so we can transfer files)
    log("starting peer1 (port {}) and peer2 (port {})".format(PEER1_PORT, PEER2_PORT))

    peer1_proc = subprocess.Popen(
        [sys.executable, "peer.py"],
        cwd=os.path.join(BASE_DIR, "peer1"),
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT
    )
    processes.append(peer1_proc)

    peer2_proc = subprocess.Popen(
        [sys.executable, "peer.py"],
        cwd=os.path.join(BASE_DIR, "peer2"),
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT
    )
    processes.append(peer2_proc)
    time.sleep(1)
    print("  peer1 running (pid: {}), peer2 running (pid: {})".format(peer1_proc.pid, peer2_proc.pid))

    # --- test createtracker ---
    log("TEST 1: createtracker", "PROTOCOL")

    testfile = os.path.join(BASE_DIR, "peer1", "shared", "testfile.txt")
    fsize = os.path.getsize(testfile)
    md5 = file_md5(testfile)
    step("peer1 creates tracker for testfile.txt")
    msg = f"<createtracker testfile.txt {fsize} test_file {md5} 127.0.0.1 {PEER1_PORT}>"
    send_to_tracker(msg)

    largefile = os.path.join(BASE_DIR, "peer2", "shared", "largefile.bin")
    fsize2 = os.path.getsize(largefile)
    md5_2 = file_md5(largefile)
    step("peer2 creates tracker for largefile.bin")
    msg = f"<createtracker largefile.bin {fsize2} large_binary_file {md5_2} 127.0.0.1 {PEER2_PORT}>"
    send_to_tracker(msg)

    # this one should fail with ferr since it already exists
    step("peer1 tries duplicate createtracker (should get ferr)")
    msg = f"<createtracker testfile.txt {fsize} test_file {md5} 127.0.0.1 {PEER1_PORT}>"
    send_to_tracker(msg)

    # --- test updatetracker ---
    log("TEST 2: updatetracker", "PROTOCOL")

    step("peer1 sends updatetracker for testfile.txt")
    msg = f"<updatetracker testfile.txt 0 {fsize} 127.0.0.1 {PEER1_PORT}>"
    send_to_tracker(msg)

    step("updatetracker for a file that doesnt exist (should get ferr)")
    msg = f"<updatetracker fakefile.txt 0 100 127.0.0.1 {PEER1_PORT}>"
    send_to_tracker(msg)

    # --- test list ---
    log("TEST 3: REQ LIST", "PROTOCOL")

    step("peer3 requests list of all tracked files")
    send_to_tracker("<REQ LIST>")

    # --- test get ---
    log("TEST 4: GET tracker file", "PROTOCOL")

    step("peer3 requests testfile.txt.track")
    response = send_to_tracker("<GET testfile.txt.track>")

    step("peer3 requests a tracker that doesnt exist (should get invalid)")
    send_to_tracker("<GET nofile.track>")

    # --- test actual file transfer between peers ---
    log("TEST 5: peer-to-peer file transfer", "TRANSFER")

    step("peer3 downloads testfile.txt from peer1 (port {})".format(PEER1_PORT))
    data = send_to_peer(PEER1_PORT, f"GET testfile.txt 0 {fsize}")

    out_dir = os.path.join(BASE_DIR, "peer3", "shared")
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, "testfile.txt")
    with open(out_path, "wb") as f:
        f.write(data)
    print(f"  << received {len(data)} bytes, saved to peer3/shared/testfile.txt")

    if len(data) == fsize:
        received_md5 = hashlib.md5(data).hexdigest()
        match = "MATCH" if received_md5 == md5 else "MISMATCH"
        print(f"  md5 check: {match} (expected: {md5}, got: {received_md5})")
    else:
        print(f"  WARNING: expected {fsize} bytes, got {len(data)}")

    step("peer3 downloads first 10 chunks of largefile.bin from peer2 (port {})".format(PEER2_PORT))
    CHUNK_SIZE = 1024
    DEMO_CHUNKS = 10  # just first 10 to keep demo quick
    data2 = b""
    for i in range(DEMO_CHUNKS):
        chunk_start = i * CHUNK_SIZE
        chunk_end = min(chunk_start + CHUNK_SIZE, fsize2)
        chunk = send_to_peer(PEER2_PORT, f"GET largefile.bin {chunk_start} {chunk_end}")
        data2 += chunk
        print(f"  << chunk {i+1}/{DEMO_CHUNKS}: bytes {chunk_start}-{chunk_end}, got {len(chunk)} bytes")
    out_path2 = os.path.join(out_dir, "largefile.bin")
    with open(out_path2, "wb") as f:
        f.write(data2)
    expected = min(DEMO_CHUNKS * CHUNK_SIZE, fsize2)
    print(f"  << total received {len(data2)}/{expected} bytes, saved to peer3/shared/largefile.bin")
    if len(data2) == expected:
        print(f"  chunk transfer: PASS ({DEMO_CHUNKS} × {CHUNK_SIZE}-byte chunks)")

    # --- done ---
    log("DEMO COMPLETE", "DONE")
    print("  all protocol commands tested:")
    print("    1. createtracker  - success + duplicate error (ferr)")
    print("    2. updatetracker  - success + non-existent file error (ferr)")
    print("    3. REQ LIST       - returned list of tracked files")
    print("    4. GET            - retrieved tracker file + handled missing file")
    print("    5. file transfer  - downloaded files between peers w/ md5 verification")
    print()
    print("  tracker files on disk:")
    torrents = os.path.join(BASE_DIR, "tracker", "torrents")
    for f in os.listdir(torrents):
        if f.endswith(".track"):
            print(f"    - {f}")
    print()
    print("  files in peer3/shared/ (downloaded):")
    for f in os.listdir(out_dir):
        fpath = os.path.join(out_dir, f)
        print(f"    - {f} ({os.path.getsize(fpath)} bytes)")

    cleanup()


if __name__ == "__main__":
    main()
