#!/usr/bin/env python3
# p2p peer program
# runs a client thread (talks to tracker), server thread (serves chunks to other peers),
# and an interactive cli so we can manually test commands

import socket
import threading
import os
import sys
import hashlib
import time

def load_client_config():
    with open("clientThreadConfig.cfg", "r") as f:
        lines = [l.strip() for l in f.readlines() if l.strip()]
    return {
        "tracker_port": int(lines[0]),
        "tracker_ip": lines[1],
        "update_interval": int(lines[2]),
    }


def load_server_config():
    with open("serverThreadConfig.cfg", "r") as f:
        lines = [l.strip() for l in f.readlines() if l.strip()]
    return {
        "listen_port": int(lines[0]),
        "shared_folder": lines[1],
        "chunk_delay": float(lines[2]) if len(lines) > 2 else 0.0,  # optional artificial delay per chunk served
    }


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


def file_md5(filepath):
    h = hashlib.md5()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def send_to_tracker(tracker_ip, tracker_port, message):
    # open a tcp connection, send the message, read the full response back
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(10)
        sock.connect((tracker_ip, tracker_port))
        sock.sendall((message + "\n").encode())

        response = b""
        while True:
            chunk = sock.recv(4096)
            if not chunk:
                break
            response += chunk
            # check if we got a full response yet
            decoded = response.decode()
            if (decoded.strip().endswith(">\n") or
                decoded.strip().endswith(">") or
                "REP LIST END" in decoded or
                "REP GET END" in decoded):
                break

        sock.close()
        return response.decode()
    except Exception as e:
        return f"<ERROR: {e}>\n"


def cmd_createtracker(client_cfg, server_cfg, filename):
    shared = server_cfg["shared_folder"]
    filepath = os.path.join(shared, filename)

    if not os.path.exists(filepath):
        print(f"  Error: File '{filename}' not found in {shared}/")
        return

    filesize = os.path.getsize(filepath)
    md5 = file_md5(filepath)
    ip = get_my_ip()
    port = server_cfg["listen_port"]
    description = filename.replace(" ", "_")

    msg = f"<createtracker {filename} {filesize} {description} {md5} {ip} {port}>"
    print(f"  Sending: {msg}")

    response = send_to_tracker(client_cfg["tracker_ip"], client_cfg["tracker_port"], msg)
    print(f"  Response: {response.strip()}")


def cmd_updatetracker(client_cfg, server_cfg, filename, start_byte, end_byte):
    ip = get_my_ip()
    port = server_cfg["listen_port"]

    msg = f"<updatetracker {filename} {start_byte} {end_byte} {ip} {port}>"
    print(f"  Sending: {msg}")

    response = send_to_tracker(client_cfg["tracker_ip"], client_cfg["tracker_port"], msg)
    print(f"  Response: {response.strip()}")


def cmd_list(client_cfg):
    msg = "<REQ LIST>"
    print(f"  Sending: {msg}")

    response = send_to_tracker(client_cfg["tracker_ip"], client_cfg["tracker_port"], msg)
    print(f"  Response:\n{response.strip()}")


def cmd_get_tracker(client_cfg, trackname):
    if not trackname.endswith(".track"):
        trackname += ".track"

    msg = f"<GET {trackname}>"
    print(f"  Sending: {msg}")

    response = send_to_tracker(client_cfg["tracker_ip"], client_cfg["tracker_port"], msg)
    print(f"  Response:\n{response.strip()}")

    # parse out the tracker file content from between the BEGIN/END markers
    if "<REP GET BEGIN>" in response:
        lines = response.strip().split("\n")
        content_lines = []
        file_md5_val = None
        in_content = False
        for line in lines:
            if "<REP GET BEGIN>" in line:
                in_content = True
                continue
            if "<REP GET END" in line:
                parts = line.strip().strip("<>").split()
                if len(parts) >= 4:
                    file_md5_val = parts[3]
                in_content = False
                continue
            if in_content:
                content_lines.append(line)

        content = "\n".join(content_lines) + "\n"

        # md5 check
        content_md5 = hashlib.md5(content.encode()).hexdigest()
        if file_md5_val and content_md5 == file_md5_val:
            print("  MD5 verification: PASSED")
        else:
            print(f"  MD5 verification: FAILED (got {content_md5}, expected {file_md5_val})")

        # save to local cache so we dont have to re-fetch
        os.makedirs("cache", exist_ok=True)
        cache_path = os.path.join("cache", trackname)
        with open(cache_path, "w") as f:
            f.write(content)
        print(f"  Tracker file saved to: {cache_path}")
        return content
    return None


def download_chunk(peers, filename, chunk_start, chunk_end, results, results_lock):
    # tries each peer in order (newest timestamp first) until one succeeds
    for peer in peers:
        try:
            # FIX: use context manager so socket is always closed even if an exception is raised mid-transfer
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.settimeout(15)
                sock.connect((peer["ip"], peer["port"]))

                request = f"GET {filename} {chunk_start} {chunk_end}\n"
                sock.sendall(request.encode())

                chunk = b""
                expected = chunk_end - chunk_start
                while len(chunk) < expected:
                    part = sock.recv(expected - len(chunk))
                    if not part:
                        break
                    chunk += part

            if chunk:
                with results_lock:
                    results[chunk_start] = chunk
                print(f"  [DOWNLOAD] Got bytes {chunk_start}-{chunk_end} from {peer['ip']}:{peer['port']}")
                return  # success — no need to try remaining peers
            else:
                print(f"  [DOWNLOAD] Empty response for bytes {chunk_start}-{chunk_end} from {peer['ip']}:{peer['port']}, trying next peer...")

        except Exception as e:
            print(f"  [DOWNLOAD] Failed bytes {chunk_start}-{chunk_end} from {peer['ip']}:{peer['port']}: {e}, trying next peer...")

    print(f"  [DOWNLOAD] All peers exhausted for bytes {chunk_start}-{chunk_end}, chunk unavailable")


def scan_null_chunks(filepath, filesize, chunk_size=1024):
    #helper method that scans for zero filled chunks and returns them as an array
    #This array is used when completing partial downloads to know which chunks need to be gotten
    missing = []
    try:
        with open(filepath, "rb") as f:
            offset = 0
            while offset < filesize:
                chunk_end = min(offset + chunk_size, filesize)
                data = f.read(chunk_end - offset)
                if not data or all(b == 0 for b in data):
                    missing.append((offset, chunk_end))
                offset = chunk_end
    except Exception as e:
        print(f"  [SCAN] Error scanning {filepath}: {e}")
    return missing


def find_contiguous_end(filepath, filesize, chunk_size=1024):
    #Since tracker format only allows ranges, find largest range of
    #continuous bytes from 0 to report to tracker when updating
    cont_end = 0
    try:
        with open(filepath, "rb") as f:
            offset = 0
            while offset < filesize:
                chunk_end = min(offset + chunk_size, filesize)
                data = f.read(chunk_end - offset)
                if not data or all(b == 0 for b in data):
                    break
                cont_end = chunk_end
                offset = chunk_end
    except Exception as e:
        print(f"  [SCAN] Error finding contiguous end in {filepath}: {e}")
    return cont_end


def cmd_download(client_cfg, server_cfg, filename, resume_from=0, missing_chunks=None, first_call=1):
    trackname = f"{filename}.track"
    cache_path = os.path.join("cache", trackname)

    if not os.path.exists(cache_path):
        print("  Fetching tracker file from server...")
        content = cmd_get_tracker(client_cfg, trackname)
        if content is None:
            print("  Error: Could not get tracker file.")
            return
    else:
        with open(cache_path, "r") as f:
            content = f.read()

    # pull the peer list out of the tracker data
    peers = []
    for line in content.strip().split("\n"):
        line = line.strip()
        if not line or line.startswith("#") or line.startswith("Filename") or \
           line.startswith("Filesize") or line.startswith("Description") or \
           line.startswith("MD5"):
            continue
        parts = line.split(":")
        if len(parts) == 5:
            peers.append({
                "ip": parts[0], "port": int(parts[1]),
                "start": int(parts[2]), "end": int(parts[3]),
                "timestamp": int(parts[4])
            })

    if not peers:
        print("  No peers available for this file.")
        return

    # remove ourselves from the peer list
    self_ip = get_my_ip()  # or detect it
    self_port = server_cfg["listen_port"]

    peers = [
        p for p in peers
        if not (p["ip"] == self_ip and p["port"] == self_port)
    ]

    if not peers:
        print("  No external peers available for this file.")
        return

    # sort by newest timestamp — freshest peers first
    peers.sort(key=lambda p: p["timestamp"], reverse=True)

    filesize = 0
    for line in content.strip().split("\n"):
        if line.startswith("Filesize:"):
            filesize = int(line.split(":")[1].strip())
            break

    # FIX: bad/missing tracker metadata would silently succeed writing nothing and call updatetracker
    if filesize == 0:
        print(f"  Error: filesize is 0 for {filename}, skipping.")
        return

    # FIX: empty chunk list would cause an error even though the file may already be complete
    if resume_from >= filesize:
        print(f"  {filename} already fully downloaded (resume_from={resume_from} >= filesize={filesize}).")
        if os.path.exists(cache_path):
            os.remove(cache_path)
            print(f"  Cache cleaned: {cache_path}")
        cmd_updatetracker(client_cfg, server_cfg, filename, "0", str(filesize))
        return

    if resume_from > 0:
        print(f"  File: {filename}, Size: {filesize} bytes (resuming from byte {resume_from})")
    else:
        print(f"  File: {filename}, Size: {filesize} bytes")
    print(f"  Available peers: {len(peers)}")

    shared = server_cfg["shared_folder"]
    os.makedirs(shared, exist_ok=True)
    output_path = os.path.join(shared, filename)

    CHUNK_SIZE = 1024

    # build chunk list — if the caller passed in specific null chunks from a
    # scan, use those directly; otherwise build sequentially from resume_from
    if missing_chunks is not None:
        chunks = missing_chunks
        print(f"  Filling {len(chunks)} null chunk(s) identified by scan")
    else:
        chunks = []
        offset = resume_from
        while offset < filesize:
            chunk_end = min(offset + CHUNK_SIZE, filesize)
            chunks.append((offset, chunk_end))
            offset = chunk_end

    print(f"  Total chunks to fetch: {len(chunks)}, distributing across {len(peers)} peer(s)")

    results = {}
    results_lock = threading.Lock()

    chunk_peer_map = []
    for chunk_start, chunk_end in chunks:
        eligible = [p for p in peers if p["start"] <= chunk_start < p["end"]]
        chunk_peer_map.append((chunk_start, chunk_end, eligible))

    # FIX: unbounded thread-per-chunk spawning exhausts ephemeral ports on large files;
    # semaphore caps concurrent connections so at most MAX_CONCURRENT sockets are open at once
    MAX_CONCURRENT = 8
    semaphore = threading.Semaphore(MAX_CONCURRENT)

    def throttled_download(eligible_sorted, chunk_start, chunk_end):
        with semaphore:
            download_chunk(eligible_sorted, filename, chunk_start, chunk_end, results, results_lock)

    threads = []
    for chunk_start, chunk_end, eligible in chunk_peer_map:
        if not eligible:
            continue
        # sort eligible peers by newest timestamp so download_chunk tries the
        # best one first and falls back down the list on failure
        eligible_sorted = sorted(eligible, key=lambda p: p["timestamp"], reverse=True)
        t = threading.Thread(target=throttled_download, args=(eligible_sorted, chunk_start, chunk_end))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    received_bytes = sum(len(v) for v in results.values())
    expected_bytes = sum(end - start for start, end in chunks)
    print(f"\n  Downloaded {received_bytes}/{expected_bytes} bytes across {len(results)}/{len(chunks)} chunks")

    # FIX: only bailing on zero chunks means a half-failed download (zero-filled chunks)
    # is silently treated as complete — require all expected bytes to be present
    if received_bytes == 0 and first_call == 1:
        print("  All peers failed. Re-fetching fresh tracker and retrying once...")
        if os.path.exists(cache_path):
            os.remove(cache_path)
        fresh = cmd_get_tracker(client_cfg, trackname)
        if fresh is None:
            print("  Error: Could not re-fetch tracker. Giving up.")
            return
        # tail-call into a clean retry — resume_from/missing_chunks preserved
        cmd_download(client_cfg, server_cfg, filename, resume_from=resume_from, missing_chunks=missing_chunks, first_call=0)
        return

    # FIX: if resume_from > 0 or missing_chunks mode but the file doesn't exist yet,
    # opening in "wb" + seeking to chunk_start silently writes zeros for bytes 0..chunk_start,
    # corrupting the file; pre-allocate so r+b seeks always land at the right offset
    if not os.path.exists(output_path) and (resume_from > 0 or missing_chunks is not None):
        with open(output_path, "wb") as f:
            f.seek(filesize - 1)
            f.write(b"\x00")

    mode = "r+b" if (missing_chunks is not None or (resume_from > 0 and os.path.exists(output_path))) else "wb"
    with open(output_path, mode) as f:
        for chunk_start, chunk_end in chunks:
            if chunk_start in results:
                f.seek(chunk_start)
                f.write(results[chunk_start])
            elif missing_chunks is None:
                # normal mode only: zero-fill so the file stays a single
                # contiguous block on disk — the gap will be retried later
                f.seek(chunk_start)
                f.write(b"\x00" * (chunk_end - chunk_start))
                print(f"  Warning: missing chunk {chunk_start}-{chunk_end}, filled with zeros")

    print(f"  File saved: {output_path}")

    # find the largest contiguous block from byte 0 for updatetracker.
    if missing_chunks is not None:
        cont_end = find_contiguous_end(output_path, filesize, CHUNK_SIZE)
    else:
        cont_end = resume_from
        for chunk_start, chunk_end in chunks:
            if chunk_start == cont_end and chunk_start in results:
                cont_end = chunk_end
            else:
                break

    if cont_end > 0:
        print(f"  Sending updatetracker: contiguous range 0-{cont_end}")
        cmd_updatetracker(client_cfg, server_cfg, filename, 0, cont_end)

    # FIX: cont_end reaching filesize is necessary but not sufficient — zero-filled failed
    # chunks could push it there; only declare complete when every expected byte was received
    if received_bytes == expected_bytes and cont_end >= filesize:
        print(f"  File download complete: {filename}")
        if os.path.exists(cache_path):
            os.remove(cache_path)
            print(f"  Cache cleaned: {cache_path}")
    else:
        still_missing = len([c for c in chunks if c[0] not in results])
        print(f"  Warning: {still_missing} chunk(s) still missing. Will retry on next update cycle.")


# --- peer server thread ---
# this is what other peers connect to when they want chunks from us

def handle_peer_request(conn, addr, shared_folder, chunk_delay=0.0):
    print(f"  [SERVER] Connection from peer {addr}")
    try:
        data = conn.recv(4096).decode().strip()
        print(f"  [SERVER] Request: {data}")

        parts = data.split()
        if not parts or parts[0].upper() != "GET":
            conn.sendall(b"<GET invalid>\n")
            conn.close()
            return

        if len(parts) < 4:
            conn.sendall(b"<GET invalid>\n")
            conn.close()
            return

        filename = parts[1]
        start = int(parts[2])
        end = int(parts[3])

        # enforce 1024 byte chunk limit per the protocol spec
        if end - start > 1024:
            conn.sendall(b"<GET invalid>\n")
            conn.close()
            return

        filepath = os.path.join(shared_folder, filename)
        if not os.path.exists(filepath):
            conn.sendall(b"<GET invalid>\n")
            conn.close()
            return

        with open(filepath, "rb") as f:
            f.seek(start)
            chunk = f.read(end - start)

        if chunk_delay > 0:
            time.sleep(chunk_delay)  # artificial throttle — must be before sendall so client actually waits
        conn.sendall(chunk)
        print(f"  [SERVER] Sent {len(chunk)} bytes of {filename} to {addr}")

    except Exception as e:
        print(f"  [SERVER] Error: {e}")
    finally:
        conn.close()


def start_peer_server(listen_port, shared_folder, chunk_delay=0.0):
    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_sock.bind(("0.0.0.0", listen_port))
    server_sock.listen(5)
    print(f"  [SERVER] Peer server listening on port {listen_port}")

    while True:
        try:
            conn, addr = server_sock.accept()
            t = threading.Thread(target=handle_peer_request, args=(conn, addr, shared_folder, chunk_delay))
            t.daemon = True
            t.start()
        except Exception:
            break


# --- periodic update thread ---
# sends updatetracker for everything in shared/ on a timer

def periodic_update(client_cfg, server_cfg):
    interval = client_cfg["update_interval"]
    shared = server_cfg["shared_folder"]
    ip = get_my_ip()
    port = server_cfg["listen_port"]

    while True:
        time.sleep(interval)
        #Attempt to complete any partial files
        print(f" [RESUME] Periodic check for partial files to complete")
        resume_incomplete_downloads(client_cfg, server_cfg)
        # send updatetracker for every file currently in the shared folder
        if os.path.exists(shared):
            for fname in os.listdir(shared):
                fpath = os.path.join(shared, fname)
                if os.path.isfile(fpath):
                    fsize = os.path.getsize(fpath)
                    msg = f"<updatetracker {fname} 0 {fsize} {ip} {port}>"
                    try:
                        send_to_tracker(client_cfg["tracker_ip"], client_cfg["tracker_port"], msg)
                        print(f"  [UPDATE] Sent periodic update for {fname}")
                    except Exception:
                        pass


# --- resume incomplete downloads ---
# checks cache and shared for matching tracker and partial files respectively
# continues download from where left off

def resume_incomplete_downloads(client_cfg, server_cfg):
    shared = server_cfg["shared_folder"]
    cache_dir = "cache"

    if not os.path.exists(cache_dir):
        return

    track_files = [f for f in os.listdir(cache_dir) if f.endswith(".track")]
    if not track_files:
        return

    print(f"\n[RESUME] Found {len(track_files)} cached tracker file(s). Checking for incomplete downloads...")

    for trackname in track_files:
        filename = trackname[:-6]
        partial_path = os.path.join(shared, filename)
        cache_path = os.path.join(cache_dir, trackname)

        try:
            with open(cache_path, "r") as f:
                content = f.read()
        except Exception as e:
            print(f"[RESUME] Could not read {cache_path}: {e}, skipping.")
            continue

        filesize = 0
        for line in content.strip().split("\n"):
            if line.startswith("Filesize:"):
                try:
                    filesize = int(line.split(":")[1].strip())
                except ValueError:
                    pass
                break

        if filesize == 0:
            print(f"[RESUME] Could not determine filesize for {filename}, skipping.")
            continue

        if os.path.exists(partial_path):
            # partial file exists — scan it for null chunks (zero-filled gaps
            # left by a previous interrupted download) rather than blindly
            # resuming from the tail, which would miss interior gaps
            partial_size = os.path.getsize(partial_path)
            print(f"[RESUME] {filename}: potential partial file, scanning for null chunks...")
            null_chunks = scan_null_chunks(partial_path, filesize)

            if null_chunks:
                print(f"[RESUME] {filename}: found {len(null_chunks)} null chunk(s), fetching fresh tracker and filling gaps...")
                cmd_get_tracker(client_cfg, f"{filename}.track")
                cmd_download(client_cfg, server_cfg, filename, missing_chunks=null_chunks)
            elif partial_size < filesize:
                # no interior gaps; file is just truncated — resume from tail
                print(f"[RESUME] {filename}: no interior gaps, resuming from byte {partial_size}...")
                cmd_get_tracker(client_cfg, f"{filename}.track")
                cmd_download(client_cfg, server_cfg, filename, resume_from=partial_size)
            else:
                #No null chunks, file is correct length, file is already downloaded
                #And just need to clean up cache
                print(f"[RESUME] {filename}: Already complete, cleaning up leftover cache.")
                os.remove(cache_path)
        else:
            # tracker in cache but no partial file; start from scratch
            print(f"[RESUME] {filename}: no partial file found, starting fresh download...")
            cmd_get_tracker(client_cfg, f"{filename}.track")
            cmd_download(client_cfg, server_cfg, filename, resume_from=0)

    print()


# --- interactive cli ---

def interactive_cli(client_cfg, server_cfg):
    peer_name = os.path.basename(os.getcwd())

    print(f"\n{'='*60}")
    print(f"  P2P Peer Program - {peer_name}")
    print(f"  Tracker: {client_cfg['tracker_ip']}:{client_cfg['tracker_port']}")
    print(f"  Listening on port: {server_cfg['listen_port']}")
    print(f"  Shared folder: {server_cfg['shared_folder']}/")
    print(f"  My IP: {get_my_ip()}")
    print(f"{'='*60}")
    print()
    print("Commands:")
    print("  createtracker <filename>")
    print("  updatetracker <filename> <start_byte> <end_byte>")
    print("  list  (or: REQ LIST)")
    print("  get <filename.track>")
    print("  download <filename>")
    print("  quit")
    print()

    while True:
        try:
            raw = input(f"{peer_name}> ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nExiting.")
            break

        if not raw:
            continue

        parts = raw.split()
        cmd = parts[0].lower()

        if cmd == "createtracker" and len(parts) >= 2:
            filename = parts[1]
            print(f"{peer_name}: createtracker {filename}")
            cmd_createtracker(client_cfg, server_cfg, filename)

        elif cmd == "updatetracker" and len(parts) >= 4:
            filename = parts[1]
            start = parts[2]
            end = parts[3]
            print(f"{peer_name}: updatetracker {filename} {start} {end}")
            cmd_updatetracker(client_cfg, server_cfg, filename, start, end)

        elif cmd in ("list", "req"):
            print(f"{peer_name}: REQ LIST")
            cmd_list(client_cfg)

        elif cmd == "get" and len(parts) >= 2:
            trackname = parts[1]
            print(f"{peer_name}: GET {trackname}")
            cmd_get_tracker(client_cfg, trackname)

        elif cmd == "download" and len(parts) >= 2:
            filename = parts[1]
            print(f"{peer_name}: downloading {filename}")
            cmd_download(client_cfg, server_cfg, filename)

        elif cmd == "quit":
            print("Exiting.")
            break

        else:
            print(f"  Unknown command: {raw}")
            print("  Try: createtracker, updatetracker, list, get, download, quit")

        print()


def main():
    client_cfg = load_client_config()
    server_cfg = load_server_config()

    os.makedirs(server_cfg["shared_folder"], exist_ok=True)
    os.makedirs("cache", exist_ok=True)

    # server thread - listens for other peers wanting file chunks
    server_thread = threading.Thread(
        target=start_peer_server,
        args=(server_cfg["listen_port"], server_cfg["shared_folder"], server_cfg["chunk_delay"])
    )
    server_thread.daemon = True
    server_thread.start()

    resume_incomplete_downloads(client_cfg, server_cfg)
    
    # background thread for periodic tracker updates
    update_thread = threading.Thread(
        target=periodic_update,
        args=(client_cfg, server_cfg)
    )
    update_thread.daemon = True
    update_thread.start()
    
    

    interactive_cli(client_cfg, server_cfg)


if __name__ == "__main__":
    main()