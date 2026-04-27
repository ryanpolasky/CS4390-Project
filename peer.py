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


def cmd_download(client_cfg, server_cfg, filename, resume_from=0):
    # grabs the tracker file, picks the best peer, downloads the whole file
    # TODO: multi-threaded chunked download from multiple peers for final
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

    # newest timestamp = freshest peer, try that one first
    peers.sort(key=lambda p: p["timestamp"], reverse=True)

    filesize = 0
    for line in content.strip().split("\n"):
        if line.startswith("Filesize:"):
            filesize = int(line.split(":")[1].strip())
            break

    if resume_from > 0:
        print(f"  File: {filename}, Size: {filesize} bytes (resuming from byte {resume_from})")
    else:
        print(f"  File: {filename}, Size: {filesize} bytes")
    print(f"  Available peers: {len(peers)}")

    shared = server_cfg["shared_folder"]
    os.makedirs(shared, exist_ok=True)
    output_path = os.path.join(shared, filename)

    for peer in peers:
        # dont try to download from ourselves lol
        if peer["port"] == server_cfg["listen_port"]:
            continue

        # skip peers if they do not have a byte window including resume_from 
        if not peer["start"] <= resume_from < peer["end"]:
            continue

        download_start = max(peer["start"], resume_from)

        print(f"  Connecting to peer {peer['ip']}:{peer['port']}...")
        try:
            received = b""
            CHUNK_SIZE = 1024
            offset = download_start
            end = peer["end"]

            while offset < end:
                chunk_end = min(offset + CHUNK_SIZE, end)

                # open a fresh connection per chunk since server closes after each request
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(15)
                sock.connect((peer["ip"], peer["port"]))

                request = f"GET {filename} {offset} {chunk_end}\n"
                sock.sendall(request.encode())

                chunk = b""
                while len(chunk) < chunk_end - offset:
                    part = sock.recv(chunk_end - offset - len(chunk))
                    if not part:
                        break
                    chunk += part

                sock.close()
                time.sleep(2)
                received += chunk
                offset = chunk_end
                pct = (len(received)+resume_from) * 100 // filesize if filesize > 0 else 100
                print(f"\r  Downloading: {pct}% ({len(received)+resume_from}/{filesize} bytes)", end="")

            #Should always start from resume_from but just covering case where it's early
            overlap = resume_from - download_start
            if overlap > 0:
                received = received[overlap:]
            write_offset = download_start + max(0, overlap)
            if resume_from > 0 and os.path.exists(output_path):
                with open(output_path, "r+b") as f:
                    f.seek(write_offset)
                    f.write(received)
            else:
                with open(output_path, "wb") as f:
                    f.write(received)

            total_bytes = resume_from + len(received)
            print(f"  File downloaded: {output_path} ({total_bytes} bytes)")

            if total_bytes >= filesize:
                if os.path.exists(cache_path):
                    os.remove(cache_path)
                    print(f"  Cache cleaned: {cache_path}")
                # let the tracker know we have the full file now
                cmd_updatetracker(client_cfg, server_cfg, filename, "0", str(filesize))
            else:
                cmd_updatetracker(client_cfg, server_cfg, filename, "0", str(total_bytes))
                print(f"  Warning: only received {total_bytes}/{filesize} bytes.")
            return

        except Exception as e:
            print(f"  Failed to download from {peer['ip']}:{peer['port']}: {e}")
            continue
        
    print("  Error: Could not download from any peer.")


# --- peer server thread ---
# this is what other peers connect to when they want chunks from us

def handle_peer_request(conn, addr, shared_folder):
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

        conn.sendall(chunk)
        print(f"  [SERVER] Sent {len(chunk)} bytes of {filename} to {addr}")

    except Exception as e:
        print(f"  [SERVER] Error: {e}")
    finally:
        conn.close()


def start_peer_server(listen_port, shared_folder):
    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_sock.bind(("0.0.0.0", listen_port))
    server_sock.listen(5)
    print(f"  [SERVER] Peer server listening on port {listen_port}")

    while True:
        try:
            conn, addr = server_sock.accept()
            t = threading.Thread(target=handle_peer_request, args=(conn, addr, shared_folder))
            t.daemon = True
            t.start()
        except Exception:
            break


# --- periodic update thread ---
# sends updatetracker for everything in shared/ on a timer
# TODO: also check for incomplete files and re-request them for final

def periodic_update(client_cfg, server_cfg):
    interval = client_cfg["update_interval"]
    shared = server_cfg["shared_folder"]
    ip = get_my_ip()
    port = server_cfg["listen_port"]

    while True:
        time.sleep(interval)
        if not os.path.exists(shared):
            continue
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
    shared =server_cfg["shared_folder"]
    cache_dir = "cache"

    if not os.path.exists(cache_dir):
        return
    
    track_files = [f for f in os.listdir(cache_dir) if f.endswith("track")]
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
                    filesize = int(line.split(':')[1].strip())
                except ValueError:
                    pass
                break
        
        if filesize == 0:
            print(f"[RESUME] Could not determine filesize for {filename}, skipping.")
            continue

        if os.path.exists(partial_path):
            partial_size = os.path.getsize(partial_path)
            #Covering cases: file complete tracker not cleaned up
            if partial_size >= filesize: 
                print(f"[RESUME] {filename}: already complete ({partial_size}/{filesize} bytes), cleaning cache.")
                cmd_updatetracker(client_cfg, server_cfg, filename, "0", str(filesize))
                os.remove(cache_path)
            #Partial download to be resumed, fetch fresh tracker and attempt download
            else:
                print(f"[RESUME] {filename}: partial file found ({partial_size}/{filesize} bytes), resuming...")
                cmd_get_tracker(client_cfg, f"{filename}.track")
                cmd_download(client_cfg, server_cfg, filename, resume_from=partial_size)
        #Tracker file but no partial, download from beginning
        else:
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
        args=(server_cfg["listen_port"], server_cfg["shared_folder"])
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
