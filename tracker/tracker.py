#!/usr/bin/env python3
# tracker server - handles all incoming peer requests over tcp
# stores .track files in the torrents/ directory

import socket
import threading
import os
import time
import hashlib


def read_config():
    with open("sconfig", "r") as f:
        lines = [l.strip() for l in f.readlines() if l.strip()]
    port = int(lines[0])
    torrents_dir = lines[1]
    update_interval = int(lines[2])
    return port, torrents_dir, update_interval


def parse_tracker_file(filepath):
    # reads a .track file, pulls out the metadata fields and the peer list
    info = {}
    peers = []
    with open(filepath, "r") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if line.startswith("Filename:"):
                info["filename"] = line.split(":", 1)[1].strip()
            elif line.startswith("Filesize:"):
                info["filesize"] = line.split(":", 1)[1].strip()
            elif line.startswith("Description:"):
                info["description"] = line.split(":", 1)[1].strip()
            elif line.startswith("MD5:"):
                info["md5"] = line.split(":", 1)[1].strip()
            elif ":" in line and not line.startswith("<"):
                # ip:port:start:end:timestamp format
                parts = line.split(":")
                if len(parts) == 5:
                    peers.append({
                        "ip": parts[0],
                        "port": parts[1],
                        "start": parts[2],
                        "end": parts[3],
                        "timestamp": parts[4]
                    })
    return info, peers


def write_tracker_file(filepath, info, peers):
    with open(filepath, "w") as f:
        f.write(f"Filename: {info['filename']}\n")
        f.write(f"Filesize: {info['filesize']}\n")
        f.write(f"Description: {info.get('description', '')}\n")
        f.write(f"MD5: {info['md5']}\n")
        f.write("#list of peers follows next\n")
        for p in peers:
            f.write(f"{p['ip']}:{p['port']}:{p['start']}:{p['end']}:{p['timestamp']}\n")


def handle_createtracker(parts, torrents_dir):
    # expecting: createtracker filename filesize description md5 ip port
    if len(parts) < 7:
        return "<createtracker fail>\n"

    filename = parts[1]
    filesize = parts[2]
    description = parts[3]
    md5 = parts[4]
    ip = parts[5]
    port = parts[6]

    track_path = os.path.join(torrents_dir, f"{filename}.track")

    # already exists = ferr per the protocol spec
    if os.path.exists(track_path):
        return "<createtracker ferr>\n"

    try:
        timestamp = str(int(time.time()))
        info = {
            "filename": filename,
            "filesize": filesize,
            "description": description,
            "md5": md5,
        }
        peers = [{
            "ip": ip,
            "port": port,
            "start": "0",
            "end": filesize,
            "timestamp": timestamp,
        }]
        write_tracker_file(track_path, info, peers)
        print(f"  [TRACKER] Created tracker file: {filename}.track")
        return "<createtracker succ>\n"
    except Exception as e:
        print(f"  [TRACKER] Error creating tracker: {e}")
        return "<createtracker fail>\n"


def handle_updatetracker(parts, torrents_dir, update_interval=900):
    # expecting: updatetracker filename start_bytes end_bytes ip port
    if len(parts) < 6:
        return "<updatetracker fail>\n"

    filename = parts[1]
    start_bytes = parts[2]
    end_bytes = parts[3]
    ip = parts[4]
    port = parts[5]

    track_path = os.path.join(torrents_dir, f"{filename}.track")

    if not os.path.exists(track_path):
        return f"<updatetracker {filename} ferr>\n"

    try:
        info, peers = parse_tracker_file(track_path)
        current_time = int(time.time())

        # check if this peer already has an entry, if so just update it
        found = False
        for p in peers:
            if p["ip"] == ip and p["port"] == port:
                p["start"] = start_bytes
                p["end"] = end_bytes
                p["timestamp"] = str(current_time)
                found = True
                break

        if not found:
            peers.append({
                "ip": ip,
                "port": port,
                "start": start_bytes,
                "end": end_bytes,
                "timestamp": str(current_time),
            })

        write_tracker_file(track_path, info, peers)
        print(f"  [TRACKER] Updated tracker: {filename}.track for peer {ip}:{port}")
        return f"<updatetracker {filename} succ>\n"
    except Exception as e:
        print(f"  [TRACKER] Error updating tracker: {e}")
        return f"<updatetracker {filename} fail>\n"


#Perodic peer pruning, default interval at 900
#By default called with interval value in sconfig (line 3)
def periodic_cleanup(torrents_dir, update_interval=900):
    #Background thread, checks every interval
    file_lock = threading.Lock()

    while True:
        # sleep first so we don't run immediately at startup before any peers
        # have had a chance to register
        time.sleep(update_interval)

        current_time = int(time.time())
        print(f"[CLEANUP] Running periodic dead-peer sweep at {current_time}...")

        try:
            track_files = [
                f for f in os.listdir(torrents_dir) if f.endswith(".track")
            ]
        except Exception as e:
            print(f"[CLEANUP] Could not list torrents dir: {e}")
            continue

        for tf in track_files:
            track_path = os.path.join(torrents_dir, tf)
            try:
                with file_lock:
                    info, peers = parse_tracker_file(track_path)

                    live = [
                        p for p in peers
                        if current_time - int(p["timestamp"]) < update_interval
                    ]
                    dead_count = len(peers) - len(live)

                    if dead_count > 0:
                        write_tracker_file(track_path, info, live)
                        print(
                            f"[CLEANUP] {tf}: removed {dead_count} dead peer(s), "
                            f"{len(live)} remaining"
                        )
            except Exception as e:
                print(f"[CLEANUP] Error processing {tf}: {e}")

        print("[CLEANUP] Sweep complete.")



def handle_list(torrents_dir):
    track_files = [f for f in os.listdir(torrents_dir) if f.endswith(".track")]
    count = len(track_files)

    response = f"<REP LIST {count}>\n"
    for i, tf in enumerate(track_files, 1):
        track_path = os.path.join(torrents_dir, tf)
        info, _ = parse_tracker_file(track_path)
        fname = info.get("filename", tf.replace(".track", ""))
        fsize = info.get("filesize", "0")
        fmd5 = info.get("md5", "")
        response += f"<{i} {fname} {fsize} {fmd5}>\n"
    response += "<REP LIST END>\n"
    return response


def handle_get(parts, torrents_dir):
    if len(parts) < 2:
        return "<GET invalid>\n"

    trackname = parts[1]
    track_path = os.path.join(torrents_dir, trackname)

    if not os.path.exists(track_path):
        return "<GET invalid>\n"

    with open(track_path, "r") as f:
        content = f.read()

    file_md5 = hashlib.md5(content.encode()).hexdigest()

    response = "<REP GET BEGIN>\n"
    response += content
    if not content.endswith("\n"):
        response += "\n"
    response += f"<REP GET END {file_md5}>\n"
    return response


def handle_client(conn, addr, torrents_dir):
    # each connection gets its own thread, handles one request then closes
    print(f"[TRACKER] Connection from {addr}")
    try:
        data = b""
        while True:
            chunk = conn.recv(4096)
            if not chunk:
                break
            data += chunk
            if b"\n" in data:
                break

        message = data.decode().strip()
        # strip the angle brackets if the peer sent them
        if message.startswith("<") and message.endswith(">"):
            message = message[1:-1]

        print(f"  [TRACKER] Received: {message}")

        parts = message.split()
        if not parts:
            conn.sendall(b"<ERROR>\n")
            return

        command = parts[0].lower()

        if command == "createtracker":
            response = handle_createtracker(parts, torrents_dir)
        elif command == "updatetracker":
            response = handle_updatetracker(parts, torrents_dir)
        elif command == "req" and len(parts) > 1 and parts[1].upper() == "LIST":
            response = handle_list(torrents_dir)
        elif command == "get":
            response = handle_get(parts, torrents_dir)
        else:
            response = "<ERROR unknown command>\n"

        conn.sendall(response.encode())
        print(f"  [TRACKER] Sent response to {addr}")

    except Exception as e:
        print(f"  [TRACKER] Error handling {addr}: {e}")
    finally:
        conn.close()
        print(f"[TRACKER] Connection closed: {addr}")


def main():
    port, torrents_dir, update_interval = read_config()
    os.makedirs(torrents_dir, exist_ok=True)

    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_sock.bind(("0.0.0.0", port))
    server_sock.listen(5)

    print(f"[TRACKER] Server listening on port {port}")
    print(f"[TRACKER] Tracker files stored in: {torrents_dir}/")
    print("[TRACKER] Ready to accept connections...")

    #Starting periodic cleanup thread
    cleanup_thread = threading.Thread(target=periodic_cleanup, args=(torrents_dir, update_interval))
    cleanup_thread.deamon = True
    cleanup_thread.start()

    try:
        while True:
            conn, addr = server_sock.accept()
            # spin up a new thread per connection
            t = threading.Thread(target=handle_client, args=(conn, addr, torrents_dir))
            t.daemon = True
            t.start()
    except KeyboardInterrupt:
        print("\n[TRACKER] Server shutting down.")
    finally:
        server_sock.close()


if __name__ == "__main__":
    main()
