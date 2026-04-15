# CS4390 - P2P File Sharing Protocol

A peer-to-peer file sharing system built for CS4390 Computer Networks, implementing a BitTorrent-like protocol with a centralized tracker server.

## Overview

The system has two main components:

- **Tracker Server** - maintains metadata about which peers are sharing which files
- **Peer Program** - shares files with other peers and downloads files from them

The tracker only handles metadata coordination. Actual file data transfers directly between peers without passing through the tracker.

## Project Structure

```
CS4390-Project/
├── peer.py                     # Master peer source (copied to each peer folder by make setup)
├── Makefile
├── demo.py                     # Automated demo script
├── tracker/
│   ├── tracker.py              # Tracker server
│   ├── sconfig                 # Tracker config (port, torrents directory)
│   └── torrents/               # Tracker files (.track) stored here
├── peer1/
│   ├── peer.py                 # Copy of master peer
│   ├── clientThreadConfig.cfg  # Tracker IP, port, update interval
│   ├── serverThreadConfig.cfg  # Peer listen port, shared folder
│   ├── shared/                 # Files this peer shares/downloads
│   └── cache/                  # Locally cached .track files
├── peer2/                      # Same structure as peer1
└── peer3/                      # Same structure as peer1
```

## Requirements

- Python 3.x
- No external dependencies — uses Python standard library only

## Setup and Running

### 1. Setup

```bash
make setup
```

This copies `peer.py` to each peer folder and creates sample test files:
- `peer1/shared/testfile.txt` — small test file (50 bytes)
- `peer2/shared/largefile.bin` — large binary file (100KB)

### 2. Run (4 separate terminals)

```bash
# Terminal 1
make run-tracker

# Terminal 2
make run-peer1

# Terminal 3
make run-peer2

# Terminal 4
make run-peer3
```

### 3. Cleanup

```bash
make clean
```

Removes copied `peer.py` files, cached `.track` files, and tracker torrents.

## Configuration Files

### Tracker — `tracker/sconfig`
```
9090        # port to listen on
torrents    # directory to store .track files
```

### Peer — `clientThreadConfig.cfg`
```
9090        # tracker port
127.0.0.1   # tracker IP address
900         # updatetracker interval in seconds (default 15 min)
```

### Peer — `serverThreadConfig.cfg`
```
4001        # port this peer listens on for incoming peer connections
shared      # shared folder name
```

Each peer has its own config files. peer1 listens on 4001, peer2 on 4002, peer3 on 4003.

## Protocol

All messages between peers and the tracker use TCP with angle-bracket wrapped text.

### createtracker
```
Peer   → Tracker: <createtracker filename filesize description md5 ip port>
Tracker → Peer:   <createtracker succ>
                  <createtracker ferr>   (file already exists)
                  <createtracker fail>   (error)
```

### updatetracker
```
Peer   → Tracker: <updatetracker filename start_bytes end_bytes ip port>
Tracker → Peer:   <updatetracker filename succ>
                  <updatetracker filename ferr>  (tracker file not found)
                  <updatetracker filename fail>  (error)
```

### LIST
```
Peer   → Tracker: <REQ LIST>
Tracker → Peer:   <REP LIST 2>
                  <1 filename filesize md5>
                  <2 filename filesize md5>
                  <REP LIST END>
```

### GET
```
Peer   → Tracker: <GET filename.track>
Tracker → Peer:   <REP GET BEGIN>
                  <tracker file contents>
                  <REP GET END md5>
```

### Peer-to-Peer file transfer
```
Peer A → Peer B: GET filename start_byte end_byte
Peer B → Peer A: [raw file bytes]
```

## Tracker File Format

Stored at `tracker/torrents/<filename>.track`:

```
Filename: movie1.avi
Filesize: 109283519
Description: Ghost_and_the_Darkness
MD5: c68c2ee8bfca4e898b396e7a935a1d92
#list of peers follows next
192.168.1.70:4001:0:109283519:1774742285
192.168.1.70:4003:0:109283519:1774742789
```

Each peer entry: `ip:port:start_byte:end_byte:timestamp`

## Testing the Full Flow

```
peer1> createtracker testfile.txt       # register file with tracker
peer2> createtracker largefile.bin      # register file with tracker

peer3> list                             # see all available files
peer3> get testfile.txt.track           # fetch tracker metadata
peer3> download testfile.txt            # download directly from peer1
```

After a successful download:
- File appears in `peer3/shared/`
- Cache file at `peer3/cache/testfile.txt.track` is deleted
- Tracker is updated to list peer3 as a seeder

## Automated Demo

```bash
make demo
```

Runs `demo.py` which starts all components automatically and tests the full protocol flow including file transfers and MD5 verification.

## Remaining Work

1. **Resume incomplete downloads on startup** — on startup the peer should check the cache for leftover tracker files and the shared folder for partial files, then resume from where it left off instead of restarting the download from scratch.

2. **Periodic incomplete-file re-check** — the periodic update thread currently only sends updatetracker for files already fully present. It should also detect incomplete files, re-fetch the latest tracker for those files from the server, and resume downloading the remaining bytes.

3. **Final demo starter script** — a timed script is needed that: starts the server and 2 seed peers at t=0, starts peers 3–8 at t=30s (each runs LIST then GET for both files), starts peers 9–13 at t=90s following the same flow, then terminates peer1 and peer2 printing a termination message.

4. **Demo large-file chunk bug** — the current demo.py sends one request for the entire large file in a single shot, which the peer server rejects since it enforces the 1024-byte limit. The demo needs to request the file in 1024-byte chunks.

5. **Stale tracker re-fetch on all-peers-fail** — when every peer in the tracker file is unreachable, the download currently gives up. It should re-fetch a fresh tracker from the server and retry with any newly listed peers.

6. **Dead peer removal** — pruning of dead peers only happens during an updatetracker call. The spec requires a peer to be considered dead after exactly one missed update interval, and removal should not depend on another peer triggering an update.

7. **Large file size for final demo** — the Makefile generates a 100KB file. The final demo requires the large file to take at least 1 min 20 sec to download, so the file needs to be significantly larger.
