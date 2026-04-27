.PHONY: all setup clean run-tracker run-peer1 run-peer2 run-peer3 demo

PYTHON = python3
PEER_SRC = peer.py

all: setup

setup:
	@echo "Setting up peer directories..."
	@cp $(PEER_SRC) peer1/peer.py
	@cp $(PEER_SRC) peer2/peer.py
	@cp $(PEER_SRC) peer3/peer.py
	@echo "Creating sample test files..."
	@echo "Hello, this is a small test file for P2P sharing." > peer1/shared/testfile.txt
	@dd if=/dev/urandom of=peer2/shared/largefile.bin bs=1024 count=10240 2>/dev/null
	@echo "Setup complete!"
	@echo ""
	@echo "To run:"
	@echo "  Terminal 1:  cd tracker && $(PYTHON) tracker.py"
	@echo "  Terminal 2:  cd peer1   && $(PYTHON) peer.py"
	@echo "  Terminal 3:  cd peer2   && $(PYTHON) peer.py"
	@echo "  Terminal 4:  cd peer3   && $(PYTHON) peer.py"

clean:
	@rm -f peer1/peer.py peer2/peer.py peer3/peer.py
	@rm -rf peer1/cache peer2/cache peer3/cache
	@rm -rf tracker/torrents/*.track
	@echo "Cleaned."

run-tracker:
	@cd tracker && $(PYTHON) tracker.py

run-peer1:
	@cd peer1 && $(PYTHON) peer.py

run-peer2:
	@cd peer2 && $(PYTHON) peer.py

run-peer3:
	@cd peer3 && $(PYTHON) peer.py

demo: setup
	@$(PYTHON) demo.py
