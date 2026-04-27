.PHONY: all setup clean run-tracker run-peer1 run-peer2 run-peer3 demo final-demo final-setup

PYTHON:= python

PEER_SRC = peer.py

all: setup

setup:
	@echo "Setting up peer directories..."
	@cp $(PEER_SRC) peer1/peer.py
	@cp $(PEER_SRC) peer2/peer.py
	@cp $(PEER_SRC) peer3/peer.py
	@echo "Creating sample test files..."
	@echo "Hello, this is a small test file for P2P sharing." > peer1/shared/testfile.txt
	@dd if=/dev/urandom of=peer2/shared/largefile.bin bs=1024 count=100 2>/dev/null
	@echo "Setup complete!"
	@echo ""
	@echo "To run:"
	@echo "  Terminal 1:  cd tracker && $(PYTHON) tracker.py"
	@echo "  Terminal 2:  cd peer1   && $(PYTHON) peer.py"
	@echo "  Terminal 3:  cd peer2   && $(PYTHON) peer.py"
	@echo "  Terminal 4:  cd peer3   && $(PYTHON) peer.py"

final-setup:
	@echo "Setting up for final demo (13 peers)..."
	@mkdir -p peer1/shared peer2/shared peer3/shared peer4/shared peer5/shared peer6/shared peer7/shared peer8/shared peer9/shared peer10/shared peer11/shared peer12/shared peer13/shared
	@echo "Copying peer.py to all peer directories..."
	@for i in 1 2 3 4 5 6 7 8 9 10 11 12 13; do \
	    cp $(PEER_SRC) peer$$i/peer.py; \
	done
	@echo "Creating sample test files for Peer1 and Peer2..."
	@echo "Hello, this is a small test file for P2P sharing." > peer1/shared/testfile.txt
	@echo "Small file size: $$(wc -c < peer1/shared/testfile.txt) bytes"
	@dd if=/dev/urandom of=peer2/shared/largefile.bin bs=1024 count=325 2>/dev/null
	@echo "Large file size: $$(wc -c < peer2/shared/largefile.bin) bytes"
	@echo ""
	@echo "Final demo setup complete!"
	@echo ""
	@echo "To run the final demo:"
	@echo "  make final-demo"
	@echo "  optional: add '|& tee final_demo.log' to the command to save output to a log file"

clean:
	@echo "Cleaning up peer directories..."
	@for i in 1 2 3 4 5 6 7 8 9 10 11 12 13; do \
	    rm -f peer$$i/peer.py; \
        rm -rf peer$$i/cache; \
        rm -rf peer$$i/shared/*; \
    done
	@echo "Cleaning tracker files..."
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

final-demo: final-setup
	@$(PYTHON) final_demo.py
