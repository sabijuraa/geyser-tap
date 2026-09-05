#!/bin/bash
# Integration test script for geyser-tap plugin
#
# Prerequisites:
#   - Solana CLI installed (solana-test-validator, solana)
#   - Plugin built (cargo build --release)
#   - WSL or Linux environment
#
# Usage:
#   ./scripts/integration-test.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
PLUGIN_PATH="$PROJECT_DIR/target/release/libgeyser_tap_plugin.so"
# config/test-validator.json never existed; test-config.json is the real one.
CONFIG_PATH="${CONFIG_PATH:-$PROJECT_DIR/test-config.json}"

# The validator MUST match solana-geyser-plugin-interface (1.18) and be built
# with the same rustc as the plugin, or it segfaults on load. Whatever is on
# PATH is often a different major version, so allow an explicit override:
#   VALIDATOR_BIN=/path/to/1.18.26/solana-test-validator ./scripts/integration-test.sh
VALIDATOR_BIN="${VALIDATOR_BIN:-solana-test-validator}"
TEST_LEDGER_DIR="/tmp/geyser-tap-test-ledger"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

cleanup() {
    log_info "Cleaning up..."
    if [ -n "$VALIDATOR_PID" ] && kill -0 "$VALIDATOR_PID" 2>/dev/null; then
        kill "$VALIDATOR_PID" 2>/dev/null || true
        wait "$VALIDATOR_PID" 2>/dev/null || true
    fi
    rm -rf "$TEST_LEDGER_DIR"
}

trap cleanup EXIT

# Check prerequisites
check_prerequisites() {
    log_info "Checking prerequisites..."

    if ! command -v "$VALIDATOR_BIN" &> /dev/null; then
        log_error "$VALIDATOR_BIN not found. Install Solana CLI or set VALIDATOR_BIN."
        log_info "Run: sh -c \"\$(curl -sSfL https://release.solana.com/stable/install)\""
        exit 1
    fi

    if ! command -v solana &> /dev/null; then
        log_error "solana CLI not found. Install Solana CLI first."
        exit 1
    fi

    if [ ! -f "$PLUGIN_PATH" ]; then
        log_error "Plugin not found at $PLUGIN_PATH"
        log_info "Build the plugin first: cargo build --release"
        exit 1
    fi

    log_info "Prerequisites OK"
}

# Create test config
create_test_config() {
    log_info "Creating test configuration..."

    cat > "$CONFIG_PATH" << 'EOF'
{
  "libpath": "",
  "plugin": {
    "channel_capacity": 10000,
    "worker_threads": 2
  },
  "grpc": {
    "enabled": true,
    "bind_address": "127.0.0.1:10000",
    "max_connections": 10,
    "send_buffer_size": 1000,
    "filters": {
      "accounts": true,
      "transactions": true,
      "slots": true,
      "entries": true,
      "block_metadata": true
    }
  },
  "kafka": {
    "enabled": false,
    "brokers": "localhost:9092",
    "topic": "geyser-tap-test",
    "producer_config": {},
    "filters": {
      "accounts": true,
      "transactions": true,
      "slots": true,
      "entries": false,
      "block_metadata": true
    }
  },
  "websocket": {
    "enabled": true,
    "bind_address": "127.0.0.1:10001",
    "max_clients": 10,
    "send_buffer_size": 1000,
    "filters": {
      "accounts": true,
      "transactions": true,
      "slots": true,
      "entries": true,
      "block_metadata": true
    }
  }
}
EOF

    # Update libpath in config
    sed -i "s|\"libpath\": \"\"|\"libpath\": \"$PLUGIN_PATH\"|" "$CONFIG_PATH"

    log_info "Config created at $CONFIG_PATH"
}

# Start test validator with plugin
start_validator() {
    log_info "Starting solana-test-validator with geyser-tap plugin..."

    rm -rf "$TEST_LEDGER_DIR"
    mkdir -p "$TEST_LEDGER_DIR"

    "$VALIDATOR_BIN" \
        --ledger "$TEST_LEDGER_DIR" \
        --geyser-plugin-config "$CONFIG_PATH" \
        --quiet \
        &

    VALIDATOR_PID=$!

    # Wait for validator to start
    log_info "Waiting for validator to start (PID: $VALIDATOR_PID)..."
    sleep 5

    # Check if validator is running
    if ! kill -0 "$VALIDATOR_PID" 2>/dev/null; then
        log_error "Validator failed to start"
        cat "$TEST_LEDGER_DIR/validator.log" 2>/dev/null || true
        exit 1
    fi

    log_info "Validator started successfully"
}

# Run test transactions
run_tests() {
    log_info "Running test transactions..."

    # Set RPC URL
    export SOLANA_RPC_URL="http://127.0.0.1:8899"

    # Generate test keypair
    local TEST_KEYPAIR="/tmp/geyser-tap-test-keypair.json"
    solana-keygen new --no-bip39-passphrase -o "$TEST_KEYPAIR" --force --silent

    # Airdrop some SOL
    log_info "Requesting airdrop..."
    solana airdrop 10 "$TEST_KEYPAIR" --url "$SOLANA_RPC_URL" || {
        log_warn "Airdrop failed, validator may need more time to initialize"
        sleep 5
        solana airdrop 10 "$TEST_KEYPAIR" --url "$SOLANA_RPC_URL"
    }

    # Send some transactions
    log_info "Sending test transactions..."
    for i in {1..5}; do
        solana transfer --from "$TEST_KEYPAIR" --url "$SOLANA_RPC_URL" \
            "$(solana-keygen pubkey "$TEST_KEYPAIR")" 0.001 --allow-unfunded-recipient \
            2>/dev/null || true
        sleep 1
    done

    log_info "Test transactions sent"

    # Give time for processing
    sleep 3

    rm -f "$TEST_KEYPAIR"
}

# Verify plugin output
verify_output() {
    log_info "Verifying plugin output..."

    # Check gRPC endpoint
    if command -v grpcurl &> /dev/null; then
        log_info "Checking gRPC endpoint..."
        grpcurl -plaintext 127.0.0.1:10000 list 2>/dev/null && log_info "gRPC: OK" || log_warn "gRPC: Not responding"
    else
        log_warn "grpcurl not installed, skipping gRPC check"
    fi

    # Check WebSocket endpoint
    if command -v websocat &> /dev/null; then
        log_info "Checking WebSocket endpoint..."
        timeout 2 websocat ws://127.0.0.1:10001 2>/dev/null && log_info "WebSocket: OK" || log_warn "WebSocket: Not responding"
    else
        log_warn "websocat not installed, skipping WebSocket check"
    fi

    # Check validator logs for plugin activity
    log_info "Checking validator logs for plugin activity..."
    if [ -f "$TEST_LEDGER_DIR/validator.log" ]; then
        if grep -q "geyser-tap" "$TEST_LEDGER_DIR/validator.log" 2>/dev/null; then
            log_info "Plugin messages found in validator logs"
            grep "geyser-tap" "$TEST_LEDGER_DIR/validator.log" | tail -10
        else
            log_warn "No plugin messages found in logs"
        fi
    fi
}

# Main
main() {
    log_info "=== Geyser-Tap Integration Test ==="

    check_prerequisites
    create_test_config
    start_validator
    run_tests
    verify_output

    log_info "=== Integration test completed ==="
    log_info "Validator is still running. Press Ctrl+C to stop."

    # Keep running until interrupted
    wait "$VALIDATOR_PID"
}

main "$@"
