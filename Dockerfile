# Geyser-Tap Plugin Build Image
#
# This Dockerfile builds the geyser-tap plugin for deployment.
# The resulting .so file can be used with Solana validators.
#
# Build:
#   docker build -t geyser-tap:latest .
#
# Extract the plugin:
#   docker run --rm -v $(pwd)/output:/output geyser-tap:latest \
#     cp /build/target/release/libgeyser_tap_plugin.so /output/

# =============================================================================
# Stage 1: Build Environment
# =============================================================================
# Rust 1.75 is REQUIRED, not a preference. The plugin exports
# `_create_plugin() -> *mut dyn GeyserPlugin`, a Rust trait-object fat pointer
# handed across dlopen. Vtable layout is not ABI-stable across compiler
# versions, so a plugin built with a different rustc than the target validator
# segfaults the validator on load. solana-test-validator 1.18.26 is built with
# rustc 1.75.0. This previously pinned 1.82 and produced a .so that crashed.
# Keep in sync with rust-toolchain.toml.
FROM rust:1.75-bookworm AS builder

# Install build dependencies
RUN apt-get update && apt-get install -y \
    cmake \
    protobuf-compiler \
    libprotobuf-dev \
    libssl-dev \
    libsasl2-dev \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

# Create build directory
WORKDIR /build

# Copy manifests first for dependency caching
COPY Cargo.toml Cargo.lock ./
COPY crates/plugin/Cargo.toml crates/plugin/
COPY crates/sink-grpc/Cargo.toml crates/sink-grpc/
COPY crates/sink-kafka/Cargo.toml crates/sink-kafka/
COPY crates/sink-ws/Cargo.toml crates/sink-ws/
COPY crates/common/Cargo.toml crates/common/
COPY crates/proto/Cargo.toml crates/proto/
COPY crates/sdk/Cargo.toml crates/sdk/

# Create dummy source files for dependency compilation
RUN mkdir -p crates/plugin/src crates/sink-grpc/src crates/sink-kafka/src \
    crates/sink-ws/src crates/common/src crates/proto/src crates/sdk/src && \
    echo "fn main() {}" > crates/plugin/src/lib.rs && \
    echo "fn main() {}" > crates/sink-grpc/src/lib.rs && \
    echo "fn main() {}" > crates/sink-kafka/src/lib.rs && \
    echo "fn main() {}" > crates/sink-ws/src/lib.rs && \
    echo "fn main() {}" > crates/common/src/lib.rs && \
    echo "fn main() {}" > crates/proto/src/lib.rs && \
    echo "fn main() {}" > crates/sdk/src/lib.rs

# Build dependencies only (cached layer)
RUN cargo build --release || true

# Remove dummy sources
RUN rm -rf crates/*/src

# Copy actual source code
COPY crates crates/

# Copy proto build script
COPY crates/proto/build.rs crates/proto/

# Build the actual plugin
RUN cargo build --release

# Verify the plugin was built
RUN ls -la target/release/libgeyser_tap_plugin.so

# =============================================================================
# Stage 2: Runtime Image (minimal)
# =============================================================================
FROM debian:bookworm-slim AS runtime

# Install runtime dependencies only
RUN apt-get update && apt-get install -y \
    libssl3 \
    libsasl2-2 \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN useradd -r -s /bin/false geyser

# Copy the plugin
COPY --from=builder /build/target/release/libgeyser_tap_plugin.so /opt/geyser-tap/

# Set permissions
RUN chown -R geyser:geyser /opt/geyser-tap

# Default command shows usage
CMD ["echo", "Plugin available at /opt/geyser-tap/libgeyser_tap_plugin.so"]

# =============================================================================
# Stage 3: Development Image (with tools)
# =============================================================================
FROM builder AS dev

# Install development tools
RUN apt-get update && apt-get install -y \
    gdb \
    valgrind \
    strace \
    curl \
    jq \
    kafkacat \
    grpcurl \
    && rm -rf /var/lib/apt/lists/*

# Install cargo tools
RUN cargo install cargo-watch cargo-expand

WORKDIR /build

CMD ["cargo", "watch", "-x", "build"]
