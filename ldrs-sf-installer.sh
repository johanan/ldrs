#!/bin/bash
set -eu
set -o pipefail 2>/dev/null || true

# Simple installer for ldrs-sf binary
# Downloads tar archive and installs to ~/.local/bin/ldrs-sf

APP_NAME="ldrs-sf"

# Determine platform (default to x86_64-unknown-linux-gnu)
PLATFORM="${LDRS_SF_PLATFORM:-x86_64-unknown-linux-gnu}"
TAR_NAME="$APP_NAME-$PLATFORM.tar.gz"
DOWNLOAD_URL="${LDRS_SF_DOWNLOAD_URL:-https://github.com/johanan/ldrs/releases/latest/download/$TAR_NAME}"

# Ensure HOME is set
if [ -z "${HOME:-}" ]; then
    echo "ERROR: HOME environment variable is not set" >&2
    exit 1
fi

INSTALL_DIR="$HOME/.local/bin"
INSTALL_PATH="$INSTALL_DIR/$APP_NAME"
TEMP_DIR=$(mktemp -d)

echo "Installing $APP_NAME ($PLATFORM) to $INSTALL_PATH"

# Cleanup function
cleanup() {
    rm -rf "$TEMP_DIR"
}
trap cleanup EXIT

# Create install directory
mkdir -p "$INSTALL_DIR"

# Download tar archive
echo "Downloading $TAR_NAME from $DOWNLOAD_URL"
if command -v curl >/dev/null 2>&1; then
    curl -sSfL "$DOWNLOAD_URL" -o "$TEMP_DIR/$TAR_NAME"
elif command -v wget >/dev/null 2>&1; then
    wget -q "$DOWNLOAD_URL" -O "$TEMP_DIR/$TAR_NAME"
else
    echo "ERROR: Neither curl nor wget found. Please install one of them." >&2
    exit 1
fi

# Extract tar and install binary
echo "Extracting $TAR_NAME"
cd "$TEMP_DIR"
tar -xzf "$TAR_NAME"

# Find and copy the binary
if [ -f "$APP_NAME" ]; then
    cp "$APP_NAME" "$INSTALL_PATH"
    chmod +x "$INSTALL_PATH"
else
    echo "ERROR: Binary $APP_NAME not found in tar archive" >&2
    exit 1
fi

echo "✓ $APP_NAME installed successfully to $INSTALL_PATH"

# Check if ~/.local/bin is in PATH
case ":$PATH:" in
    *":$INSTALL_DIR:"*)
        # Already in PATH, do nothing
        ;;
    *)
        echo ""
        echo "NOTE: $INSTALL_DIR is not in your PATH."
        echo "Add this to your shell profile (.bashrc, .zshrc, etc.):"
        echo "  export PATH=\"\$HOME/.local/bin:\$PATH\""
        echo ""
        echo "Or restart your shell if it's already configured."
        ;;
esac

echo "Run '$APP_NAME --help' to get started."
