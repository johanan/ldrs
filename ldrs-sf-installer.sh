#!/bin/bash
set -euo pipefail

# Simple installer for ldrs-sf binary
# Downloads and installs to ~/.local/bin/ldrs-sf

APP_NAME="ldrs-sf"
DOWNLOAD_URL="${LDRS_SF_DOWNLOAD_URL:-https://github.com/johanan/ldrs/releases/latest/download/ldrs-sf}"

# Ensure HOME is set
if [ -z "${HOME:-}" ]; then
    echo "ERROR: HOME environment variable is not set" >&2
    exit 1
fi

INSTALL_DIR="$HOME/.local/bin"
INSTALL_PATH="$INSTALL_DIR/$APP_NAME"

echo "Installing $APP_NAME to $INSTALL_PATH"

# Create install directory
mkdir -p "$INSTALL_DIR"

# Download binary
echo "Downloading $APP_NAME from $DOWNLOAD_URL"
if command -v curl >/dev/null 2>&1; then
    curl -sSfL "$DOWNLOAD_URL" -o "$INSTALL_PATH"
elif command -v wget >/dev/null 2>&1; then
    wget -q "$DOWNLOAD_URL" -O "$INSTALL_PATH"
else
    echo "ERROR: Neither curl nor wget found. Please install one of them." >&2
    exit 1
fi

# Make executable
chmod +x "$INSTALL_PATH"

echo "✓ $APP_NAME installed successfully to $INSTALL_PATH"

# Check if ~/.local/bin is in PATH
if [[ ":$PATH:" != *":$INSTALL_DIR:"* ]]; then
    echo ""
    echo "NOTE: $INSTALL_DIR is not in your PATH."
    echo "Add this to your shell profile (.bashrc, .zshrc, etc.):"
    echo "  export PATH=\"\$HOME/.local/bin:\$PATH\""
    echo ""
    echo "Or restart your shell if it's already configured."
fi

echo "Run '$APP_NAME --help' to get started."