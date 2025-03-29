#!/bin/bash
# Script to start language daemons for PipeLink

echo "Starting PipeLink language daemons..."
set -e

# Check for required language runtimes
PYTHON_OK=true
R_OK=true
JULIA_OK=true

echo "Checking language runtimes..."

# Check Python
if ! command -v python3 >/dev/null 2>&1; then
    echo "Warning: Python 3 not found. Python daemon will not be started."
    PYTHON_OK=false
fi

# Check R
if ! command -v Rscript >/dev/null 2>&1; then
    echo "Warning: R not found. R daemon will not be started."
    R_OK=false
fi

# Check Julia
if ! command -v julia >/dev/null 2>&1; then
    echo "Warning: Julia not found. Julia daemon will not be started."
    JULIA_OK=false
fi

# Prepare command to start daemons
LANGUAGES=""

if [ "$PYTHON_OK" = true ]; then
    LANGUAGES="python"
fi

if [ "$R_OK" = true ]; then
    if [ -n "$LANGUAGES" ]; then
        LANGUAGES="$LANGUAGES,r"
    else
        LANGUAGES="r"
    fi
fi

if [ "$JULIA_OK" = true ]; then
    if [ -n "$LANGUAGES" ]; then
        LANGUAGES="$LANGUAGES,julia"
    else
        LANGUAGES="julia"
    fi
fi

if [ -z "$LANGUAGES" ]; then
    echo "Error: No supported language runtimes found. Cannot start any daemons."
    exit 1
fi

echo "Starting daemons for: $LANGUAGES"

# Start the daemons using pipelink CLI
pipelink daemon start --languages $LANGUAGES

echo "Daemons started successfully!"
echo ""
echo "To check daemon status, run: pipelink daemon status"
echo "To stop daemons, run: pipelink daemon stop"
echo ""
echo "Daemons will remain running until explicitly stopped or the system is restarted."
echo "They will handle cross-language pipeline operations without the startup overhead." 