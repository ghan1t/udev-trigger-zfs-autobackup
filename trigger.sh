#!/usr/bin/env bash

set -euo pipefail

SCRIPT_NAME=$(basename "${BASH_SOURCE[0]}")
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
VENV="${SCRIPT_DIR}/venv"
VENV_PYTHON="${VENV}/bin/python3"
PROG="trigger.py"

usage() {
    cat <<EOF
Usage: ${SCRIPT_NAME} [-h] [--install] ...

Wrapper for ${PROG}, which triggers zfs-autobackup when attaching backup disk.

-h, --help                       Print this help and exit
-i, --install                    Install or update dependencies

Other arguments are passed as-is to ${PROG}.

EOF

    if [ -x "${VENV_PYTHON}" ]; then
        exec "${VENV_PYTHON}" "${SCRIPT_DIR}/$PROG" --help
    else
        exit 0
    fi
}

# Function to install Python dependencies
install_dependencies() {
    # Check if the virtual environment directory exists
    if [ -d "${VENV}" ]; then
        echo "Virtual environment already exists. Updating dependencies."
    else
        echo "Creating Python virtual environment..."
        # Create Python virtual environment (isolated from Python installation on TrueNAS SCALE)
        # Use --without-pip because ensurepip is not available.
        python3 -m venv "${VENV}" --without-pip

        # Install pip inside virtual environment
        curl -fSL https://bootstrap.pypa.io/get-pip.py | "$VENV_PYTHON"
    fi

    # Install our dependencies inside the virtual environment
    "$VENV_PYTHON" -m pip install -r "${SCRIPT_DIR}/requirements.txt"
}

if [[ "$#" == 0 ]]; then
    usage
fi

for arg in "$@"; do
    case "$arg" in
        -h | --help)
            usage
            ;;
        -i | --install)
            install_dependencies
            shift
            ;;
        *) continue ;;
    esac
done

if [[ "$#" > 0 ]]; then
    exec "${VENV_PYTHON}" "${SCRIPT_DIR}/$PROG" "$@"
fi
