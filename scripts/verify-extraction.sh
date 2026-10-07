#!/usr/bin/env bash
set -euo pipefail
plugin_root=$(cd "$(dirname "$0")/.." && pwd)
scratch=$(mktemp -d "${TMPDIR:-/tmp}/color-nf-awsbatch-executor-extraction.XXXXXX")
trap 'rm -rf "$scratch"' EXIT
mkdir -p "$scratch/plugin" "$plugin_root/build/extraction"
tar -C "$plugin_root" --exclude='./.git' --exclude='./build' --exclude='./.gradle' -cf - . | tar -C "$scratch/plugin" -xf -
if find "$scratch/plugin" -type l | grep -q .; then
    echo 'Portable package must not contain symlinks back to the checkout' >&2
    exit 1
fi
nextflow_bin=${NEXTFLOW_BIN:-nextflow}
nextflow_bin=$(command -v "$nextflow_bin")
env -u COLOR_ROOT -u CLRENV_PATH -u CLRENV_OVERLAY_PATH \
    NXF_HOME="$scratch/nxf-home" NEXTFLOW_BIN="$nextflow_bin" \
    "$scratch/plugin/scripts/verify.sh" > "$plugin_root/build/extraction/verify.log" 2>&1 || {
        cat "$plugin_root/build/extraction/verify.log"
        exit 1
    }
mkdir -p "$plugin_root/build/extraction/results"
cp -R "$scratch/plugin/build/validation/." "$plugin_root/build/extraction/results/"
echo "Standalone extraction passed; report: $plugin_root/build/extraction/verify.log"
