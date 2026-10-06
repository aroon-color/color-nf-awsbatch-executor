#!/usr/bin/env bash
set -euo pipefail
plugin_root=$(cd "$(dirname "$0")/.." && pwd)
export NXF_HOME="${NXF_HOME:-$plugin_root/build/validation/nxf-home}"
export NXF_SYNTAX_PARSER=v2
export NXF_DISABLE_CHECK_LATEST=true
export NXF_OFFLINE=true
nextflow_bin=${NEXTFLOW_BIN:-nextflow}
if ! "$nextflow_bin" -version | grep -q 'version 26.04.1 '; then
    echo 'Validation requires Nextflow 26.04.1 and Java 21' >&2
    exit 1
fi
cd "$plugin_root"
./gradlew test installValidationPlugin installAmazonPlugin
for scenario in success isolation ordinary-first final-failure final-host missing-output lost-response ordinary-only; do
    run_dir="$plugin_root/build/validation/$scenario"
    mkdir -p "$run_dir"
    export TIERED_VALIDATION_SCENARIO="$scenario"
    export TIERED_VALIDATION_EVENTS="$run_dir/events.tsv"
    : > "$TIERED_VALIDATION_EVENTS"
    exit_status=0
    (
        cd "$run_dir"
        "$nextflow_bin" -C "$plugin_root/validation/nextflow.config" run "$plugin_root/validation/main.nf" -ansi-log false -w "$run_dir/work"
    ) > "$run_dir/run.log" 2>&1 || exit_status=$?
    case "$scenario" in
        success|isolation|ordinary-first)
            if [ "$exit_status" -ne 0 ]; then cat "$run_dir/run.log"; exit 1; fi
            ;;
        *)
            if [ "$exit_status" -eq 0 ]; then echo "$scenario unexpectedly succeeded" >&2; exit 1; fi
            ;;
    esac
    actual=$(awk -F '\t' '$1 == "taskA" { print $2 }' "$TIERED_VALIDATION_EVENTS" | paste -sd, -)
    expected='fixture-spot,fixture-spot,fixture-spot,fixture-demand'
    case "$scenario" in
        ordinary-first) expected='fixture-spot,fixture-spot,fixture-spot,fixture-spot,fixture-demand' ;;
        lost-response) expected='fixture-spot' ;;
        ordinary-only) expected='fixture-spot,fixture-spot,fixture-spot' ;;
    esac
    if [ "$actual" != "$expected" ]; then
        echo "$scenario: expected $expected; observed $actual" >&2
        cat "$run_dir/run.log"
        exit 1
    fi
    if ! awk -F '\t' '$4 != 1 || $2 != $5 || $6 != "MEMORY=1024" { exit 1 }' "$TIERED_VALIDATION_EVENTS"; then
        echo "$scenario: retry strategy, queue tag, or resource overrides changed" >&2
        exit 1
    fi
    if [ "$scenario" = isolation ]; then
        actual_b=$(awk -F '\t' '$1 == "taskB" { print $2 }' "$TIERED_VALIDATION_EVENTS" | paste -sd, -)
        if [ "$actual_b" != 'fixture-spot,fixture-spot' ]; then
            echo 'Task B did not retain its independent retry budget' >&2
            exit 1
        fi
    fi
    if grep -q 'WARN: Unrecognized config option' "$run_dir/run.log"; then
        cat "$run_dir/run.log"
        exit 1
    fi
    printf '%s: %s (exit %s)\n' "$scenario" "$actual" "$exit_status"
done

# Successful cached tasks must submit no backend jobs during a resumed launch.
export TIERED_VALIDATION_SCENARIO=success
export TIERED_VALIDATION_EVENTS="$plugin_root/build/validation/resume-events.tsv"
: > "$TIERED_VALIDATION_EVENTS"
(
    cd "$plugin_root/build/validation/success"
    "$nextflow_bin" -C "$plugin_root/validation/nextflow.config" run "$plugin_root/validation/main.nf" -ansi-log false -resume -w "$PWD/work"
) > "$plugin_root/build/validation/resume.log" 2>&1
if [ -s "$TIERED_VALIDATION_EVENTS" ]; then
    echo 'Cached tasks submitted new backend jobs on resume' >&2
    exit 1
fi
echo 'resume: no submissions for cached successful tasks'
