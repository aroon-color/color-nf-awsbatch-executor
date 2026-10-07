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
for scenario in success isolation ordinary-first final-failure final-host missing-output lost-response ordinary-only infra-stage-in infra-stage-out infra-mixed infra-final infra-permanent; do
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
        success|isolation|ordinary-first|infra-stage-in|infra-stage-out|infra-mixed)
            if [ "$exit_status" -ne 0 ]; then cat "$run_dir/run.log"; exit 1; fi
            ;;
        *)
            if [ "$exit_status" -eq 0 ]; then echo "$scenario unexpectedly succeeded" >&2; exit 1; fi
            ;;
    esac
    python3 - "$run_dir/tiered-batch-summary.json" "$scenario" <<'PYTEST'
import json, sys
with open(sys.argv[1]) as source: summary = json.load(source)
assert summary['complete'] is True, summary
if sys.argv[2] == 'success':
    assert summary['spotInterruptions'] == 3, summary
    assert summary['spotRetries'] == 2, summary
    assert summary['onDemandFallbacks'] == 1, summary
    assert summary['spotInstances'] == 1 and summary['onDemandInstances'] == 1, summary
    assert summary['estimatedCostUsd'] == 0.0675, summary
    assert summary['pricedAttempts'] == 5, summary
PYTEST
    actual=$(awk -F '\t' '$1 == "taskA" { print $2 }' "$TIERED_VALIDATION_EVENTS" | paste -sd, -)
    expected='fixture-spot,fixture-spot,fixture-spot,fixture-demand'
    case "$scenario" in
        ordinary-first) expected='fixture-spot,fixture-spot,fixture-spot,fixture-spot,fixture-demand' ;;
        lost-response) expected='fixture-spot' ;;
        ordinary-only|infra-permanent) expected='fixture-spot,fixture-spot,fixture-spot' ;;
        infra-mixed) expected='fixture-spot,fixture-spot,fixture-spot,fixture-spot,fixture-spot,fixture-demand' ;;
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
    if [[ "$scenario" == infra-* && "$scenario" != infra-permanent ]]; then
        if ! grep -q 'classification=INFRASTRUCTURE;.*infrastructureFailures=3;' "$run_dir/.nextflow.log"; then
            echo "$scenario: missing independent infrastructure threshold" >&2
            exit 1
        fi
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
python3 - "$plugin_root/build/validation/success/tiered-batch-summary.json" <<'PYTEST'
import json, sys
with open(sys.argv[1]) as source: summary = json.load(source)
assert summary['attempts'] == 0 and summary['estimatedCostUsd'] == 0, summary
PYTEST
echo 'resume: no submissions or new compute cost for cached successful tasks'

# Reporting-only mode retains upstream AWS Batch scheduling and never uses a demand queue.
run_dir="$plugin_root/build/validation/builtin"
mkdir -p "$run_dir"
export TIERED_VALIDATION_SCENARIO=success
export TIERED_VALIDATION_EVENTS="$run_dir/events.tsv"
: > "$TIERED_VALIDATION_EVENTS"
sed -e "s/tiered-awsbatch-validation/awsbatch-validation/" -e "s/enabled = true/enabled = false/" "$plugin_root/validation/nextflow.config" > "$run_dir/base.config"
# Turn reporting back on after changing executor/policy flags.
printf '\ntieredAwsBatch.reporting = true\naws.batch.maxSpotAttempts = 1\n' >> "$run_dir/base.config"
(
    cd "$run_dir"
    "$nextflow_bin" -C "$run_dir/base.config" run "$plugin_root/validation/main.nf" -ansi-log false -w "$run_dir/work"
) > "$run_dir/run.log" 2>&1
python3 - "$run_dir/tiered-batch-summary.json" <<'PYTEST'
import json, sys
with open(sys.argv[1]) as source: summary = json.load(source)
assert summary['spotInterruptions'] == 3 and summary['spotRetries'] == 3, summary
assert summary['onDemandAttempts'] == 0 and summary['onDemandFallbacks'] == 0, summary
assert summary['estimatedCostUsd'] == 0.046875, summary
PYTEST
echo 'builtin: reporting enabled; five Spot attempts, no on-demand fallback'
