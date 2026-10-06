# nf-tiered-batch

This repository builds the Nextflow plugin `nf-tiered-awsbatch`.

## Summary

Nextflow executor `tiered-awsbatch` extends `nf-amazon` and counts host interruption
failures separately for each logical task. After three such failures on a Spot-only
queue, it submits one final execution on the mapped on-demand-only queue. A failed
final execution terminates the workflow, including failures detected by Nextflow
after Batch succeeds (for example, missing outputs).

The executor retains upstream job definitions, staging, cancellation, resource
requests, polling, and API throttling. Each execution is a distinct Nextflow attempt
and Batch job. It forces one Batch execution per submission and disables SDK retries
on SubmitJob. If a submission fails or its response is lost, execution stops: inspect
accepted jobs before resuming rather than risking duplicate submissions.

## Get started

Supported versions are **Nextflow 26.04.1**, **nf-amazon 3.9.1**, and **Java 21**.
This plugin is not published to a registry. Build and install it locally first:

```bash
./gradlew test installPlugin installAmazonPlugin
```

Set `NXF_HOME` consistently for Gradle installation and Nextflow execution. The
upstream plugin dependency is downloaded from a pinned public OCI blob, verified
with SHA-512, and used only for compilation/tests. Production uses the separately
installed `nf-amazon` plugin through its declared plugin dependency.

## Example

```groovy
plugins {
    id 'nf-amazon@3.9.1'
    id 'nf-tiered-awsbatch@0.1.0'
}

process {
    executor = 'tiered-awsbatch'
    container = 'ubuntu:24.04'
    queue = 'analysis-spot'
    errorStrategy = 'retry'
    maxRetries = 5
    maxErrors = -1
}

tieredAwsBatch {
    enabled = true
    interruptionThreshold = 3
    ordinaryRetryAllowance = 2
    queueMappings = ['analysis-spot': 'analysis-on-demand']
}

aws.region = 'us-east-1'
workDir = 's3://example-work-bucket/nextflow'
```

Use ordinary AWS credential providers and upstream `aws.batch` options for CLI
paths, volumes, roles, staging, and logging. Queue mappings are explicit; each source
must contain only enabled, valid SPOT compute environments and each destination only
enabled, valid EC2 compute environments in the configured region. Startup validates
those properties via Batch DescribeJobQueues and DescribeComputeEnvironments.
Operators must also ensure destination instances support the task's architecture,
memory/CPU, container image, roles, network, and volume requirements; no capacity or
permission guarantee is inferred from the queue being enabled.

Use static `errorStrategy = 'retry'`, `maxErrors = -1`, and a core retry ceiling at
least `interruptionThreshold + ordinaryRetryAllowance`. Incompatible per-process
overrides fail clearly rather than silently defeating the policy. The final attempt
receives task-level `errorStrategy = 'terminate'`; Nextflow 26.04.1 interprets zero
maxRetries as one, so zero is not used as a retry guard.

Ordinary failures have their own allowance. With allowance two, a third ordinary
failure ends the task. They never increment interruption counts; mixed failures may
produce more than three Spot submissions. Host EC2 termination reasons on verified
Spot-only queues are the classifier evidence, matching upstream AWS retry practice.
This does not independently distinguish reclaim notifications from every other host
termination. Codes 137/143 alone never trigger fallback.

## Supported scope and resume

V1 supports Docker-image-based, single-container EC2 jobs only. It rejects arrays,
pre-existing `job-definition://` references, Fargate, and Fusion. Mixed Spot/on-demand
source queues are unsupported. Backend retries in job definitions are overridden
with attempts=1. User cancellation remains the upstream cancellation path.

State is shared among executor instances within one Nextflow launch and released
at shutdown. Identity uses fully qualified process name and task index, which remain
stable when core retry copies get new task IDs. Every new `-resume` invocation starts
fresh budgets for unfinished tasks. Successful cached tasks do not submit jobs.
Final failures stop the workflow earlier than an ignore-and-finish policy would.

## Build and verify

Run from this directory; no Color checkout, Python environment, or root build is
required. Java 21, a pinned Nextflow executable, Bash, and standard Unix tools are
required for the complete verification entry point. Gradle and declared Maven/OCI
dependencies download on first use.

```bash
./gradlew test packagePlugin
NEXTFLOW_BIN=/absolute/path/to/nextflow ./scripts/verify.sh
NEXTFLOW_BIN=/absolute/path/to/nextflow ./scripts/verify-extraction.sh
```

Verification loads a separate test-only plugin that replaces the external AWS
boundary, runs real Nextflow scheduling/retries/output validation, and asserts queue
sequences, tags, resources, final failures, isolation, and cache behavior. Production
archives never contain this fixture or its executor. SDK submission tests use a
local HTTP server to prove that a retrying client sends SubmitJob only once.

`verify-extraction.sh` copies only this package outside the checkout and uses a fresh
Nextflow plugin home. Neither command launches AWS jobs. Results are under
`build/validation/` and `build/extraction/`. Runtime compatibility findings and exact
verification outcomes are recorded in `validation/compatibility.md`.

## Color integration

Color's consumer supplies `pipeline.tiered_aws_batch.enabled` (default true) and
`queue_mappings`, rendering generic Nextflow configuration. The leader image builds
a pinned commit of this repository in a separate JDK stage and copies the compiled executor plus pinned
nf-amazon dependency into its runtime `NXF_HOME`. Both environment defaults include
all existing task and demux queue mappings to the existing on-demand leader queue.
AWS launchers generate the templated config when callers omit one, so no plugin
installation, extra enablement flag, or custom config is required in the leader.
The AWS profile overrides the structural-variant module's legacy ignore policy. Existing local/stub
execution is unchanged. Disable the consumer setting to restore built-in awsbatch
for future launches; do not migrate active jobs as part of rollback.

## License

Original Color code retains its internal proprietary rights. See LICENSE and NOTICE
for third-party provenance. Public source availability does not grant an open-source redistribution license.
No plugin registry publication is included.
