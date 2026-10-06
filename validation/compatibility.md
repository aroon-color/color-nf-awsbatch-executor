# Compatibility and validation record

Validated locally on 2026-10-06 with Nextflow **26.04.1**, nf-amazon **3.9.1**,
Java **21.0.1**, Gradle **8.14**, and the Nextflow Gradle plugin **1.0.0-beta.15**.
These versions are the supported baseline.

## Extension feasibility

Production subclasses AwsBatchExecutor and AwsBatchTaskHandler, retaining upstream
job definitions, staging, cancellation, polling, API throttling, and resources.
Protected request creation, description, completion, and trace hooks support the
policy. The actual Nextflow runtime loaded the installed executor and its pinned
nf-amazon dependency. Production archives exclude the separate test-only backend.

Core retry copies get new task IDs but preserve process name and task index, which
form the session-scoped logical identity. State is released at shutdown. Core
attempt counts reconcile ordinary errors detected after backend success.
Nextflow 26.04.1 interprets maxRetries=0 as one; the final handler instead sets
errorStrategy=terminate before execution. Tests deliberately use maxRetries=20
and prove final backend failures and missing outputs produce no fifth submission.

A request-scoped SDK plugin forces SubmitJob maximum attempts one. A real SDK
client allowing three attempts sends exactly one request to a local HTTP server
returning a retryable failure. Batch retryStrategy.attempts is independently one.
Submission errors, including a lost response after acceptance, are unrecoverable.

## Automated verification

scripts/verify.sh builds/tests the plugin and runs real Nextflow scheduling against
an isolated external AWS fixture. The complete matrix passed:

| Scenario                                   | Task A queues                     | Outcome                           |
| ------------------------------------------ | --------------------------------- | --------------------------------- |
| Three interruptions                        | Spot, Spot, Spot, on-demand       | Success                           |
| Task isolation                             | Spot, Spot, Spot, on-demand       | Success; Task B stays Spot twice  |
| Ordinary failure then three interruptions  | Spot, Spot, Spot, Spot, on-demand | Success                           |
| Final application failure                  | Spot, Spot, Spot, on-demand       | Workflow failure                  |
| Final host termination                     | Spot, Spot, Spot, on-demand       | Workflow failure                  |
| Final missing output after backend success | Spot, Spot, Spot, on-demand       | Workflow failure                  |
| Lost SubmitJob response                    | Spot                              | Workflow failure; no resubmission |
| Ordinary failures only, allowance two      | Spot, Spot, Spot                  | Workflow failure                  |
| Successful cached -resume                  | No new submissions                | Success                           |

Accepted fixture jobs verify Batch attempts=1, effective queue tags, and requested
memory. Unit tests cover classification, independent budgets, failure deduplication,
output-validation reconciliation, invalid configuration/queues, and SDK suppression.
Workflow lint passes without errors/warnings; runtime logs have no unknown plugin
configuration warnings. Scripts pass Bash syntax checking.

scripts/verify-extraction.sh passed the same matrix after copying only this package
outside Color, unsetting Color environment references, using a fresh Nextflow home,
and rebuilding. No package symlinks refer to the monorepo. Ignored build/validation
and build/extraction directories contain generated reports.

Color integration checks passed: 51 configuration/launcher tests including the
existing exact legacy-rendering test and config-free AWS submission, 24 CI parser
tests, and Ruff. Both base and production
consumer settings now enable tiered retries and include mappings for all task queues.
Read-only AWS inspection confirmed the configured source queues are enabled/valid
SPOT-only and the existing leader destinations are enabled/valid EC2-only in both
environments; source and destination share their instance role and general families.
The destination has a 2048-vCPU ceiling and is also used by leaders.
IAM policy simulation confirmed both existing leader instance roles allow
batch:DescribeJobQueues and batch:DescribeComputeEnvironments.

## External validation still required

No AWS jobs, queue changes, plugin registry publication, or deployment were performed. Before enabling,
validate real queue mappings, run a small S3-backed container job, force controlled
host interruptions, and verify staging, native IDs/queue trace fields, roles,
cancellation, and resources. Confirm destination architecture, instance memory/CPU,
network, and volumes separately: queue enabled/type checks do not prove suitability.

The complete linux/amd64 leader image built locally as
color-nextflow-tiered:validation. Its offline smoke test verified Nextflow 26.04.1,
all three bundled plugins, the generated AWS executor/enablement, and actual
executor discovery (using disabled configuration to stop before any AWS API).
The image compiles portable bytecode in a native JDK stage and copies no test plugin.
The version-check step replaces self-update, which was observed to install 26.04.6
instead of the pinned release. Reproduce the offline check by running
pipeline_nextflow_leader/tests/smoke_tiered_executor.sh inside the image with Bash.
The standalone retry matrix also passed with NXF_OFFLINE=true and its preinstalled
nf-amazon dependency. The Linux CI job still needs its normal CI execution. V1 rejects Fusion, arrays, Fargate,
pre-existing job definitions, and multi-container/multi-node overrides. New resumed
launches reset unfinished-task budgets. Host EC2 termination reasons on verified
Spot-only queues are evidence, not independent proof of an EC2 reclaim notification.
