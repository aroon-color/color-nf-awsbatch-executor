# Compatibility and validation record

Validated locally on 2026-10-07 with Nextflow **26.04.1**, nf-amazon **3.9.1**,
Java **21.0.1**, Gradle **8.14**, and the Nextflow Gradle plugin **1.0.0-beta.15**.
These versions are the supported baseline.

## Extension feasibility

Production subclasses AwsBatchExecutor and AwsBatchTaskHandler, retaining upstream
job definitions, staging, cancellation, polling, API throttling, and resources.
Protected request creation, description, completion, and trace hooks support the
policy. The actual Nextflow runtime loaded the installed executor and its pinned
nf-amazon dependency. Production archives exclude the test-only backend; verification overlays it only into an isolated plugin home.

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

## Infrastructure retry update (0.2.0)

The integration run `integration-test-V2026-10-07-14-19-03` showed AWS CLI S3
`download failed` diagnostics containing `ConnectionResetError(104, 'Connection reset
by peer')`, while Batch reported only `Essential container in task exited`. These
failures previously consumed the application allowance. Version 0.2.0 recognizes
such transient staging failures from bounded diagnostic tails and accounts for them
separately. Three infrastructure failures trigger the final on-demand execution;
three Spot host interruptions still trigger it independently. The core retry ceiling
must include both thresholds and the application allowance (default 8).

Regression scenarios cover stage-in, stage-out with a successful application exit
file but failed wrapper/container, mixed failure budgets, final infrastructure failure,
and permanent AccessDenied failures. Both CloudWatch strings and staged diagnostic
paths are exercised, including errors beyond the first 64 KiB of a worker log.
No clinical sample identifiers or private logs are included in the fixtures.

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
instead of the pinned release. The former Color smoke script was removed; plugin verification remains standalone.
The standalone retry matrix also passed with NXF_OFFLINE=true and its preinstalled
nf-amazon dependency. The original standalone Linux CI verification passed. V1 rejects Fusion, arrays, Fargate,
pre-existing job definitions, and multi-container/multi-node overrides. New resumed
launches reset unfinished-task budgets. Host EC2 termination reasons on verified
Spot-only queues are evidence, not independent proof of an EC2 reclaim notification.

Reporting validation additionally covers deduplicated attempt snapshots, three interruptions vs
two Spot retries, distinct shared hosts, memory/CPU allocation, unknown prices, per-host/task
cost sums, failed workflow summaries, zero incremental cost on cached resume, and upstream AWS
Batch reporting without tiered scheduling. EC2/ECS/pricing SDK request tests use deterministic
external responses; real Nextflow fixtures use explicit one-hour attempts and host/rate metadata.
The production package includes only the Pricing SDK module and resolves shared SDK classes from
nf-amazon, avoiding duplicate classloaders. AWS account billing reconciliation is not performed.
