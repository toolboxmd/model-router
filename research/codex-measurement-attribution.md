# Trustworthy Codex usage and timing attribution

Decision: [Establish trustworthy Codex usage and timing attribution](https://github.com/toolboxmd/model-router/issues/4).
Date: 2026-09-17. Status: research resolved; complete-task sample validation remains separate.

## Answer

Use a versioned, Codex-specific reader over existing evidence. Preserve response identity, turn identity, route configuration, provenance and completeness. Join consumption to an explicit outcome record. Do not infer a business task solely from a conversation or quota window.

This advances Model Router's evidence-based routing Objective. It specifies measurement semantics, not a collector, routing change, service or new proof requirement.

## Evidence scope

The parent inspected one completed local Codex turn. Twelve unique response records sum to its final cumulative total: 957,029 input tokens, including 825,216 cached input, plus 3,279 output tokens including 266 reported reasoning tokens. Uncached input is 131,813; the cached share is 86.2%. The input series grows from 33,247 to 129,876. Recorded start-to-completion time is 181.625 seconds. These counts show consumption and growth, not avoidable waste. Private identities and transcripts are not published.

The sample contains `token_usage_record` with response/session/thread/root-turn/turn identities, response usage and cumulative totals. `turn_context` records model and effort. This establishes the inspected installed format only. The sample is a discussion turn without children or delivery outcome labels; it cannot prove complete-task attribution.

The public [Codex App Server contract](https://learn.chatgpt.com/docs/app-server) independently documents session-tree identity, turn lifecycle, usage updates, compaction and rerouting notifications. Command execution and dynamic tool items can expose `durationMs`; account rate-limit reads expose separate metered buckets. Preserve client version and schema provenance because not every client records every notification.

## Availability and collection rules

| Measurement | Availability | Required treatment |
| --- | --- | --- |
| Response usage and cumulative counters | Direct in inspected log | Count unique response usage; use cumulative values as reconciliation checkpoints. |
| Cached and uncached input | Derived after schema validation | In this Codex schema cached input is a subset of input. Never apply this formula to an unrelated host schema. |
| Reasoning and output | Direct when reported | In inspected Codex records reasoning is included in output; do not add it twice. |
| Cache writes | Format-dependent | Preserve the field and its provenance. Missing means unknown; a recorded zero does not prove every serving path exposes writes. |
| Model and effort | Requested/observed client evidence | Record configuration and rerouting events separately. A CLI flag or client label is not backend attestation. |
| Input growth and compaction | Derived/direct when present | Chart per-response input with compaction and configuration boundaries. Growth or misses alone do not prove waste. |
| Elapsed time | Derived from lifecycle timestamps | Keep model work, full task, human wait and delivery intervals distinct. No terminal event means incomplete duration. |
| Some tool durations | Direct where retained | Reuse existing optional duration fields before proposing new instrumentation. |
| Queue, inference and dependency waits | Partial or unavailable | Timestamp gaps cannot separate causes. Name absent spans rather than assigning all gaps to reasoning. |
| Complete task and child attribution | Requires outcome/dispatch evidence | Use explicit task, attempt and phase labels with native thread/turn links. Validate joins on the next sample. |
| Critical path | Derived only with complete dependency spans | Summed parallel durations are not elapsed time. Missing edges mean unknown critical path. |
| Quota snapshots | Direct from separate account meter | Retain bucket, reset time and observation timestamp; concurrent tasks confound per-task deltas. |
| Actual cost | Unknown unless billed evidence exists | Keep invoice, rate-card estimate, reported free route and incomplete estimate separate. |
| Repeated reads, tests and retries | Observable only with retained tool/attempt evidence | Repetition is a diagnostic signal; determine whether inputs or proof obligations changed. |
| Handoff size and phase consumption | Requires explicit boundaries | Record each handoff and phase. Label estimated token lengths if the tokenizer or rendered request is unavailable. |

[Prompt caching](https://developers.openai.com/api/docs/guides/prompt-caching) depends on eligible matching prefixes, boundaries and model settings. [Reasoning documentation](https://developers.openai.com/api/docs/guides/reasoning) distinguishes reported reasoning from visible output. Use the relevant provider schema and price version instead of universal arithmetic.

## Deduplication, recovery and completeness

1. Preserve raw counters with source, host/version, event timestamp and observation timestamp. Normalize separately.
2. Namespace response identity by provider and recording scope. Repeated copies of the same response must not become new consumption. Conflicting copies remain an explicit discrepancy; do not silently choose or sum them.
3. Add each distinct usage-bearing response once. Do not add snapshots from response, turn, thread and legacy notifications together.
4. Reconcile with cumulative checkpoints. Investigate discrepancies: missing records, replayed history, fork inheritance, resets or changed accounting are possible explanations.
5. Do not assume every retry has a new recorded response ID. Retain attempt evidence and charge incomplete attempts as known partial usage with an explicit completeness flag.
6. Resume and fork can restore prior usage. The pinned [replay implementation](https://github.com/openai/codex/blob/5af85998c24fb3353ddd8164c3ed472057b03cb3/codex-rs/app-server/src/request_processors/token_usage_replay.rs) sends historical snapshots without generating a new persisted usage event. Its fallback attribution can use turn position when explicit identities do not match. Preserve that weaker provenance.
7. A session-tree root or originating root turn is not proof of current task ownership. Reused children, new user work and resumed tasks require explicit assignment boundaries. Reject ambiguous joins.
8. Keep aborted and missing-final-record attempts. Known consumption can be a lower bound even when final cost and elapsed time are unavailable.

## Minimum outcome-linked record

Record task/Issue identity, task family, policy version, attempt and phase, host/version, requested and observed model/effort, native session/thread/turn/response identities, dispatch relationships, source provenance, timestamps, raw usage, normalized usage semantics, terminal state, completeness and discrepancy reasons. Link exact candidate/proof/review/delivery evidence from the quality decision. Collect quota separately; do not substitute it for token usage.

## Corrections to the independent drafts

Both drafts overstated the absence of tool timing. Claude conflated a session tree with a task and treated some model-switch notices as indispensable even though per-turn configuration is available. Muse imported cloud Agents API `subagent_id` joins into local Codex without proving equivalence. The [cloud API](https://developers.openai.com/api/docs/guides/agents-api/observability) is a separate contract, not evidence that local logs contain those fields. Neither draft establishes universal response-ID behavior under retries.

## Remaining proof

The availability decision is resolved with explicit unknowns. The blocked sample-account Task must test parent/child joins, resumed work, incomplete attempts and outcome links on bounded real records. Different clients require their own compatibility evidence. No universal collector support, actual quota attribution or backend model verification is claimed.
