# Reuse existing tools for usage across machines

Date: 2026-09-17. Owning question: user-requested build-versus-integrate research, following [map #3](https://github.com/toolboxmd/model-router/issues/3).

## Decision

Reuse existing readers. Evaluate ccusage first for session exports, with Tokscale as the stronger candidate when request-level normalized records are necessary. Reuse CodexBar for shared-account quota observations. Add only the missing machine/account identity, idempotent import, task/outcome links and completeness reporting after a bounded compatibility test.

This is a research recommendation, not a validated deployment selection. No inspected product establishes the whole closed loop out of the box. The installed CodexBar export works locally; ccusage and Tokscale have not been run against our records or on Rocky.

The user explicitly broadened this research to sessions on this Mac, Rocky and other servers using the same agent harnesses and API keys/subscriptions. Browser chat is excluded. This supersedes the map's Codex-only research assumption, not the separate v1 implementation Objective. No Project Direction or routing policy changes here.

Completion condition: identify reusable integrations, their exact gaps, and the smallest test that can select one. The current constraint is accounting compatibility, not a missing dashboard. Delete proposed new parsers, dashboard and persistent service work until existing tools fail the required checks.

## Candidates and evidence

### CodexBar: reuse account meters and existing local totals

Installed version 0.60.3 is available on this Mac. A read-only command, `codexbar cost --provider codex --provider-native-only --days 1 --format json`, exited successfully in 4.5 seconds. It returned totals, project breakdowns, coverage, a timestamp and `listPriceEstimate` provenance. Its history flag was true. This verifies the export interface, not independent correctness of every session total. Private project paths and usage exports remain local.

The [CLI contract](https://github.com/steipete/CodexBar/blob/b6e65a83dc471817b7ff7678e68e0204c9dd604f/docs/cli.md) provides one-shot usage/cost JSON and optional HTTP endpoints. Prefer one-shot output initially; a server is unnecessary for the accounting test. The [provider documentation](https://github.com/steipete/CodexBar/blob/b6e65a83dc471817b7ff7678e68e0204c9dd604f/docs/providers.md) describes provider-specific quota and spend sources, rather than one universal event ledger.

A decisive limitation: the [OpenCode reader](https://github.com/steipete/CodexBar/blob/b6e65a83dc471817b7ff7678e68e0204c9dd604f/Sources/CodexBarCore/Providers/OpenCodeGo/OpenCodeGoLocalUsageReader.swift) filters local assistant records to `providerID = opencode-go`. Our Muse route uses `opencode`, so this reader does not provide its local session accounting. The [OpenCode documentation](https://github.com/steipete/CodexBar/blob/b6e65a83dc471817b7ff7678e68e0204c9dd604f/docs/opencode.md) explicitly separates remote quota from local history.

For Grok, [local signals](https://github.com/steipete/CodexBar/blob/b6e65a83dc471817b7ff7678e68e0204c9dd604f/Sources/CodexBarCore/Providers/Grok/GrokLocalSessionScanner.swift) aggregate context/compaction counters. Those are not a complete request-consumption ledger. The [Grok provider](https://github.com/steipete/CodexBar/blob/b6e65a83dc471817b7ff7678e68e0204c9dd604f/docs/grok.md) has billing integrations with availability limits. Reuse its meter without treating context size as consumed tokens.

### ccusage: first candidate for a small session-export integration

Inspected source `3c5556a775ebcf1e59844d4283c8c5b30529c290` supports Claude Code, Codex, OpenCode and Grok, with [JSON reports](https://github.com/ccusage/ccusage/blob/3c5556a775ebcf1e59844d4283c8c5b30529c290/docs/guide/json-output.md). The [MIT license](https://github.com/ccusage/ccusage/blob/3c5556a775ebcf1e59844d4283c8c5b30529c290/apps/ccusage/LICENSE) permits reuse subject to its notice requirements.

The [OpenCode loader](https://github.com/ccusage/ccusage/blob/3c5556a775ebcf1e59844d4283c8c5b30529c290/rust/adapters/opencode/src/loader.rs) reads SQLite and legacy files, deduplicates message identities and is not restricted to OpenCode Go. Its [parser](https://github.com/ccusage/ccusage/blob/3c5556a775ebcf1e59844d4283c8c5b30529c290/rust/adapters/opencode/src/parser.rs) handles cache and reasoning accounting. The [Codex replay module](https://github.com/ccusage/ccusage/blob/3c5556a775ebcf1e59844d4283c8c5b30529c290/rust/adapters/codex/src/replay.rs) addresses inherited history.

Its CLI reports are primarily session/period aggregates. Request-event export, account attribution and cross-machine synchronization were not established. Multiple input roots do not by themselves establish safe merging of copied sessions. The [Grok contract](https://github.com/ccusage/ccusage/blob/3c5556a775ebcf1e59844d4283c8c5b30529c290/docs/guide/grok/index.md) counts completed turns and explicitly excludes killed mid-turn usage. Preserve that incompleteness. Recorded Grok cost ticks remain provider-reported cost, not proof of a separate subscription invoice, regardless of the tool's terminology.

### Tokscale: candidate for richer parser reuse

Inspected source `dcf8d3656bbcecf05d112c6f35f9be328e4f66b7` offers session/client/model JSON grouping, and an [MIT license](https://github.com/junhoyeo/tokscale/blob/dcf8d3656bbcecf05d112c6f35f9be328e4f66b7/LICENSE). Its [core library](https://github.com/junhoyeo/tokscale/blob/dcf8d3656bbcecf05d112c6f35f9be328e4f66b7/crates/tokscale-core/src/lib.rs) publicly exposes sessions, scanners and `UnifiedMessage`. This is a possible seam for event-level collection without writing parsers anew, not a proven stable integration API.

Its [OpenCode parser](https://github.com/junhoyeo/tokscale/blob/dcf8d3656bbcecf05d112c6f35f9be328e4f66b7/crates/tokscale-core/src/sessions/opencode.rs) preserves reasoning/cache buckets and dedup keys. Its [Codex parser](https://github.com/junhoyeo/tokscale/blob/dcf8d3656bbcecf05d112c6f35f9be328e4f66b7/crates/tokscale-core/src/sessions/codex.rs) treats reasoning as a subset of output before normalization. Its [Grok parser](https://github.com/junhoyeo/tokscale/blob/dcf8d3656bbcecf05d112c6f35f9be328e4f66b7/crates/tokscale-core/src/sessions/grok.rs) normalizes usage and assigns dedup keys.

Both ccusage and Tokscale currently inspect Codex's legacy token-count surface. Our current log contains both that surface and newer `token_usage_record` events. This is not evidence they miss all current usage, but the two representations must reconcile. Pin the selected release and verify it contains the inspected behavior. Do not treat main-branch source as evidence about an installed package.

### Other tools do not displace the shortlist

[TokenTelemetry](https://github.com/VasiHemanth/tokentelemetry/blob/c866a5e532de5d1834dbaff9c2a0ea6ad08606db/backend/main.py) has broad coverage, but the inspected OpenCode step-finish loop sums input/output, takes the maximum cache-read across steps, and omits reasoning from its total. That would undercount the per-step consumption in our Muse-shaped data. Its [history keys](https://github.com/VasiHemanth/tokentelemetry/blob/c866a5e532de5d1834dbaff9c2a0ea6ad08606db/backend/history_store.py) also do not establish machine identity. This is source-based evidence, not a deployed-product test.

[Agent Trail](https://github.com/camtrik/agent-trail/blob/6e53f21f292955974ac21a082edb4ac216e2ad92/ingest/db/schema.sql) has useful session/provenance storage, but inspected coverage lacks Grok and a proven cross-machine identity contract. Its license could not be verified from a LICENSE file. Leet Agent Tracker appeared in search results with a promising warehouse design, but its live repository returned 404; it is not an actionable verified dependency.

## Proposed integration and invariants

This section is design inference from the evidence, not an existing product capability claim.

1. **One local export per execution machine.** Read that machine's harness logs through a pinned existing reader. Attach machine ID, harness/version, source root, collector version, native session identity, provider/model, timestamps and coverage. Count the execution server's session once when a desktop remotely controls it.
2. **One private combined usage ledger.** Import over an existing authorized transport. Upsert cumulative session snapshots; never append-and-sum every poll. Keep native event identities and source provenance where available. Copies of the same session across machines are replicas, not new consumption. Continued work after migration must retain genuinely new events. Ambiguous overlaps stay flagged.
3. **Separate account meters.** Map configured account aliases across hosts without exporting secrets. Keep quota windows, resets, credits and billed evidence under that account. Two machines observing one subscription do not create two budgets. Never add API account aggregates to the same locally recorded requests. Unattributed differences remain account-only evidence.
4. **Join task outcomes.** Add task/attempt/phase links and the existing acceptance, repair, review and delayed-regression evidence. A session may contain several tasks, and a task may span sessions, models and machines. Session totals alone cannot allocate that split reliably.
5. **Report coverage with every total.** Offline machine, missing records, interrupted turn, stale meter, unknown price and ambiguous task ownership remain visible. Unobserved usage is not zero. A complete global number requires all participating machines and retention windows to be accounted for.

Start with scheduled collection only after the one-shot semantics pass. No new proxy, dashboard, public upload, credential synchronization or host service is needed for this research decision.

## Next discriminating test

Extend the existing bounded sample-account Task rather than start a general tool-building project:

- Inventory actual harness versions and log roots on this Mac and Rocky; remote state is not yet inspected.
- Run pinned ccusage session exports and, if necessary for event identities, Tokscale against the same bounded records. Verify release/source correspondence first.
- Reconcile the known Codex 12-response turn (960,308 total tokens) and Muse run (416,440 additive native tokens). Include Claude helper usage.
- Import the same export twice, copy a session between hosts, then append new work. Verify no duplicate consumption and no lost continuation.
- Include fork/resume history, shared-account observations, a stopped attempt, and a task split across sessions. Distinguish detectable incompleteness from recoverable consumption.
- Measure parsing time, data volume and missing identity fields. Choose the simplest passing reader. Prefer upstream fixes or a narrow adapter over a parser fork.

The result should decide the integration and expose the remaining recording gap. It must not promise exact interrupted consumption when no source records it. Research is complete; deployment and complete-loop validation remain unperformed.
