# Claude Code and Muse Contributor research comparison

Date: 2026-09-17. Two independent native CLI routes received the same two research questions, approved direction context and sanitized Codex sample. The parent owns source checks, synthesis and Issue publication. This is one observation per route, not a model ranking.

## Observed routes

| Route | Researcher execution | Reported model cost | Configuration evidence |
| --- | ---: | ---: | --- |
| Claude Code 2.1.274, Sonnet high | 201.867 seconds | $0.7549138 list estimate | CLI requested `sonnet --effort high`; init and usage identify `claude-sonnet-5`. Usage also includes a Haiku 4.5 helper. |
| OpenCode 1.18.29, Muse Contributor xhigh | 81.074 seconds | $0 reported on free model route | Session records the requested provider/model and `variant: xhigh`. `max` was not advertised; the user approved xhigh. |

Client configuration is verified, not backend effort attestation. Claude's main Sonnet estimate is $0.5192478 and its Haiku helper adds $0.235666. Neither dollar value is an actual subscription invoice. OpenCode reports zero model cost; external tool charges were not independently observed. Timing excludes parent synthesis, checking and publication. CLI duration and launch-to-terminal-event duration have slightly different boundaries.

Muse's initial attempt used the wrong effective working directory and was stopped. The corrected attempt passed an explicit directory and PWD, and resolved read-only tool permissions before launch. The invalid attempt's visible usage is at least 24,934 tokens; its interrupted work is incomplete and excluded from the valid-route timing. Setup failure is not a model-quality failure.

## Usage and schema lesson

[Machine-readable measurements](research-comparison-2026-09-17.json) retain each native schema. Muse's eight step totals reconcile to 416,440 tokens: 96,033 input excluding cache, 312,840 cache reads, 5,390 output excluding reasoning and 2,177 reasoning. For this OpenCode record those categories are additive. The supplied Codex sample instead includes cached input inside input and reasoning inside output. Blindly sharing the normalization formula would undercount or double-count.

Claude's result includes 15,646 Sonnet output tokens, with 7,039 thinking tokens inside that total, and separate cache-creation/read input fields. The helper's usage remains part of the route. Raw private transcripts, unrelated files and credentials are not published.

## Qualitative review

Muse found more specific code-level evidence, including a pinned resume-usage replay implementation. Claude relied more heavily on broad documentation and issue reports, with several bare issue identifiers. Both covered the main questions and clearly separated telemetry from invoices. Muse produced its draft faster in this run.

Neither draft was ready to adopt unchanged:

- Both overstated missing tool timing; current local App Server documentation exposes some duration fields.
- Claude confused some claimed-but-unfixed bugs with false positives and drifted into sampled-review/Spark-review suggestions outside the agreed baseline.
- Muse imported cloud Agents API `subagent_id` joins into local Codex without proving compatibility.
- Both treated some identity, retry and task-join semantics as stronger than their evidence established.
- PR approval alone does not recover all earlier acceptance failures. Independent agreement alone does not confirm a defect.

The parent checked the current [local App Server contract](https://learn.chatgpt.com/docs/app-server), the separate [cloud usage contract](https://developers.openai.com/api/docs/guides/agents-api/observability), and [pinned replay source](https://github.com/openai/codex/blob/5af85998c24fb3353ddd8164c3ed472057b03cb3/codex-rs/app-server/src/request_processors/token_usage_replay.rs), then corrected the synthesized decisions. Shared omissions demonstrate why two agreeing models are not sufficient verification.

## Result

Use Muse Contributor as a promising research-draft candidate with primary-source checking. This run does not establish that it replaces Sonnet generally or that two researchers should become the default. Preserve Luna-max bug hunting and Spark's tiny-change lane while collecting route-specific evidence.

The accepted outputs are [usage and timing attribution](codex-measurement-attribution.md) and [quality evidence](implementation-review-quality-evidence.md). The next Wayfinder frontier is the bounded complete-outcome sample, not production instrumentation or policy changes.
