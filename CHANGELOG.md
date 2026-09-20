# Changelog

## [0.8.0] - 2026-09-20

### Added

- Single 0.7.0 to 0.8.0 transition covering Issues 13-16: signal classes with pool/model/window capacity (13), turn reports with measurements and ledger schema 2 (14), one escalation with recoverable step budget and #8 minors (15), harness seam with legacy lane removal and Go Luna dispatch fallback (16); harness seam: every harness call goes through runner/harnesses.py; legacy worker lane removed; dispatch falls back to Luna on OpenCode Go

## [0.7.0] - 2026-09-19

### Added

- Escalation ladder with one recovery escalation and a terminal hand-back to the planner, job-level step budget with recover-owned launch budget, question conflict clearing, resume and recovery fixes from #8

## [0.6.0] - 2026-09-19

### Added

- Turn reports, structured dispatcher evidence, per-invocation measurements with native identities, job provenance, and ledger schema version 2

## [0.5.0] - 2026-09-19

### Added

- Signal classes applied: exhaustion moves pools with zero retries, overload moves families after a bounded window, capacity per pool, model, and window with a degraded state and preflight skips

## [0.4.0] - 2026-09-19

### Added

- Policy v2 as data with lanes, subscription pools, and signal classes; skill table generated from the policy; glossary

## [0.3.0] - 2026-09-18

### Added

- Add an opt-in durable local runner that hands Claude-planned work to a persistent Codex Luna dispatcher and Muse workers with crash-safe process ownership

## [0.2.1] - 2026-09-19

### Changed

- Record the confirmed Project Direction: Claude Code host, one policy for skill and runner, subscription pools

## [0.2.0] - 2026-09-17

### Added

- Add a small Codex routing skill with one Markdown model table and bounded escalation; narrow project direction to Codex v1.

- Add the Codex-only routing skill, canonical model policy, bounded recovery, native dispatch reference, package validation, and approved project direction.

## [0.1.0] - 2026-09-03

### Added

- Establish the Model Router project and confirmed Project Direction
