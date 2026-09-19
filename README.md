# Model Router

One versioned routing policy and a durable runner. AgentsMD owns workflow,
authority, and proof; Model Router owns the routes in
[`runner/policy.py`](runner/policy.py), from which the
[Codex skill reference](skills/model-routing/references/codex.md) is generated.
The [routing Skill](skills/model-routing/SKILL.md) sends implementation to the
runner and keeps native Codex subagents for ticket review. Terms are in
[GLOSSARY.md](GLOSSARY.md).

## Install from a release

This package has not been released yet. Once released, ask Codex's
`$skill-installer` to install `skills/model-routing` from `toolboxmd/model-router`
at that exact release tag. Start a fresh session and confirm the Skill is available
before activating the AgentsMD integration.

The plugin manifest also supports future marketplace distribution; no marketplace
entry or installation is included here. Validate with the existing Skill Creator
and Plugin Creator validators. Actual routing behavior is verified through ordinary work.

## Project Direction

[Vision](VISION.md) · [Mission](MISSION.md) · [Objective](OBJECTIVE.md).
Active work belongs in GitHub Issues.

## Durable runner (opt-in, stdlib only)

`python -m runner` accepts prepared work from a Claude planner, hands it to a
persistent Codex Luna dispatcher, returns planner questions, and runs Muse
implementation turns through an owned local OpenCode server. Saved jobs
survive planner exit and controller death without starting a second writer.
It is not installed or started automatically. See [RUNNER.md](RUNNER.md).

```
python -m runner --state-dir DIR submit --request-id ID --task-file TASK.json \
  --workspace PATH --planner-session SID --lane default --start
python -m runner --state-dir DIR status --request-id ID
python -m runner --state-dir DIR recover --request-id ID
```
