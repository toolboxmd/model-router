# Model Router

The runtime behind Prism in Chromeria (the T3 Code fork). A planner thread
hands an authorized task to the durable runner, and a dispatcher carries it
through worker threads to one open PR. Every turn runs as a T3 child thread
of the planner thread. AgentsMD owns workflow, authority, and proof; Model
Router owns the routes and lanes in [`runner/policy.py`](runner/policy.py),
while T3 decides which models are enabled, their usage windows, and the
Prism role preferences. Terms are in [GLOSSARY.md](GLOSSARY.md).

## Install from a release

Install the complete plugin from an exact released source. The package is
the runtime: `bin/model-router` and `runner/`. It carries no Skill; usage
guidance lives in Chromeria's Prism tool descriptions (`prism_submit`,
`prism_status`, `prism_questions`, `prism_answer`), which call the newest
installed release.

Once Toolybara promotes a release into ToolboxMD Marketplace, install it through
your host's native plugin path:

```sh
# Codex
codex plugin marketplace add toolboxmd/marketplace
codex plugin add model-router@toolboxmd

# Claude Code
claude plugin marketplace add toolboxmd/marketplace
claude plugin install model-router@toolboxmd

# Grok Build
# Add the ToolboxMD marketplace first if it is not already configured.
grok plugin marketplace add toolboxmd/marketplace
grok plugin install model-router --trust
```

Marketplace records the exact source tag, commit, and Project Record digest.
Provider publication and fresh-host acceptance remain separate proof.
Resolve `MODEL_ROUTER_ROOT` to the installed plugin's real root, then verify:

```sh
"$MODEL_ROUTER_ROOT/bin/model-router" --version
"$MODEL_ROUTER_ROOT/bin/model-router" --help
```

Installing the package starts no service or model request and does not grant
delivery authority. See [RUNNER.md](RUNNER.md) for requirements.

## Automatic releases

A `VERSION` change merged into `main` starts the [release workflow](.github/workflows/release.yml).
It runs the same complete deterministic proof used for pull requests, then
validates the exact commit with released AgentsMD versionctl before creating an
annotated tag and stable GitHub Release. Tests run offline against T3 fakes;
they require no installed agent plugins, credentials, or T3 server. Pull-request proof has
read-only permissions; only the main-branch publishing job can write releases.
`.version-policy.json` authorizes this through
`githubReleasePolicy: on-version-commit`.
Release runs queue instead of replacing waiting runs, up to GitHub's limit of
100 pending runs.

Toolybara already enrolls Model Router and discovers published releases in its
hourly Marketplace scan. No additional source-repository credentials are needed.
Check the resulting Marketplace version separately; a source release alone does
not prove promotion, installation or behavioral verification.

If publication fails, rerun the failed workflow. It can finish a release whose
annotated tag already identifies the same exact commit. Conflicting tags,
draft releases and prereleases stop without replacement. A manual workflow
dispatch on `main` also supports recovery; other branches cannot publish.

## Project Direction

[Vision](VISION.md) · [Mission](MISSION.md) · [Objective](OBJECTIVE.md).
Active work belongs in GitHub Issues.

## Durable runner (stdlib only)

The bundled `bin/model-router` accepts prepared work from a planner in a T3
thread, runs a Luna dispatcher and its workers as child threads of that
planner thread, asks the planner in its thread when judgment is needed, and
posts the end state there. Before each step it reads T3's Prism provider
snapshot: models turned off in T3 leave routing, full usage windows rest
their routes until the reset, and Prism role preferences reorder the lanes.
Saved jobs survive planner exit and controller death without starting a
second writer. It is not started automatically. See [RUNNER.md](RUNNER.md).

```
"$MODEL_ROUTER_ROOT/bin/model-router" --state-dir DIR submit --request-id ID --task-file TASK.json \
  --workspace PATH --planner-session SID --planner-t3-thread TID --lane default --start
"$MODEL_ROUTER_ROOT/bin/model-router" --state-dir DIR status --request-id ID
"$MODEL_ROUTER_ROOT/bin/model-router" --state-dir DIR recover --request-id ID
```
