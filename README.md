# Model Router

One versioned routing policy and a durable runner. AgentsMD owns workflow,
authority, and proof; Model Router owns the routes in
[`runner/policy.py`](runner/policy.py), from which the
[shared routing reference](skills/model-routing/references/codex.md) is generated.
The [routing Skill](skills/model-routing/SKILL.md) sends implementation to the
bundled runner. Codex-specific ticket review has a separate host reference. Terms are in
[GLOSSARY.md](GLOSSARY.md).

## Install from a release

Install the complete plugin from an exact released source. The package contains
`skills/`, `bin/model-router`, and `runner/`; installing only the Skill subtree
omits the runtime and is unsupported.

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
Cursor distribution is generated from that same release with its launcher and
runtime included. Provider publication and fresh-host acceptance remain separate
proof. For a host that loads native Skills rather than these plugin manifests,
keep the complete release in a stable directory and register its
`skills/model-routing` directory through that host's Skill mechanism. Resolve
symlinks to that release before locating the launcher.

Start a fresh host session and confirm `model-routing` is available. Resolve
`MODEL_ROUTER_ROOT` to that installed plugin's real root, then verify:

```sh
"$MODEL_ROUTER_ROOT/bin/model-router" --version
"$MODEL_ROUTER_ROOT/bin/model-router" --help
```

The shared Skill can load across harnesses, but starting a job still requires
an existing Claude Code planner session. Additional planner callbacks are not
implemented. Installing the package starts no service or model request and does
not grant delivery authority. See [RUNNER.md](RUNNER.md) for requirements.

## Automatic releases

A `VERSION` change merged into `main` starts the [release workflow](.github/workflows/release.yml).
It runs the complete deterministic test suite and validates the exact commit
with released AgentsMD versionctl before creating an annotated tag and stable
GitHub Release. `.version-policy.json` authorizes this through
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

## Durable runner (opt-in, stdlib only)

The bundled `bin/model-router` accepts prepared work from a Claude planner, hands it to a
persistent Codex Luna dispatcher, returns planner questions, and runs
implementation turns through an owned local OpenCode server, following the
policy's lanes across the full OpenCode Go implementer chain with sticky
homes and per-tier concurrency caps, or the native Grok Build CLI. Saved
jobs survive planner exit and controller death without starting a second
writer.
It is not installed or started automatically. See [RUNNER.md](RUNNER.md).

```
"$MODEL_ROUTER_ROOT/bin/model-router" --state-dir DIR submit --request-id ID --task-file TASK.json \
  --workspace PATH --planner-session SID --lane default --start
"$MODEL_ROUTER_ROOT/bin/model-router" --state-dir DIR status --request-id ID
"$MODEL_ROUTER_ROOT/bin/model-router" --state-dir DIR recover --request-id ID
```
