# Live recipe: Sonnet medium -> Luna max -> Muse xhigh -> planner-question -> completion

EXTERNAL LIVE VERIFICATION ONLY. Nothing here runs in deterministic tests.
Do not run this to force Go quota exhaustion. Reuse earlier route evidence
where valid. Requires human authority, real CLIs, and real quota.

Fixture: `fixtures/live_sequence.json`.

```bash
SD=/tmp/rr-live
REQ=live-sonnet-luna-muse-001
WS=/tmp/live-ws
mkdir -p "$WS"

# 1. Submit with built-in defaults (no executor/callback commands).
#    Override route only for the bounded Sonnet medium probe.
python -m runner --state-dir "$SD" submit --request-id "$REQ" \
  --task-file fixtures/live_sequence.json \
  --workspace "$WS" --planner-session claude-live-planner-001 \
  --route sonnet/medium --start

# 2. Luna max dispatch uses codex exec --json --output-last-message PATH
#    --model gpt-5.6-luna -c model_reasoning_effort="max" --sandbox
#    workspace-write --cd WS. The returned thread/task ID persists before
#    the dispatch counts as accepted. Resume is codex exec resume ID
#    --json with the saved workspace as cwd (never --cd/--reasoning).
python -m runner --state-dir "$SD" status --request-id "$REQ"

# 3. Implementation runs opencode run --format json --pure --dir WS --model
#    opencode/muse-spark-1.3-contributor-free --variant xhigh --agent build
#    free-first, carrying the saved artifact/output (--session resumes the
#    saved session). Free to Go (opencode-go/muse-spark-1.3-contributor)
#    needs the exact vendor FreeUsageLimitError (including decoded
#    responseBody) or retry free_tier_limit + provider opencode for the
#    saved session, with abort + idle confirm over an owned ephemeral
#    per-job opencode serve --pure --hostname 127.0.0.1 --port 0 process.

# 4. Luna planner_question persists before Claude --resume claude-live-planner-001
#    (production default --model fable-5.1 --effort max; this bounded live
#    probe explicitly overrides to --model claude-sonnet-5 --effort
#    medium); never forks a new planner session.
python -m runner --state-dir "$SD" questions --request-id "$REQ"

# 5. Planner answer persists before resuming the same Luna task.
python -m runner --state-dir "$SD" answer --request-id "$REQ" \
  --qid q-live-1 --answer "Approved as written."

# 6. Completion under the controller lease; authority/proof/review/delivery
#    stay in the target project.
python -m runner --state-dir "$SD" recover --request-id "$REQ"
python -m runner --state-dir "$SD" status --request-id "$REQ"

# 7. Independent review (external): Luna max ticket review, then one
#    Opus 5 high combined review of the exact final candidate.
```

Quota rule: only `FREE_ALLOWANCE_EXHAUSTED` with `confirmed:true`, the exact
vendor `FreeUsageLimitError` (including decoded `responseBody` JSON), or a
retry with reason `free_tier_limit` and provider `opencode` proves free
exhaustion and permits `muse-spark-xhigh-free` -> `muse-spark-xhigh-go`.
Generic 429, `RateLimitError`/`rate_limit`, timeouts, permission/consent,
invalid-plan, `DataPolicyError`, `RegionError`, `AuthError`, and
`GoUsageLimitError`/`account_rate_limit` never switch quota. Zen overflow
and direct paid APIs stay disabled.
