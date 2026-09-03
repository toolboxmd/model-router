# Objective

Ship the first trustworthy Model Router in which a supported host presents the
strongest eligible model as the human-facing orchestrator and completes one
authorized software task without asking the human to choose a provider, model,
harness, reasoning effort, or established workflow step.

The Objective is complete when:

- A versioned static policy ranks eligible human-facing and execution routes.
  The highest-ranked available route becomes the human-facing orchestrator
  without per-task human selection.
- The orchestrator may select any eligible execution route. The static policy
  supplies the default when the orchestrator does not request one, and the
  router rejects every ineligible combination.
- `dispatch(WorkRequest) -> TerminalReport` is the only external interface.
  Routing, capacity checks, execution, proof, review, escalation, delivery
  continuation, and event recording remain internal.
- An immutable constitution owns Human Gates, proof requirements, success
  definitions, review rules, retry and escalation ceilings, model allowlists,
  and policy freeze conditions. No policy or orchestrator decision can weaken
  it.
- A Codex adapter supports eligible Spark, Luna, Terra, and Sol routes. A Grok
  adapter supports Grok 4.6 through the established Grok Build CLI invocation
  contract. Each adapter declares its supported model and reasoning-effort
  combinations and rejects unsupported routes.
- Every attempted task receives deterministic proof from the target project
  and implementing-agent self-review. The implementing agent and human-facing
  orchestrator cannot grade their own success.
- Independent review is omitted for low-risk deterministic work, sampled for
  ordinary work, and mandatory for high-risk work.
- After normal correction inside the initial execution, the orchestrator may
  authorize at most one useful cross-model escalation. The system does not
  race duplicate attempts or enter retry loops.
- Every already-authorized delivery step continues without returning control
  to the human merely for workflow routing.
- Every dispatch appends a passive event containing policy version, route,
  adapter, reasoning effort, proof, review, escalation, delivery states,
  terminal outcome, elapsed time, human interruption, delayed-outcome handles,
  and quota evidence that is measured or explicitly unknown.
- Deterministic tests exercise the Dispatch interface with fake agent and proof
  adapters across acceptance, ineligible routes, unavailable capacity, unknown
  quota, proof failure, escalation, timeout, cancellation, and Human Gate
  outcomes.
- Bounded real software tasks complete through both production adapters using
  their target projects' actual proof, without rebuilding or rerunning the old
  benchmark matrix.

A learned router, automatic policy rewriting, a routing dashboard, hidden
benchmark verifiers, duplicate model runs, mandatory Grok evaluation,
gpt-reserve, and Claude Code or OpenBot execution adapters are outside this
Objective.
