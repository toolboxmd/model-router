# Objective

Complete authorized software tasks on this Mac through one released,
capacity-aware routing contract, with Claude Code as the human-facing host,
without the human choosing a provider, model, harness, or reasoning effort.

The Objective is complete when:

- One versioned policy is the single source for the Codex skill and the
  durable runner. It defines stages, ordered routes, pools, per-model
  capacity windows, and signal classes. Model names live in policy data.
- Every route runs on a subscription pool through an existing adapter: Zen
  free, OpenCode Go, and the xAI subscription for workers; Codex for
  read-only dispatch and review; Claude Code for planning. No pay-per-token
  API keys and no paid balance overflow.
- Quota exhaustion moves a task to the next pool without retries, provider
  overload moves it to another model family within one minute, and no route
  is an older generation of a model already in the pool.
- Each task receives the target project's own proof and the implementing
  agent's self-review; independent review follows AgentsMD. Neither the
  implementing agent nor the orchestrator grades its own success.
- At most one automatic escalation per task, after which evidence returns to
  the planner. No duplicate attempts and no retry loops.
- Any stage's harness and model change by editing policy, and a new role
  needs a policy row and a report contract, not a state-machine change.
- Every invocation records requested and observed route, policy version,
  elapsed time, usage counters with their source semantics, native session
  and message identities, and a terminal class, in a form Agent Observer can
  consume without relabeling. Missing measurements remain unknown.
- Deterministic tests with fake harnesses exercise acceptance, ineligible
  routes, exhausted and overloaded capacity, proof failure, escalation,
  timeout, cancellation, controller and worker death, and Human Gate
  outcomes. Unobserved behavior remains explicitly unverified.
- AgentsMD references Model Router for model selection without duplicating
  its policy or taking workflow ownership.
- The contract is released at an exact tag, installed through its supported
  path, and completes real tasks with their projects' actual proof, under
  explicit delivery authority.

A learned or self-rewriting policy, an autonomous improvement loop,
multi-machine fleet views or dashboards, ACP transport, additional
human-facing hosts, Gemini or Antigravity routes, benchmark matrices, and
startup services remain outside this milestone.
