# Evidence for implementation and review quality

Decision: [Define evidence for implementation and review quality](https://github.com/toolboxmd/model-router/issues/5).
Date: 2026-09-17. Status: research resolved; collection feasibility remains a separate Task.

## Answer

Bind acceptance and review findings to exact candidates and independent evidence. Record both successful delivery and the failed attempts that preceded it. Keep observed defects separate from total defect recall, which ordinary work does not reveal.

This advances Model Router's Objective by making quality-preserving routing comparisons possible without a benchmark matrix. Preserve Luna-max bug hunting and Spark's tiny-change candidate lane; this decision does not change routing or proof obligations.

## Relevant primary evidence

[Bug Hunt Bench's method and dataset](https://bughunt.productcompass.pm/method) use independent, blind grading against known planted defects and score actual fixes. The [dataset](https://bughunt.productcompass.pm/data/benchmark.json), updated 2026-09-17, separates fixes, partial work, claims without fixes, extras and false-positive fixes. It reports repeated-run variability and different cost bases. Its Luna-max row records 33/105 fixes and a $1.80 list estimate in one run. This supports retaining the baseline, not claiming general PR-review recall.

Ordinary work lacks an exhaustive answer key. Reuse its approved acceptance contract, targeted reproductions, independent review, real integration evidence where authorized, and later confirmed regressions. Passing tests proves the tested assertions, not the absence of defects. An independent model's agreement is also not proof that a finding is true.

## Operational definitions

| Measurement | Definition and evidence |
| --- | --- |
| Accepted outcome | Required behavioral acceptance, proof and review pass on an identified candidate. Keep merged, released and Live Verified as separate states. |
| First-pass acceptance | The first candidate submitted to the declared acceptance gate passes without a repair. Declare this boundary before counting. A PR's final approval alone cannot reconstruct earlier local failures. |
| Repair cycle | An identified failure or confirmed finding leads to a changed candidate and revalidation. Distinguish normal implementation iterations from acceptance repairs. |
| Confirmed finding | A concrete defect supported by a reproducer, violated contract, verified dataflow or equivalent evidence. Store severity, identity, affected candidate and adjudication. |
| Rejected finding | Adjudication establishes the alleged defect is not valid. Store the reason. Unresolved, disputed, duplicate, out-of-scope and not-yet-fixed findings are separate states. |
| False-positive share | Rejected-invalid findings divided by adjudicated confirmed plus rejected-invalid findings. Report counts and unresolved coverage; this is not a classifier false-positive rate over all correct code. |
| Confirmed finding yield | Deduplicated confirmed findings per reviewed candidate, stratified by severity and task family. A large count alone does not establish reviewer quality. |
| Observed miss | A later independent process confirms a defect present in the reviewed candidate that was not reported. Record detection time, attribution confidence and source. |
| Delayed regression | A later failure causally linked to the candidate with supporting evidence. A revert is a signal, not automatic proof of a regression. |
| Human intervention | Recorded redirection, repair or decision with reason; distinguish required human authority from avoidable workflow interruption. |
| Scope | Compare the actual candidate against the authorized outcome and constraints. A small diff is not automatically low consequence. |
| Failed-attempt cost | Include attributable consumption, elapsed time and repair work from every attempt. Partial observations remain incomplete, not zero. |

A claim without a fix is not necessarily a false positive. Both independent drafts blurred this distinction; their benchmark terminology must not become our production label taxonomy unchanged.

## Fair comparisons

Keep route version, model, effort, host, task family, declared difficulty/risk, verifier version and assignment reason. Spark tasks and Luna hunts have different selection criteria. Compare within eligible strata, and report observational comparisons as associations rather than causal improvements.

For a routing hypothesis, predeclare the changed variable, quality floor, useful improvement and stop condition. Use a small authorized randomized or matched comparison when necessary; keep assignment independent of expected success. Do not add duplicate work routinely. Preserve required review while sampling an additional independent audit to measure marginal confirmed findings.

Blind additional reviewers to route and prior findings when practical, but retain the task contract and relevant code access. Adjudicate the union of findings and deduplicate them; raw overlap or agreement is insufficient. A second model is another detector, not an infallible judge.

Count failure, timeout, environment blocker, quota blocker and incomplete evidence separately. Do not compare cost only among winners while hiding failed attempts. Report the full attempt cost for each outcome and the completion rate for the eligible cohort.

Choose a stated follow-up window for delayed defects, publish observation coverage, and mark outcomes whose window has not elapsed as immature. Zero observed regressions with incomplete follow-up is not zero defect risk. Preserve causal confidence when several later changes affect the same code.

## Minimum evidence contract

Reuse the owning Issue and its existing handoff: exact base/candidate, task family, route/version, acceptance boundary, proof commands/results, reviewer identity, findings with evidence and adjudication, repair links, human interventions, delivery state and delayed-outcome links. Join the consumption record from the attribution decision by task and attempt. New mandatory fields should be added only where existing records cannot support these definitions.

## Remaining proof

The definitions are resolved. The sample-account Task must establish which fields already exist, which require a small explicit outcome record, and their collection overhead. True total defect recall, a universal model ranking, and acceptable tradeoffs across materially different work remain unproved. No current proof gate or user authority is weakened.
