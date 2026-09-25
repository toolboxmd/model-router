"""Independent review turn before a job completes (toolboxmd/model-router#126).

Deterministic: every T3 turn runs against the in-memory fake T3 client.
"""
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from runner import controller, core, policy, store, t3exec, t3snapshot  # noqa: E402
from tests.fakes import (isolate_t3_env, prism_provider, prism_snapshot,  # noqa: E402
                         snap, use_fake_t3)


def setUpModule():
    isolate_t3_env()


APPROVE = {"verdict": "approve", "findings": ""}
CHANGES = {"verdict": "request_changes", "findings": "secret leaks into proof.log"}


def git(ws, *args):
    subprocess.run(["git", *args], cwd=str(ws), check=True, capture_output=True)


def head_of(ws):
    return core._workspace_head(str(ws))


def set_state(sd, rid, **fields):
    con = store.connect(sd)
    try:
        row = con.execute("SELECT controller_state FROM jobs WHERE request_id=?",
                          (rid,)).fetchone()
        st = json.loads(row["controller_state"] or "{}")
        st.update(fields)
        con.execute("UPDATE jobs SET controller_state=?, status='running' WHERE request_id=?",
                    (json.dumps(st, sort_keys=True), rid))
        con.commit()
    finally:
        con.close()


def events(sd, rid, kind):
    con = store.connect(sd)
    try:
        rows = con.execute("SELECT payload_json FROM events WHERE request_id=? AND kind=?"
                           " ORDER BY id", (rid, kind)).fetchall()
    finally:
        con.close()
    return [json.loads(r["payload_json"] or "{}") for r in rows]


def creates(fake):
    return [c for c in fake.commands if c.get("type") == "thread.create"]


class ReviewTurn(unittest.TestCase):
    def setUp(self):
        t3snapshot.reset()
        self.addCleanup(t3snapshot.reset)
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        base = Path(tmp.name)
        self.sd = str(base / "state")
        self.ws = base / "ws"
        self.ws.mkdir()
        git(self.ws, "init")
        git(self.ws, "config", "user.email", "test@example.test")
        git(self.ws, "config", "user.name", "Test")
        (self.ws / "a.txt").write_text("one\n")
        git(self.ws, "add", "-A")
        git(self.ws, "commit", "-m", "one")

    def job_at_completion(self, rid, replies=()):
        core.submit(self.sd, rid, {"goal": "x", "proof": "true"}, str(self.ws), "p",
                    planner_t3_thread="planner-t3")
        fake = use_fake_t3(self, self.sd, rid, replies=list(replies))
        job = core.get_job(self.sd, rid)
        full = {"assistant_text": "did stuff", "usage": None, "native_ids": {},
                "finish": "stop", "actual_model": None}
        report = controller._write_turn_report(self.sd, rid, job, 1,
                                               "muse-spark-xhigh-free", full, "ses")
        self.assertEqual(report["status"], "ok")
        set_state(self.sd, rid, seq=1, last_action_name="completion",
                  last_action={"action": "completion", "output": "DONE"})
        return fake

    def test_approve_completes_with_verdict_and_sha(self):
        fake = self.job_at_completion("ap", [APPROVE])
        head = head_of(self.ws)
        done = controller.step(self.sd, "ap")
        self.assertEqual(done["action"], "completed", done)
        # One reviewer thread, a child of the dispatcher thread, on the
        # review stage's first route in read-only plan mode.
        review = [c for c in creates(fake) if "review" in c["title"]]
        self.assertEqual(len(review), 1)
        self.assertEqual(review[0]["parentThreadId"], "sub.planner-t3.disp")
        self.assertEqual(review[0]["modelSelection"]["model"],
                         policy.route_spec(policy.stage_routes("review")[0])["model"])
        prompt = [t for tid, t in fake.posts if tid == review[0]["threadId"]][0]
        self.assertIn(f"Reviewed commit: {head}", prompt)
        result = json.loads(core.get_job(self.sd, "ap")["result_json"])
        result = json.loads(result["output"]) if "review" not in result else result
        self.assertEqual(result["review"]["verdict"], "approve")
        self.assertEqual(result["review"]["reviewed_sha"], head)
        verdicts = events(self.sd, "ap", "review_verdict")
        self.assertEqual([(v["round"], v["verdict"], v["reviewed_sha"]) for v in verdicts],
                         [(1, "approve", head)])

    def test_request_changes_then_approve_on_new_head(self):
        fake = self.job_at_completion(
            "rc", [CHANGES, "fixed the leak",
                   {"action": "completion", "output": "DONE AGAIN"}, APPROVE])
        first_head = head_of(self.ws)
        res = controller.step(self.sd, "rc")
        self.assertEqual(res["action"], "review-changes-requested", res)
        st = controller._load_controller_state(core.get_job(self.sd, "rc"))
        self.assertEqual(st["last_action"]["action"], "implementation")
        self.assertEqual(st["ladder"]["failures"], 1)
        # The worker fixes and commits: the next turn runs on a new head.
        (self.ws / "a.txt").write_text("two\n")
        git(self.ws, "commit", "-am", "fix")
        new_head = head_of(self.ws)
        res = controller.step(self.sd, "rc")
        self.assertEqual(res["action"], "implementation-resumed", res)
        worker = [t for _tid, t in fake.posts if "worker seq" in t]
        self.assertTrue(any(CHANGES["findings"] in t for t in worker), fake.posts)
        done = controller.step(self.sd, "rc")
        self.assertEqual(done["action"], "completed", done)
        verdicts = events(self.sd, "rc", "review_verdict")
        self.assertEqual([(v["round"], v["verdict"], v["reviewed_sha"]) for v in verdicts],
                         [(1, "request_changes", first_head), (2, "approve", new_head)])
        self.assertIn("secret", verdicts[0]["findings"])
        self.assertEqual(len([c for c in creates(fake) if "review" in c["title"]]), 2)

    def test_review_rounds_are_bounded_by_the_ladder(self):
        self.job_at_completion("lim", [CHANGES])
        # The ladder budget is spent: recovery ran and the planner's one
        # authorized attempt was used. Another request for changes ends
        # the job with its evidence instead of another round.
        set_state(self.sd, "lim", ladder={"failures": 3, "rung": "recovery",
                                          "escalated": True},
                  planner_recovery_used=True)
        res = controller.step(self.sd, "lim")
        self.assertEqual(res["action"], "review-changes-requested", res)
        res = controller.step(self.sd, "lim")
        self.assertEqual(res["action"], "failed", res)
        self.assertEqual(core.get_job(self.sd, "lim")["status"], "failed")
        self.assertEqual(len(events(self.sd, "lim", "review_verdict")), 1)

    def test_request_changes_counts_as_a_ladder_failure(self):
        self.job_at_completion("lad", [CHANGES])
        set_state(self.sd, "lad", ladder={"failures": 3, "rung": "recovery",
                                          "escalated": True})
        controller.step(self.sd, "lad")
        st = controller._load_controller_state(core.get_job(self.sd, "lad"))
        self.assertEqual(st["ladder"]["failures"], 4)

    def test_restart_during_review_adopts_the_thread(self):
        fake = self.job_at_completion("rs")
        head = head_of(self.ws)
        # The controller dies while watching the review turn.
        with mock.patch.object(t3exec, "watch_turn",
                               side_effect=RuntimeError("controller died")):
            with self.assertRaises(RuntimeError):
                controller.step(self.sd, "rs")
        review = [c for c in creates(fake) if "review" in c["title"]]
        self.assertEqual(len(review), 1)
        tid = review[0]["threadId"]
        saved = controller._t3_thread_for(core.get_job(self.sd, "rs"),
                                          f"review_1_{head[:12]}")
        self.assertEqual(saved["thread_id"], tid)
        self.assertTrue(saved["turn_started"])
        review_posts = lambda: [t for t_id, t in fake.posts if t_id == tid]  # noqa: E731
        self.assertEqual(len(review_posts()), 1)
        fake.complete(tid, json.dumps(APPROVE))
        done = controller.step(self.sd, "rs")
        self.assertEqual(done["action"], "completed", done)
        # Adopted: no second thread and no second prompt.
        self.assertEqual(len([c for c in creates(fake) if "review" in c["title"]]), 1)
        self.assertEqual(len(review_posts()), 1)

    def test_prism_reviewer_list_routes_the_review(self):
        fake = self.job_at_completion("pr", [APPROVE])
        fake.prism = prism_snapshot(
            [prism_provider("claudeAgent", ["claude-opus-5-5"])],
            lanes={"reviewer": {"medium": [{"instanceId": "claudeAgent",
                                            "model": "claude-opus-5-5",
                                            "effort": "high"}]}})
        t3snapshot.refresh(fake, "proj-1", None, force=True)
        done = controller.step(self.sd, "pr")
        self.assertEqual(done["action"], "completed", done)
        review = [c for c in creates(fake) if "review" in c["title"]]
        self.assertEqual(review[0]["modelSelection"]["instanceId"], "claudeAgent")

    def review_threads(self, fake):
        return [c["threadId"] for c in creates(fake) if "review" in c["title"]]

    def on_review_post(self, fake, hook):
        """Call ``hook(thread_id)`` after each post onto a review thread."""
        original = fake.post_message

        def post(thread_id, text, *a, **k):
            out = original(thread_id, text, *a, **k)
            if thread_id in self.review_threads(fake):
                hook(thread_id)
            return out
        fake.post_message = post

    def test_missing_verdict_gets_one_repair_post(self):
        fake = self.job_at_completion("rp", ["looks fine to me", '{"verdict": "Approved"}'])
        done = controller.step(self.sd, "rp")
        self.assertEqual(done["action"], "completed", done)
        tid = self.review_threads(fake)[0]
        posts = [t for t_id, t in fake.posts if t_id == tid]
        self.assertEqual(len(posts), 2)
        self.assertIn("VERDICT REPAIR", posts[1])
        self.assertNotIn("looks fine", posts[1])

    def test_missing_verdict_after_repair_blocks_recoverably(self):
        self.job_at_completion("mv", ["looks fine to me", "still no json"])
        res = controller.step(self.sd, "mv")
        self.assertEqual(res["action"], "blocked")
        reason = core.get_job(self.sd, "mv")["block_reason"]
        self.assertTrue(reason.startswith("review_missing_verdict"), reason)
        self.assertNotIn("looks fine", reason)
        self.assertNotIn("still no json", reason)
        self.assertTrue(reason.startswith(core.RECOVER_OWNED_BLOCKS))

    def test_recover_after_missing_verdict_runs_a_fresh_review(self):
        fake = self.job_at_completion("fr", ["no", "no"])
        self.assertEqual(controller.step(self.sd, "fr")["action"], "blocked")
        first = self.review_threads(fake)
        self.assertEqual(len(first), 1)
        fake.child_replies = [json.dumps(APPROVE)]
        set_state(self.sd, "fr")  # recover reopens the job (status running)
        done = controller.step(self.sd, "fr")
        self.assertEqual(done["action"], "completed", done)
        threads = self.review_threads(fake)
        self.assertEqual(len(threads), 2)
        self.assertNotEqual(threads[0], threads[1])
        self.assertEqual(len([1 for t_id, _ in fake.posts if t_id == first[0]]), 2)

    def test_interrupted_review_turn_continues_once(self):
        fake = self.job_at_completion("it", [APPROVE, APPROVE])
        seen = []

        def interrupt_first(tid):
            if not seen:
                seen.append(tid)
                fake.scripts[tid] = snap(tid, state="interrupted", session_status="ready")
        self.on_review_post(fake, interrupt_first)
        done = controller.step(self.sd, "it")
        self.assertEqual(done["action"], "completed", done)
        tid = self.review_threads(fake)[0]
        self.assertEqual(len([1 for t_id, _ in fake.posts if t_id == tid]), 2)

    def test_exhausted_review_routes_wait_for_capacity(self):
        fake = self.job_at_completion("cap", [APPROVE])
        for route in policy.stage_routes("review"):
            core.record_capacity(self.sd, route, "exhausted", {"source": "test"},
                                 reset_at="2999-01-01T00:00:00+00:00")
        res = controller.step(self.sd, "cap")
        self.assertEqual(res["action"], "blocked")
        reason = core.get_job(self.sd, "cap")["block_reason"]
        self.assertTrue(reason.startswith("review_capacity_wait"), reason)
        self.assertTrue(reason.startswith(core.RECOVER_OWNED_BLOCKS))
        self.assertEqual(self.review_threads(fake), [])

    def test_limit_mid_review_then_reset_and_recover_runs_a_new_review(self):
        fake = self.job_at_completion("mid")
        fake.prism = prism_snapshot(
            [prism_provider("codex", ["gpt-5.6-luna"])],
            lanes={"reviewer": {"medium": [{"instanceId": "codex",
                                            "model": "gpt-5.6-luna",
                                            "effort": "max"}]}})
        t3snapshot.refresh(fake, "proj-1", None, force=True)
        self.assertEqual(policy.stage_routes("review"), ["luna/max"])
        original = fake.dispatch

        def limit_first_review(command):
            out = original(command)
            if command.get("type") == "thread.create" and "review" in command["title"] \
                    and len(self.review_threads(fake)) == 1:
                tid = command["threadId"]
                fake.scripts[tid] = snap(tid, state="running", session_status="error",
                                         last_error="You have hit your usage limit")
            return out
        fake.dispatch = limit_first_review
        res = controller.step(self.sd, "mid")
        self.assertEqual(res["action"], "blocked", res)
        reason = core.get_job(self.sd, "mid")["block_reason"]
        self.assertTrue(reason.startswith("review_capacity_wait"), reason)
        self.assertIn("luna/max", core.exhausted_routes(self.sd))
        # The limit resets; recover reopens the job.
        core.clear_capacity(self.sd, "luna/max")
        set_state(self.sd, "mid")
        fake.child_replies = [json.dumps(APPROVE)]
        done = controller.step(self.sd, "mid")
        self.assertEqual(done["action"], "completed", done)
        threads = self.review_threads(fake)
        self.assertEqual(len(threads), 2)
        self.assertNotEqual(threads[0], threads[1])
        # The old error was never re-read into a new capacity mark.
        self.assertNotIn("luna/max", core.exhausted_routes(self.sd))

    def test_untracked_stray_file_does_not_block_review(self):
        fake = self.job_at_completion("un", [APPROVE])
        self.on_review_post(fake, lambda _tid: (self.ws / "stray.log").write_text("x\n"))
        done = controller.step(self.sd, "un")
        self.assertEqual(done["action"], "completed", done)

    def test_reviewer_commit_is_not_accepted(self):
        fake = self.job_at_completion("cm", [APPROVE])

        def reviewer_commits(_tid):
            (self.ws / "a.txt").write_text("reviewer commit\n")
            git(self.ws, "commit", "-qam", "reviewer")
        self.on_review_post(fake, reviewer_commits)
        res = controller.step(self.sd, "cm")
        self.assertEqual(res["action"], "blocked", res)
        self.assertTrue(core.get_job(self.sd, "cm")["block_reason"]
                        .startswith("review_changed_workspace"))

    def test_reviewer_that_writes_is_not_accepted(self):
        fake = self.job_at_completion("wr", [APPROVE])

        def reviewer_commits(_tid):
            (self.ws / "a.txt").write_text("reviewer edit\n")
        self.on_review_post(fake, reviewer_commits)
        res = controller.step(self.sd, "wr")
        self.assertEqual(res["action"], "blocked", res)
        job = core.get_job(self.sd, "wr")
        self.assertTrue(job["block_reason"].startswith("review_changed_workspace"),
                        job["block_reason"])
        self.assertTrue(job["block_reason"].startswith(core.RECOVER_OWNED_BLOCKS))
        self.assertEqual(events(self.sd, "wr", "review_verdict"), [])
        self.assertNotEqual(job["status"], "succeeded")

    def test_pr_body_carries_the_verdict(self):
        section = controller.review_pr_section(
            {"verdict": "approve", "round": 2, "route": "luna/max",
             "reviewed_sha": "abc123", "findings": ""})
        self.assertIn(controller.REVIEW_PR_MARKER, section)
        self.assertIn("Verdict: approve", section)
        self.assertIn("Reviewed SHA: `abc123`", section)

    def test_pr_body_replaces_only_the_marked_section(self):
        old = controller.review_pr_section({"verdict": "request_changes", "round": 1,
                                            "reviewed_sha": "a1", "findings": "x"})
        body = "intro\n\n" + old + "\n\nfooter text"
        new = controller.review_pr_section({"verdict": "approve", "round": 2,
                                            "reviewed_sha": "b2", "findings": ""})
        out = controller.replace_review_section(body, new)
        self.assertTrue(out.startswith("intro\n\n"))
        self.assertTrue(out.endswith("\n\nfooter text"))
        self.assertIn("`b2`", out)
        self.assertNotIn("`a1`", out)

    def test_pr_section_redacts_findings(self):
        secret = "ghp_" + "a" * 36
        section = controller.review_pr_section(
            {"verdict": "request_changes", "round": 1, "reviewed_sha": "a1",
             "findings": f"token {secret} leaked"})
        self.assertNotIn(secret, section)

    def test_parse_verdict_takes_the_last_object(self):
        text = 'notes {"x": 1}\n```json\n{"verdict": "request_changes", "findings": ["a", "b"]}\n```'
        self.assertEqual(controller.parse_review_verdict(text),
                         {"verdict": "request_changes", "findings": "a\nb"})
        self.assertIsNone(controller.parse_review_verdict('{"verdict": "maybe"}'))
        self.assertEqual(controller.parse_review_verdict('{"verdict": "Request Changes"}')
                         ["verdict"], "request_changes")


if __name__ == "__main__":
    unittest.main()
