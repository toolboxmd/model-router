"""Exercise publication with real local Git and an isolated GitHub boundary."""
from contextlib import ExitStack
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from scripts import release


class ReleaseTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name) / "source"
        self.remote = Path(self.tmp.name) / "remote.git"
        self.root.mkdir()
        subprocess.run(["git", "init", "--bare", str(self.remote)], check=True,
                       capture_output=True)
        self.git("init", "-b", "main")
        self.git("config", "user.name", "Release test")
        self.git("config", "user.email", "release@example.invalid")
        self.git("remote", "add", "origin", str(self.remote))
        self.policy = self.root / ".version-policy.json"
        self.policy.write_text(json.dumps({"githubReleasePolicy": "on-version-commit",
                                          "releaseBranch": "main"}))
        self.git("add", ".")
        self.git("commit", "-m", "versioned fixture")
        self.sha = self.git("rev-parse", "HEAD")
        self.tag = "v0.25.0"
        self.notes = "## 0.25.0\n\nLiteral `code`, $value and two lines.\n"
        self.github_release = None
        self.api_error = None
        self.create_error = None
        self.valid = True
        self.hidden_reads_remaining = 0
        self.created_release_override = None
        self.origin = f"https://github.com/{release.REPOSITORY}"
        self.writes = []
        self.validator_calls = []
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        self.stack.enter_context(patch.object(release, "ROOT", self.root))
        self.real_command = release.command
        self.stack.enter_context(patch.object(release, "command", self.command))
        self.stack.enter_context(patch.dict(os.environ, {
            "GITHUB_REPOSITORY": release.REPOSITORY, "GITHUB_REF": "refs/heads/main",
            "GITHUB_EVENT_NAME": "push", "GITHUB_SHA": self.sha,
        }))

    def git(self, *args):
        return subprocess.check_output(["git", *args], cwd=self.root, text=True,
                                       stderr=subprocess.PIPE).strip()

    def command(self, *args, **kwargs):
        if args[0] == "versionctl-fixture":
            self.validator_calls.append(args)
            exists = subprocess.run(["git", "show-ref", "--verify", "--quiet",
                                     f"refs/tags/{self.tag}"], cwd=self.root).returncode == 0
            return json.dumps({"ok": self.valid, "sha": self.sha, "tag": self.tag,
                               "version": "0.25.0", "changelogEntry": self.notes,
                               "tagExists": exists})
        if args[:4] == ("git", "remote", "get-url", "origin"):
            return self.origin
        if args[:2] == ("gh", "api"):
            if self.api_error:
                raise RuntimeError(self.api_error)
            self.assertEqual(args[2:], ("--paginate", "--slurp",
                f"repos/{release.REPOSITORY}/releases?per_page=100"))
            # The matching release is on a later page. Tag lookup alone would
            # return 404 for drafts, even with push access.
            if self.github_release is not None and self.hidden_reads_remaining > 0:
                self.hidden_reads_remaining -= 1
                return json.dumps([[self.release_payload(tag_name="v0.1.0")], []])
            return json.dumps([[self.release_payload(tag_name="v0.1.0")],
                               [self.github_release] if self.github_release else []])
        if args[:3] == ("gh", "release", "create"):
            self.writes.append(args)
            if self.create_error:
                raise RuntimeError(self.create_error)
            self.assertEqual(args[3], self.tag)
            self.assertEqual(args[args.index("--repo") + 1], release.REPOSITORY)
            self.assertEqual(args[args.index("--target") + 1], self.sha)
            self.assertIn("--verify-tag", args)
            self.assertEqual(Path(args[args.index("--notes-file") + 1]).read_text(), self.notes)
            self.assert_remote_identity()
            self.github_release = self.release_payload(**(self.created_release_override or {}))
            return self.github_release["html_url"]
        if args[0] == "gh":
            self.fail(f"Unexpected GitHub command: {args}")
        if args[:2] == ("git", "push") or (args[0] == "git" and "-a" in args):
            self.writes.append(args)
        return self.real_command(*args, **kwargs)

    def release_payload(self, **updates):
        result = {"tag_name": self.tag, "draft": False, "prerelease": False,
                  "html_url": f"https://github.com/{release.REPOSITORY}/releases/tag/{self.tag}"}
        result.update(updates)
        return result

    def tag_remote(self, *, annotated=True, sha=None):
        if annotated:
            self.git("tag", "-a", self.tag, sha or self.sha, "-m", "existing annotation")
        else:
            self.git("tag", self.tag, sha or self.sha)
        self.git("push", "origin", f"refs/tags/{self.tag}")

    def assert_remote_identity(self):
        self.assertEqual(subprocess.check_output(
            ["git", "--git-dir", str(self.remote), "cat-file", "-t", self.tag],
            text=True).strip(), "tag")
        self.assertEqual(subprocess.check_output(
            ["git", "--git-dir", str(self.remote), "rev-parse", self.tag + "^{commit}"],
            text=True).strip(), self.sha)

    def test_publishes_exact_annotated_tag_then_verifies_stable_release(self):
        result = release.publish("versionctl-fixture")
        self.assertEqual(result["state"], "released")
        self.assertEqual(result["sha"], self.sha)
        self.assertEqual(result["marketplace"], "awaiting-toolybara")
        self.assertEqual(len(self.writes), 3)
        self.assert_remote_identity()
        self.assertEqual(self.validator_calls, [
            ("versionctl-fixture", "release-check", "--sha", self.sha, "--json")])

    def test_retry_after_release_failure_reuses_immutable_tag(self):
        self.create_error = "temporary GitHub failure"
        with self.assertRaisesRegex(RuntimeError, "temporary GitHub"):
            release.publish("versionctl-fixture")
        tag_object = self.git("rev-parse", self.tag)
        self.writes.clear()
        self.create_error = None
        self.assertEqual(release.publish("versionctl-fixture")["state"], "released")
        self.assertEqual(self.git("rev-parse", self.tag), tag_object)
        self.assertEqual(len(self.writes), 1)
        self.writes.clear()
        self.assertEqual(release.publish("versionctl-fixture")["state"], "already-released")
        self.assertEqual(self.writes, [])

    def test_wrong_commit_tag_is_not_replaced(self):
        previous = self.sha
        self.git("commit", "--allow-empty", "-m", "later version")
        self.sha = self.git("rev-parse", "HEAD")
        os.environ["GITHUB_SHA"] = self.sha
        self.tag_remote(sha=previous)
        with self.assertRaisesRegex(RuntimeError, "not an annotated tag on the validated commit"):
            release.publish("versionctl-fixture")
        self.assertEqual(self.writes, [])

    def test_lightweight_tag_is_not_replaced(self):
        self.tag_remote(annotated=False)
        with self.assertRaisesRegex(RuntimeError, "not an annotated tag"):
            release.publish("versionctl-fixture")
        self.assertEqual(self.writes, [])

    def test_existing_draft_or_prerelease_is_not_modified(self):
        self.tag_remote()
        for values in [{"draft": True}, {"prerelease": True}]:
            with self.subTest(values=values):
                self.github_release = self.release_payload(**values)
                with self.assertRaisesRegex(RuntimeError, "expected published stable"):
                    release.publish("versionctl-fixture")
                self.assertEqual(self.writes, [])

    def test_draft_on_a_later_page_blocks_creation_of_its_missing_tag(self):
        self.github_release = self.release_payload(draft=True)
        with self.assertRaisesRegex(RuntimeError, "expected published stable"):
            release.publish("versionctl-fixture")
        self.assertEqual(self.writes, [])
        self.assertEqual(self.git("ls-remote", "--tags", "origin"), "")

    def test_existing_release_without_a_remote_tag_is_not_reconstructed(self):
        self.github_release = self.release_payload()
        with self.assertRaisesRegex(RuntimeError, "no matching remote annotated tag"):
            release.publish("versionctl-fixture")
        self.assertEqual(self.writes, [])

    def test_github_read_failure_does_not_create_a_tag(self):
        self.api_error = "authentication failed"
        with self.assertRaisesRegex(RuntimeError, "authentication failed"):
            release.publish("versionctl-fixture")
        self.assertEqual(self.writes, [])

    def test_failed_version_validation_does_not_publish(self):
        self.valid = False
        with self.assertRaisesRegex(RuntimeError, "did not approve the exact commit"):
            release.publish("versionctl-fixture")
        self.assertEqual(self.writes, [])

    def test_unauthorized_contexts_fail_before_validation_or_mutation(self):
        for values in [{"GITHUB_REPOSITORY": "someone/fork"},
                       {"GITHUB_REF": "refs/heads/feature"},
                       {"GITHUB_EVENT_NAME": "pull_request"},
                       {"GITHUB_SHA": "0" * 40}]:
            with self.subTest(values=values), patch.dict(os.environ, values):
                with self.assertRaises(RuntimeError):
                    release.publish("versionctl-fixture")
        self.assertEqual(self.validator_calls, [])
        self.assertEqual(self.writes, [])

    def test_wrong_origin_cannot_receive_the_tag(self):
        self.origin = "https://github.com/someone/else.git"
        with self.assertRaisesRegex(RuntimeError, "authorized source repository"):
            release.publish("versionctl-fixture")
        self.assertEqual(self.writes, [])

    def test_manual_policy_disables_publication(self):
        self.policy.write_text(json.dumps({"githubReleasePolicy": "manual"}))
        self.assertEqual(release.publish("versionctl-fixture")["state"], "disabled-by-policy")
        self.assertEqual(self.validator_calls, [])
        self.assertEqual(self.writes, [])

    def test_recovery_dispatch_on_main_can_complete_publication(self):
        with patch.dict(os.environ, {"GITHUB_EVENT_NAME": "workflow_dispatch"}):
            self.assertEqual(release.publish("versionctl-fixture")["state"], "released")

    def test_eventually_consistent_release_verifies_on_first_run(self):
        # The release API may not list a just-created release immediately.
        # Verification must retry with backoff inside its budget instead of
        # failing on the first unreadable read.
        self.hidden_reads_remaining = 2
        with patch("time.sleep") as mock_sleep:
            result = release.publish("versionctl-fixture")
        self.assertEqual(result["state"], "released")
        self.assertEqual(result["url"], self.github_release["html_url"])
        self.assertEqual(mock_sleep.call_count, 2)
        delays = [call.args[0] for call in mock_sleep.call_args_list]
        self.assertEqual(delays[0], release.VERIFY_INITIAL_DELAY_SECONDS)
        for previous, current in zip(delays, delays[1:]):
            self.assertAlmostEqual(current, min(previous * release.VERIFY_BACKOFF_FACTOR,
                                                release.VERIFY_MAX_DELAY_SECONDS))
        self.assertLessEqual(sum(delays), release.VERIFY_BUDGET_SECONDS)

    def test_genuine_verification_failure_reports_missing_field(self):
        # A release that stays wrong must still fail, naming the exact field.
        self.created_release_override = {"html_url": ""}
        with patch("time.sleep") as mock_sleep:
            with self.assertRaises(RuntimeError) as ctx:
                release.publish("versionctl-fixture")
        message = str(ctx.exception)
        self.assertIn("Release publication could not be verified", message)
        self.assertIn("html_url", message)
        # The malformed release fails fast without consuming the retry budget.
        self.assertEqual(mock_sleep.call_count, 0)


class GitHubReadTests(unittest.TestCase):
    def test_failed_listing_never_means_the_release_is_absent(self):
        for stderr in ["gh: Not Found (HTTP 404)", "gh: Forbidden (HTTP 403)", "network failure"]:
            with self.subTest(stderr=stderr), patch.object(subprocess, "run", return_value=
                    subprocess.CompletedProcess([], 1, "", stderr)):
                with self.assertRaises(RuntimeError):
                    release.command("gh", "api", "unused")


if __name__ == "__main__":
    unittest.main()
