#!/usr/bin/env python3
"""Publish a validated main-branch version; safely resume a partial release."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import tempfile
import time


ROOT = Path(__file__).resolve().parents[1]
REPOSITORY = "toolboxmd/model-router"

# Post-creation verification budget for the eventually consistent release API.
# Bounded retries with exponential backoff: never judge a just-created release
# on its first unreadable read.
VERIFY_MAX_ATTEMPTS = 6
VERIFY_INITIAL_DELAY_SECONDS = 1.0
VERIFY_BACKOFF_FACTOR = 2.0
VERIFY_MAX_DELAY_SECONDS = 8.0
VERIFY_BUDGET_SECONDS = 30.0


def command(*args):
    result = subprocess.run(args, cwd=ROOT, text=True, capture_output=True)
    if result.returncode:
        raise RuntimeError(result.stderr.strip() or result.stdout.strip() or
                           f"{args[0]} exited {result.returncode}")
    return result.stdout.strip()


def remote_tag(tag, sha):
    ref = f"refs/tags/{tag}"
    refs = dict(line.split()[::-1] for line in
                command("git", "ls-remote", "--tags", "origin", ref, ref + "^{}").splitlines())
    if ref not in refs:
        return False
    if refs.get(ref + "^{}") != sha:
        raise RuntimeError(f"{tag} is not an annotated tag on the validated commit")
    return True


def _sleep(seconds):
    time.sleep(seconds)


def read_release(tag):
    # Tag lookup excludes drafts. The paginated listing includes them for the
    # workflow's contents-write token, so conflicts are found before any write.
    pages = json.loads(command("gh", "api", "--paginate", "--slurp",
                               f"repos/{REPOSITORY}/releases?per_page=100"))
    matches = [item for page in pages for item in page if item.get("tag_name") == tag]
    if not matches:
        return None
    if len(matches) != 1:
        raise RuntimeError("More than one release uses the requested tag")
    release = matches[0]
    problems = []
    if release.get("tag_name") != tag:
        problems.append(f"tag_name is {release.get('tag_name')!r}, expected {tag!r}")
    if release.get("draft") is not False:
        problems.append(f"draft is {release.get('draft')!r}, expected False")
    if release.get("prerelease") is not False:
        problems.append(f"prerelease is {release.get('prerelease')!r}, expected False")
    if not release.get("html_url"):
        problems.append("html_url is missing or empty")
    if problems:
        raise RuntimeError("Existing release is not the expected published stable release: "
                           + "; ".join(problems))
    return release


def verify_publication(tag, sha):
    """Wait for a just-created release to become readable, then verify it.

    The release API is eventually consistent: a release created moments ago
    may not be listed yet. Retry unreadable reads with exponential backoff
    inside a fixed budget. Genuine failures (a release with wrong fields, a
    duplicate tag, a missing remote tag) fail immediately with the exact
    missing field instead of being retried.
    """
    delay = VERIFY_INITIAL_DELAY_SECONDS
    start = time.monotonic()
    attempts = 0
    last_error = None
    release = None
    while True:
        attempts += 1
        try:
            release = read_release(tag)
        except RuntimeError as exc:
            message = str(exc)
            if "More than one release" in message or "expected published stable" in message:
                raise RuntimeError(f"Release publication could not be verified: {exc}") from exc
            last_error = exc
            release = None
        if release is not None:
            break
        if attempts >= VERIFY_MAX_ATTEMPTS:
            break
        if time.monotonic() - start + delay > VERIFY_BUDGET_SECONDS:
            break
        _sleep(delay)
        delay = min(delay * VERIFY_BACKOFF_FACTOR, VERIFY_MAX_DELAY_SECONDS)
    if release is None:
        if last_error is not None:
            raise RuntimeError(
                f"Release publication could not be verified: release {tag} not readable "
                f"after {attempts} attempts within {VERIFY_BUDGET_SECONDS:.0f}s budget: "
                f"last error: {last_error}") from last_error
        raise RuntimeError(
            f"Release publication could not be verified: release {tag} not readable "
            f"after {attempts} attempts within {VERIFY_BUDGET_SECONDS:.0f}s budget")
    try:
        tag_ok = remote_tag(tag, sha)
    except RuntimeError as exc:
        raise RuntimeError(f"Release publication could not be verified: {exc}") from exc
    if not tag_ok:
        raise RuntimeError(
            f"Release publication could not be verified: remote tag {tag} "
            "missing for the validated commit")
    return release


def publish(versionctl):
    if (os.environ.get("GITHUB_REPOSITORY") != REPOSITORY or
            os.environ.get("GITHUB_REF") != "refs/heads/main" or
            os.environ.get("GITHUB_EVENT_NAME") not in {"push", "workflow_dispatch"}):
        raise RuntimeError("Release requires this repository's main-branch workflow")
    sha = command("git", "rev-parse", "HEAD")
    if sha != os.environ.get("GITHUB_SHA"):
        raise RuntimeError("Checkout does not match the exact workflow commit")
    if command("git", "remote", "get-url", "origin") not in {
            f"https://github.com/{REPOSITORY}", f"https://github.com/{REPOSITORY}.git",
            f"git@github.com:{REPOSITORY}.git"}:
        raise RuntimeError("Origin is not the authorized source repository")
    policy = json.loads((ROOT / ".version-policy.json").read_text())
    if policy["githubReleasePolicy"] != "on-version-commit":
        return {"state": "disabled-by-policy", "sha": sha}
    if policy.get("releaseBranch", "main") != "main":
        raise RuntimeError("Release branch no longer matches the workflow")
    report = json.loads(command(versionctl, "release-check", "--sha", sha, "--json"))
    if not report.get("ok") or report.get("sha") != sha:
        raise RuntimeError("Version validator did not approve the exact commit")

    tag = report["tag"]
    tagged = remote_tag(tag, sha)
    release = read_release(tag)
    if release and not tagged:
        raise RuntimeError("Published release has no matching remote annotated tag")
    state = "already-released" if release else "released"
    if not tagged:
        if not report["tagExists"]:
            command("git", "-c", "user.name=github-actions[bot]", "-c",
                    "user.email=41898282+github-actions[bot]@users.noreply.github.com",
                    "tag", "-a", tag, sha, "-m", f"Release {tag}")
        command("git", "push", "origin", f"refs/tags/{tag}")
        if not remote_tag(tag, sha):
            raise RuntimeError("Pushed tag is missing from the remote")
    if release is None:
        with tempfile.TemporaryDirectory(prefix="model-router-release-") as tmp:
            notes = Path(tmp) / "notes.md"
            notes.write_text(report["changelogEntry"], encoding="utf-8")
            command("gh", "release", "create", tag, "--repo", REPOSITORY,
                    "--verify-tag", "--target", sha, "--title", tag,
                    "--notes-file", str(notes))
        release = verify_publication(tag, sha)
    return {"state": state, "sha": sha, "tag": tag, "version": report["version"],
            "url": release["html_url"], "marketplace": "awaiting-toolybara"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--versionctl", required=True, help="Path to the pinned release validator")
    args = parser.parse_args()
    try:
        result = publish(args.versionctl)
    except (RuntimeError, OSError, ValueError, KeyError) as exc:
        parser.exit(1, f"Release failed: {exc}\n")
    print(json.dumps(result, sort_keys=True))
    if summary := os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(summary, "a", encoding="utf-8") as handle:
            handle.write("### Model Router release\n\n")
            for key, value in result.items():
                handle.write(f"- {key}: `{value}`\n")
            handle.write("\nToolybara's hourly scan owns Marketplace promotion. "
                         "Installation and behavioral verification are separate.\n")


if __name__ == "__main__":
    main()
