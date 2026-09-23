#!/usr/bin/env python3
"""Publish a validated main-branch version; safely resume a partial release."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import tempfile


ROOT = Path(__file__).resolve().parents[1]
REPOSITORY = "toolboxmd/model-router"


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
    if (release.get("tag_name") != tag or release.get("draft") is not False or
            release.get("prerelease") is not False or not release.get("html_url")):
        raise RuntimeError("Existing release is not the expected published stable release")
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
        release = read_release(tag)
        if release is None or not remote_tag(tag, sha):
            raise RuntimeError("Release publication could not be verified")
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
