#!/usr/bin/env python3
"""
Generate an H1 2025 GitHub contributions report for a given user.

- Pull PRs, issues, and commits authored between 2025-01-01 and 2025-06-30
- Compute additions/deletions/changed lines for PRs and commits
- Estimate MTTR for issues labeled as incidents (or with incident keywords)
"""

import os
import sys
import time
import json
import math
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional, Tuple
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from urllib.error import HTTPError

GITHUB_API = "https://api.github.com"
H1_START = "2025-01-01T00:00:00Z"
H1_END = "2025-06-30T23:59:59Z"


def get_headers() -> Dict[str, str]:
    token = os.environ.get("GITHUB_TOKEN")
    headers = {"Accept": "application/vnd.github+json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return headers


def github_get(url: str, params: Optional[Dict[str, Any]] = None) -> Any:
    headers = get_headers()
    full_url = url
    if params:
        sep = '&' if '?' in url else '?'
        full_url = f"{url}{sep}{urlencode(params)}"
    req = Request(full_url, headers=headers, method="GET")
    try:
        with urlopen(req, timeout=30) as resp:
            content = resp.read()
            ct = resp.headers.get("Content-Type", "application/json")
            # GitHub returns JSON; decode
            data = json.loads(content.decode("utf-8"))
            # attach headers in case callers need them
            return {"__data__": data, "__headers__": dict(resp.headers)}
    except HTTPError as e:
        # simple rate-limit retry
        body = e.read().decode("utf-8", errors="ignore")
        if e.code == 403 and "rate limit" in body.lower():
            reset = e.headers.get("X-RateLimit-Reset")
            if reset:
                wait_s = max(0, int(reset) - int(time.time()) + 2)
                time.sleep(min(wait_s, 60))
                with urlopen(req, timeout=30) as resp2:
                    content2 = resp2.read()
                    data2 = json.loads(content2.decode("utf-8"))
                    return {"__data__": data2, "__headers__": dict(resp2.headers)}
        raise


def paginate(url: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    results: List[Dict[str, Any]] = []
    page = 1
    while True:
        merged_params = dict(params or {})
        merged_params.update({"per_page": 100, "page": page})
        resp = github_get(url, params=merged_params)
        items = resp.get("__data__")
        if not isinstance(items, list):
            break
        results.extend(items)
        if len(items) < 100:
            break
        page += 1
    return results


def search_github(query: str, sort: Optional[str] = None, order: str = "desc") -> List[Dict[str, Any]]:
    url = f"{GITHUB_API}/search/issues"
    params: Dict[str, Any] = {"q": query, "per_page": 100, "order": order}
    if sort:
        params["sort"] = sort
    results: List[Dict[str, Any]] = []
    page = 1
    while True:
        params["page"] = page
        resp = github_get(url, params=params)
        data = resp.get("__data__", {})
        items = data.get("items", [])
        results.extend(items)
        if len(items) < 100 or page >= 10:
            break
        page += 1
    return results


def within_h1_2025(iso_date: str) -> bool:
    dt = datetime.fromisoformat(iso_date.replace("Z", "+00:00"))
    return datetime.fromisoformat(H1_START.replace("Z", "+00:00")) <= dt <= datetime.fromisoformat(H1_END.replace("Z", "+00:00"))


def fetch_prs(user: str) -> List[Dict[str, Any]]:
    # Use search API for issues with is:pr author:user created:H1
    query = f"is:pr author:{user} created:2025-01-01..2025-06-30"
    items = search_github(query, sort="created")
    # augment with PR details including additions/deletions/changed files
    prs: List[Dict[str, Any]] = []
    for it in items:
        pr_url = it.get("pull_request", {}).get("url")
        if not pr_url:
            # sometimes search returns issues; skip
            continue
        try:
            pr = github_get(pr_url).get("__data__", {})
        except Exception:
            continue
        pr_data = {
            "number": pr.get("number"),
            "title": pr.get("title"),
            "state": pr.get("state"),
            "merged": pr.get("merged"),
            "additions": pr.get("additions"),
            "deletions": pr.get("deletions"),
            "changed_files": pr.get("changed_files"),
            "created_at": pr.get("created_at"),
            "merged_at": pr.get("merged_at"),
            "closed_at": pr.get("closed_at"),
            "repo_full_name": pr.get("base", {}).get("repo", {}).get("full_name"),
            "html_url": pr.get("html_url"),
        }
        prs.append(pr_data)
    return prs


def fetch_issues(user: str) -> List[Dict[str, Any]]:
    query = f"is:issue author:{user} created:2025-01-01..2025-06-30"
    items = search_github(query, sort="created")
    issues: List[Dict[str, Any]] = []
    for it in items:
        issues.append(
            {
                "number": it.get("number"),
                "title": it.get("title"),
                "state": it.get("state"),
                "created_at": it.get("created_at"),
                "closed_at": it.get("closed_at"),
                "repo_full_name": it.get("repository_url", "").replace(f"{GITHUB_API}/repos/", ""),
                "html_url": it.get("html_url"),
                "labels": [l.get("name") for l in it.get("labels", []) if isinstance(l, dict)],
            }
        )
    return issues


def fetch_commits(user: str) -> List[Dict[str, Any]]:
    # Use the commits search API
    # Note: commit search is limited; we will pull up to 1000 results
    url = f"{GITHUB_API}/search/commits"
    headers = get_headers()
    headers["Accept"] = "application/vnd.github.cloak-preview"
    query = f"author:{user} committer-date:2025-01-01..2025-06-30"
    params = {"q": query, "per_page": 100}
    all_items: List[Dict[str, Any]] = []
    page = 1
    while True:
        params["page"] = page
        # build request manually to set Accept header
        full_url = f"{url}?{urlencode(params)}"
        req = Request(full_url, headers=headers, method="GET")
        try:
            with urlopen(req, timeout=30) as resp:
                data = json.loads(resp.read().decode("utf-8"))
        except HTTPError as e:
            body = e.read().decode("utf-8", errors="ignore")
            if e.code == 403 and "rate limit" in body.lower():
                reset = e.headers.get("X-RateLimit-Reset")
                if reset:
                    wait_s = max(0, int(reset) - int(time.time()) + 2)
                    time.sleep(min(wait_s, 60))
                    with urlopen(req, timeout=30) as resp2:
                        data = json.loads(resp2.read().decode("utf-8"))
            else:
                raise
        items = data.get("items", [])
        all_items.extend(items)
        if len(items) < 100 or page >= 10:
            break
        page += 1

    commits: List[Dict[str, Any]] = []
    for it in all_items:
        # shape varies; extract minimal fields and then fetch details for stats
        repo_full = it.get("repository", {}).get("full_name")
        sha = it.get("sha") or it.get("hash")
        html_url = it.get("html_url")
        if not repo_full or not sha:
            # derive repo from URL if possible
            if html_url and "/commit/" in html_url:
                parts = html_url.split("/commit/")
                repo_part = parts[0].replace("https://github.com/", "")
                repo_full = repo_part
                sha = parts[1]
        if not repo_full or not sha:
            continue
        try:
            detail = github_get(f"{GITHUB_API}/repos/{repo_full}/commits/{sha}").get("__data__", {})
            stats = detail.get("stats", {}) or {}
            commits.append(
                {
                    "repo_full_name": repo_full,
                    "sha": detail.get("sha"),
                    "message": detail.get("commit", {}).get("message"),
                    "date": detail.get("commit", {}).get("committer", {}).get("date"),
                    "html_url": detail.get("html_url"),
                    "additions": stats.get("additions"),
                    "deletions": stats.get("deletions"),
                    "total": stats.get("total"),
                }
            )
        except Exception:
            continue
    # filter to H1 (commit API already filtered by date, but be safe)
    commits = [c for c in commits if c.get("date") and within_h1_2025(c["date"])]
    return commits


def calc_mttr_hours(created_iso: Optional[str], closed_iso: Optional[str]) -> Optional[float]:
    if not created_iso or not closed_iso:
        return None
    created = datetime.fromisoformat(created_iso.replace("Z", "+00:00"))
    closed = datetime.fromisoformat(closed_iso.replace("Z", "+00:00"))
    delta = closed - created
    return round(delta.total_seconds() / 3600, 2)


def detect_incident_issue(title: str, labels: List[str]) -> bool:
    text = (title or "").lower()
    labelset = {l.lower() for l in (labels or [])}
    keywords = ["incident", "outage", "sev", "p1", "p0", "security incident"]
    if any(k in text for k in keywords):
        return True
    if any(k in labelset for k in ["incident", "outage", "sev1", "sev2", "p1", "p0"]):
        return True
    return False


def aggregate(user: str) -> Dict[str, Any]:
    prs = fetch_prs(user)
    issues = fetch_issues(user)
    commits = fetch_commits(user)

    totals = {
        "prs_opened": len(prs),
        "prs_merged": sum(1 for p in prs if p.get("merged")),
        "pr_additions": sum(p.get("additions") or 0 for p in prs),
        "pr_deletions": sum(p.get("deletions") or 0 for p in prs),
        "commits": len(commits),
        "commit_additions": sum(c.get("additions") or 0 for c in commits),
        "commit_deletions": sum(c.get("deletions") or 0 for c in commits),
        "issues_opened": len(issues),
        "issues_closed": sum(1 for i in issues if i.get("closed_at")),
    }

    # MTTR for incident-like issues authored by the user
    incident_issues = [i for i in issues if detect_incident_issue(i.get("title", ""), i.get("labels", []))]
    mttrs = [calc_mttr_hours(i.get("created_at"), i.get("closed_at")) for i in incident_issues]
    mttrs = [m for m in mttrs if m is not None]
    totals["incident_issues"] = len(incident_issues)
    totals["incident_mttr_hours_avg"] = round(sum(mttrs) / len(mttrs), 2) if mttrs else None

    return {
        "user": user,
        "window": {"start": H1_START, "end": H1_END},
        "totals": totals,
        "prs": prs,
        "issues": issues,
        "commits": commits,
    }


def main() -> None:
    user = os.environ.get("TARGET_GITHUB_USER") or (sys.argv[1] if len(sys.argv) > 1 else None)
    if not user:
        print("Usage: h1_2025_github_report.py <github-username>")
        sys.exit(2)
    data = aggregate(user)
    out_json = f"/workspace/{user}_h1_2025_report.json"
    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    print(out_json)


if __name__ == "__main__":
    main()

