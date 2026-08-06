#!/usr/bin/env python3
"""
Analyze GitHub contributions for a user between two dates.

Data collected:
- Pull requests authored: repo, number, title, state, created/merged/closed times, additions, deletions, changed_files, commits, merge_commit_sha, html_url, cycle_time_days
- Issues authored: repo, number, title, state, created/closed times, labels, html_url, time_to_close_days
- PRs reviewed (by user): repo, number, title, state, created/updated times, html_url

Outputs a JSON file with aggregates:
- totals: prs_created, prs_merged, lines_added, lines_deleted, files_changed, avg_cycle_time_days
- issues: totals and MTTR for incident-like issues (by label heuristics)
- repos_contributed: list of repos via PRs

Auth: optional GITHUB_TOKEN env var to increase rate limits.
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Optional, Tuple
from urllib.parse import urlencode
from urllib.request import Request, urlopen


GITHUB_API = "https://api.github.com"
USER_AGENT = "contrib-analyzer/1.0"


def get_auth_headers() -> Dict[str, str]:
    headers = {"User-Agent": USER_AGENT, "Accept": "application/vnd.github+json"}
    token = os.environ.get("GITHUB_TOKEN")
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return headers


def http_get(url: str, params: Optional[Dict[str, str]] = None, headers_override: Optional[Dict[str, str]] = None) -> Tuple[Dict, Dict[str, str]]:
    if params:
        url = f"{url}?{urlencode(params)}"
    headers = get_auth_headers()
    if headers_override:
        headers.update(headers_override)
    req = Request(url, headers=headers)
    with urlopen(req) as resp:
        data = json.loads(resp.read().decode("utf-8"))
        headers = {k.lower(): v for k, v in resp.headers.items()}
        return data, headers


def parse_link_header(link_header: Optional[str]) -> Dict[str, str]:
    if not link_header:
        return {}
    parts = link_header.split(",")
    links: Dict[str, str] = {}
    for part in parts:
        section = part.strip().split(";")
        if len(section) < 2:
            continue
        url = section[0].strip().lstrip("<").rstrip(">")
        rel = None
        for sec in section[1:]:
            sec = sec.strip()
            if sec.startswith("rel="):
                rel = sec.split("=", 1)[1].strip('"')
                break
        if rel:
            links[rel] = url
    return links


def paginate(url: str, params: Optional[Dict[str, str]] = None, item_key: Optional[str] = None) -> Iterable[Dict]:
    page = 1
    per_page = 100
    params = dict(params or {})
    params.update({"per_page": str(per_page), "page": str(page)})
    while True:
        data, headers = http_get(url, params)
        items = data.get(item_key) if item_key else data
        if not items:
            break
        for item in items:
            yield item
        links = parse_link_header(headers.get("link"))
        if "next" not in links:
            break
        # parse next page number from the next link
        next_url = links["next"]
        # reset params based on next_url query is simpler: just loop by incrementing page
        page += 1
        params["page"] = str(page)
        # Be gentle with API
        time.sleep(0.15)


def search_issues(query: str) -> Iterable[Dict]:
    url = f"{GITHUB_API}/search/issues"
    page = 1
    per_page = 100
    total_returned = 0
    while True:
        params = {"q": query, "per_page": str(per_page), "page": str(page)}
        data, _ = http_get(url, params)
        items = data.get("items", [])
        if not items:
            break
        for item in items:
            yield item
        total_returned += len(items)
        if len(items) < per_page or total_returned >= 1000:  # search cap
            break
        page += 1
        time.sleep(0.15)


def search_commits(query: str) -> Iterable[Dict]:
    """Search commits using GitHub's commit search API."""
    url = f"{GITHUB_API}/search/commits"
    page = 1
    per_page = 100
    total_returned = 0
    # Requires a custom Accept header
    headers_override = {"Accept": "application/vnd.github.text-match+json"}
    while True:
        params = {"q": query, "per_page": str(per_page), "page": str(page)}
        data, _ = http_get(url, params, headers_override=headers_override)
        items = data.get("items", [])
        if not items:
            break
        for item in items:
            yield item
        total_returned += len(items)
        if len(items) < per_page or total_returned >= 1000:
            break
        page += 1
        time.sleep(0.15)


def get_pr_details(repo_full_name: str, number: int) -> Dict:
    url = f"{GITHUB_API}/repos/{repo_full_name}/pulls/{number}"
    pr, _ = http_get(url)
    # Not fetching files by default to save rate limit; pr has additions/deletions
    return pr


def get_commit_details(repo_full_name: str, sha: str) -> Dict:
    url = f"{GITHUB_API}/repos/{repo_full_name}/commits/{sha}"
    commit, _ = http_get(url)
    return commit


def get_pulls_for_commit(repo_full_name: str, sha: str) -> List[Dict]:
    url = f"{GITHUB_API}/repos/{repo_full_name}/commits/{sha}/pulls"
    # Preview header required
    data, _ = http_get(url, headers_override={"Accept": "application/vnd.github.groot-preview+json"})
    if isinstance(data, list):
        return data
    return []


def iso_to_dt(iso_str: Optional[str]) -> Optional[datetime]:
    if not iso_str:
        return None
    return datetime.fromisoformat(iso_str.replace("Z", "+00:00")).astimezone(timezone.utc)


def days_between(start: Optional[datetime], end: Optional[datetime]) -> Optional[float]:
    if not start or not end:
        return None
    delta = end - start
    return round(delta.total_seconds() / 86400.0, 2)


def is_incident_issue(labels: List[Dict]) -> bool:
    incident_markers = [
        "incident",
        "sev",
        "severity",
        "p0",
        "p1",
        "s1",
        "s2",
        "outage",
        "hotfix",
    ]
    label_names = [lbl.get("name", "").lower() for lbl in labels or []]
    return any(any(marker in name for marker in incident_markers) for name in label_names)


def safe_repo_full_name(item: Dict) -> Optional[str]:
    # search/issues gives repository_url like https://api.github.com/repos/{owner}/{repo}
    repo_url = item.get("repository_url")
    if not repo_url:
        return None
    parts = repo_url.rstrip("/").split("/")
    if len(parts) < 2:
        return None
    owner = parts[-2]
    repo = parts[-1]
    return f"{owner}/{repo}"


def analyze(user: str, start_date: str, end_date: str) -> Dict:
    date_range = f"{start_date}..{end_date}"

    # PRs authored by the user
    pr_query = f"is:pr author:{user} created:{date_range}"
    pr_items: List[Dict] = list(search_issues(pr_query))

    prs_detailed: List[Dict] = []
    for item in pr_items:
        repo_full_name = safe_repo_full_name(item)
        number = item.get("number")
        if not repo_full_name or not number:
            continue
        try:
            pr = get_pr_details(repo_full_name, int(number))
        except Exception as e:
            # fall back to minimal info if PR fetch fails
            pr = {
                "number": number,
                "title": item.get("title"),
                "state": item.get("state"),
                "created_at": item.get("created_at"),
                "merged_at": None,
                "closed_at": item.get("closed_at"),
                "additions": 0,
                "deletions": 0,
                "changed_files": 0,
                "commits": None,
                "merge_commit_sha": None,
                "html_url": item.get("html_url"),
                "_fetch_error": str(e),
            }
        created_dt = iso_to_dt(pr.get("created_at"))
        end_dt = iso_to_dt(pr.get("merged_at")) or iso_to_dt(pr.get("closed_at"))
        cycle_days = days_between(created_dt, end_dt)
        prs_detailed.append(
            {
                "repo": repo_full_name,
                "number": pr.get("number"),
                "title": pr.get("title"),
                "state": pr.get("state"),
                "created_at": pr.get("created_at"),
                "merged_at": pr.get("merged_at"),
                "closed_at": pr.get("closed_at"),
                "additions": pr.get("additions", 0),
                "deletions": pr.get("deletions", 0),
                "changed_files": pr.get("changed_files", 0),
                "commits": pr.get("commits"),
                "merge_commit_sha": pr.get("merge_commit_sha"),
                "html_url": pr.get("html_url"),
                "cycle_time_days": cycle_days,
            }
        )
        # sleep slightly to be kind to API
        time.sleep(0.08)

    # Issues authored by the user
    issue_query = f"is:issue author:{user} created:{date_range}"
    issue_items: List[Dict] = list(search_issues(issue_query))
    issues_detailed: List[Dict] = []
    for it in issue_items:
        repo_full_name = safe_repo_full_name(it)
        created_at = it.get("created_at")
        closed_at = it.get("closed_at")
        created_dt = iso_to_dt(created_at)
        closed_dt = iso_to_dt(closed_at)
        ttc_days = days_between(created_dt, closed_dt)
        issues_detailed.append(
            {
                "repo": repo_full_name,
                "number": it.get("number"),
                "title": it.get("title"),
                "state": it.get("state"),
                "created_at": created_at,
                "closed_at": closed_at,
                "labels": it.get("labels", []),
                "html_url": it.get("html_url"),
                "time_to_close_days": ttc_days,
                "is_incident": is_incident_issue(it.get("labels", [])),
            }
        )

    # Issues assigned to the user (to better estimate MTTR even if not author)
    issue_assigned_query = f"is:issue assignee:{user} closed:{date_range}"
    issue_assigned_items: List[Dict] = list(search_issues(issue_assigned_query))
    issues_assigned: List[Dict] = []
    for it in issue_assigned_items:
        repo_full_name = safe_repo_full_name(it)
        created_at = it.get("created_at")
        closed_at = it.get("closed_at")
        created_dt = iso_to_dt(created_at)
        closed_dt = iso_to_dt(closed_at)
        ttc_days = days_between(created_dt, closed_dt)
        issues_assigned.append(
            {
                "repo": repo_full_name,
                "number": it.get("number"),
                "title": it.get("title"),
                "state": it.get("state"),
                "created_at": created_at,
                "closed_at": closed_at,
                "labels": it.get("labels", []),
                "html_url": it.get("html_url"),
                "time_to_close_days": ttc_days,
                "is_incident": is_incident_issue(it.get("labels", [])),
            }
        )

    # PRs reviewed by the user
    reviewed_query = f"is:pr reviewed-by:{user} updated:{date_range}"
    reviewed_items: List[Dict] = list(search_issues(reviewed_query))
    reviewed_slim: List[Dict] = []
    for it in reviewed_items:
        reviewed_slim.append(
            {
                "repo": safe_repo_full_name(it),
                "number": it.get("number"),
                "title": it.get("title"),
                "state": it.get("state"),
                "created_at": it.get("created_at"),
                "updated_at": it.get("updated_at"),
                "html_url": it.get("html_url"),
            }
        )

    # Commits authored by or committed by the user within the date range
    commit_query_author = f"author:{user} author-date:{date_range}"
    commit_query_committer = f"committer:{user} committer-date:{date_range}"
    commit_items = list(search_commits(commit_query_author)) + list(search_commits(commit_query_committer))
    # dedupe by sha
    seen_shas: set = set()
    commits_detailed: List[Dict] = []
    for item in commit_items:
        sha = item.get("sha")
        if not sha or sha in seen_shas:
            continue
        seen_shas.add(sha)
        repo_info = item.get("repository") or {}
        repo_full_name = repo_info.get("full_name")
        if not repo_full_name:
            # attempt to parse from html_url
            html_url = item.get("html_url") or ""
            parts = html_url.split("/")
            if len(parts) >= 7:
                repo_full_name = f"{parts[3]}/{parts[4]}"
        try:
            commit_detail = get_commit_details(repo_full_name, sha) if repo_full_name else {}
        except Exception as e:
            commit_detail = {"_fetch_error": str(e)}
        stats = commit_detail.get("stats") or {}
        files = commit_detail.get("files") or []
        additions = int(stats.get("additions") or 0)
        deletions = int(stats.get("deletions") or 0)
        total = int(stats.get("total") or (additions + deletions))
        commit_url = (commit_detail.get("html_url") or item.get("html_url"))
        commit_date = (
            (commit_detail.get("commit") or {}).get("author", {}).get("date")
            or (commit_detail.get("commit") or {}).get("committer", {}).get("date")
        )
        message = (commit_detail.get("commit") or {}).get("message") or (item.get("commit") or {}).get("message")
        # map to PRs if any
        prs_for_commit = []
        if repo_full_name:
            try:
                prs_for_commit = get_pulls_for_commit(repo_full_name, sha)
            except Exception:
                prs_for_commit = []
        commits_detailed.append(
            {
                "repo": repo_full_name,
                "sha": sha,
                "date": commit_date,
                "message": message,
                "html_url": commit_url,
                "additions": additions,
                "deletions": deletions,
                "total_changes": total,
                "files_changed": len(files),
                "associated_prs": [
                    {"number": pr.get("number"), "html_url": pr.get("html_url"), "state": pr.get("state")}
                    for pr in prs_for_commit
                ],
            }
        )
        time.sleep(0.05)

    # Aggregates
    total_additions = sum(int(p.get("additions") or 0) for p in prs_detailed)
    total_deletions = sum(int(p.get("deletions") or 0) for p in prs_detailed)
    total_files_changed = sum(int(p.get("changed_files") or 0) for p in prs_detailed)
    merged_prs = [p for p in prs_detailed if p.get("merged_at")]
    cycle_times = [p["cycle_time_days"] for p in prs_detailed if p.get("cycle_time_days") is not None]
    avg_cycle_time = round(sum(cycle_times) / len(cycle_times), 2) if cycle_times else None

    # Issues MTTR for incidents
    incident_issues = [i for i in issues_detailed if i.get("is_incident")] + [i for i in issues_assigned if i.get("is_incident")]
    incident_ttrs = [i["time_to_close_days"] for i in incident_issues if i.get("time_to_close_days") is not None]
    avg_incident_mttr = round(sum(incident_ttrs) / len(incident_ttrs), 2) if incident_ttrs else None

    # General issue close time
    all_ttrs = [i["time_to_close_days"] for i in (issues_detailed + issues_assigned) if i.get("time_to_close_days") is not None]
    avg_issue_ttr = round(sum(all_ttrs) / len(all_ttrs), 2) if all_ttrs else None

    repos_contributed = sorted({r for r in ([p.get("repo") for p in prs_detailed] + [c.get("repo") for c in commits_detailed]) if r})

    result = {
        "user": user,
        "start_date": start_date,
        "end_date": end_date,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "pull_requests": prs_detailed,
        "issues": issues_detailed,
        "issues_assigned": issues_assigned,
        "reviews": reviewed_slim,
        "commits": commits_detailed,
        "aggregates": {
            "prs_created": len(prs_detailed),
            "prs_merged": len(merged_prs),
            "lines_added": total_additions,
            "lines_deleted": total_deletions,
            "files_changed": total_files_changed,
            "avg_cycle_time_days": avg_cycle_time,
            "repos_contributed": repos_contributed,
            "issues_created": len(issues_detailed),
            "avg_issue_close_time_days": avg_issue_ttr,
            "incident_issues_count": len(incident_issues),
            "avg_incident_mttr_days": avg_incident_mttr,
            "prs_reviewed": len(reviewed_slim),
            "commits_count": len(commits_detailed),
            "commit_lines_changed": sum(int(c.get("total_changes") or 0) for c in commits_detailed),
            "commit_additions": sum(int(c.get("additions") or 0) for c in commits_detailed),
            "commit_deletions": sum(int(c.get("deletions") or 0) for c in commits_detailed),
        },
    }

    return result


def main() -> None:
    parser = argparse.ArgumentParser(description="Analyze GitHub contributions for a user in a date range")
    parser.add_argument("--user", required=True, help="GitHub username")
    parser.add_argument("--start", required=True, help="Start date YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="End date YYYY-MM-DD")
    parser.add_argument("--out", required=True, help="Output JSON path")
    args = parser.parse_args()

    try:
        data = analyze(args.user, args.start, args.end)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    print(f"Wrote {args.out}")


if __name__ == "__main__":
    main()

