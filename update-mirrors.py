#!/usr/bin/env python3
"""Rebuild an archlinuxcn mirror status document shaped like
https://archlinux.org/mirrors/status/json/.

Every run is one check round:

  * the mirror list is read from archlinuxcn/mirrorlist-repo (or a local file),
  * every mirror's <arch>/archlinuxcn.db is downloaded, timed and hashed,
  * the round is appended to a check history file,
  * the status document is aggregated from the checks inside --cutoff.

Field semantics follow archweb, the code behind archlinux.org:

  completion_pct   successful checks / all checks in the window
  last_sync        mtime of the mirror's archlinuxcn.db, i.e. the age of the
                   data it serves; archlinuxcn has no lastsync file, so mtime is
                   the only freshness marker mirrors carry. When a mirror sends
                   no Last-Modified the upstream mtime is used if the database
                   content is identical to upstream, otherwise last_sync stays
                   null
  delay            mean(now - last_sync)
  duration_avg     mean download time of archlinuxcn.db
  duration_stddev  sample standard deviation of those times
  score            (delay_hours + duration_avg + duration_stddev) / completion_pct
  active           at least one successful check in the window
  up_to_date       database content identical to repo.archlinuxcn.org right now
  num_checks       most checks any single URL received inside the window
  check_frequency  seconds between the first and the last round of the window

Only the fields of the reference document are guaranteed to be understood by
tools that consume it; name/up_to_date/error are additions.

"url" is the repository root of the mirror ('https://host/archlinuxcn/'), the
same shape archlinux.org reports, so reflector can consume the document:

    reflector --url https://.../mirrors.json --country China --sort score --latest 5

reflector always appends '$repo/os/$arch' to "url", which cannot express the
archlinuxcn layout ('<root>/$arch'). reflector-archlinuxcn, shipped next to this
script, overrides that template and the database path used for rate sorting.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import email.utils
import hashlib
import json
import os
import re
import statistics
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone

DEFAULT_MIRRORLIST = (
    "https://raw.githubusercontent.com/archlinuxcn/mirrorlist-repo/master/"
    "archlinuxcn-mirrorlist"
)
DEFAULT_ARCH = "x86_64"
UPSTREAM_HOST = "repo.archlinuxcn.org"
DB_NAME = "archlinuxcn.db"
USER_AGENT = "archlinuxcn-mirror-status/1.0"
JSON_VERSION = 3

SERVER_RE = re.compile(r"^\s*#?\s*Server\s*=\s*(\S+)\s*$")
PAREN_RE = re.compile(r"\(([^()]*)\)\s*$")

# Ordered: the first rule that matches a location string wins.
COUNTRY_RULES = [
    (("hong kong", "hongkong", "香港"), ("Hong Kong", "HK")),
    (("taiwan", "台湾", "台北", "台南", "新竹"), ("Taiwan", "TW")),
    (("macau", "macao", "澳门"), ("Macau", "MO")),
    (("singapore", "新加坡"), ("Singapore", "SG")),
    (("netherlands", "amsterdam", "荷兰"), ("Netherlands", "NL")),
    (("germany", "duesseldorf", "düsseldorf", "德国"), ("Germany", "DE")),
    (("estonia", "tallinn", "爱沙尼亚"), ("Estonia", "EE")),
    (("japan", "osaka", "日本"), ("Japan", "JP")),
    (("australia", "sydney", "澳大利亚"), ("Australia", "AU")),
    (
        (
            "united states",
            "u.s.",
            "usa",
            "berkeley",
            "san jose",
            "california",
            "美国",
        ),
        ("United States", "US"),
    ),
    (
        (
            "china",
            "cernet",
            "中国",
            "北京",
            "上海",
            "天津",
            "重庆",
            "安徽",
            "合肥",
            "山东",
            "济南",
            "黑龙江",
            "哈尔滨",
            "吉林",
            "长春",
            "浙江",
            "杭州",
            "江苏",
            "南京",
            "广东",
            "深圳",
            "湖北",
            "武汉",
            "甘肃",
            "兰州",
            "陕西",
            "西安",
            "河南",
            "南阳",
            "湖南",
            "长沙",
            "四川",
            "成都",
            "福建",
            "厦门",
            "江西",
            "南昌",
            "辽宁",
            "沈阳",
            "大连",
            "河北",
            "石家庄",
            "山西",
            "太原",
            "内蒙古",
            "呼和浩特",
            "广西",
            "南宁",
            "海南",
            "海口",
            "贵州",
            "贵阳",
            "云南",
            "昆明",
            "西藏",
            "拉萨",
            "宁夏",
            "银川",
            "青海",
            "西宁",
            "新疆",
            "乌鲁木齐",
            "天津",
            "湖北",
            "武昌",
        ),
        ("China", "CN"),
    ),
]


# --------------------------------------------------------------------------- #
# small helpers
# --------------------------------------------------------------------------- #


def iso(ts):
    """Epoch seconds -> 'YYYY-MM-DDTHH:MM:SSZ'."""
    return datetime.fromtimestamp(int(ts), timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_http_date(value):
    if not value:
        return None
    try:
        dt = email.utils.parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.timestamp()


def country_for(location):
    if not location:
        return None, None
    text = location.lower()
    # 'Global CDN w/o China' and plain 'Global CDN' have no single country.
    if "global cdn" in text or "w/o china" in text:
        return None, None
    for needles, result in COUNTRY_RULES:
        if any(needle in text for needle in needles):
            return result
    return None, None


def write_atomic(path, text):
    directory = os.path.dirname(os.path.abspath(path)) or "."
    os.makedirs(directory, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=directory, prefix=".tmp-", suffix=".json")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(tmp, 0o644)
        os.replace(tmp, path)
    except BaseException:
        if os.path.exists(tmp):
            os.unlink(tmp)
        raise


def fetch_text(source, timeout):
    """Read a URL or a local path."""
    if source.startswith(("http://", "https://")):
        request = urllib.request.Request(source, headers={"User-Agent": USER_AGENT})
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return response.read().decode("utf-8", "replace")
    with open(source, encoding="utf-8", errors="replace") as handle:
        return handle.read()


# --------------------------------------------------------------------------- #
# mirror list
# --------------------------------------------------------------------------- #


def parse_mirrorlist(text):
    """Turn the archlinuxcn mirrorlist into mirror records.

    Each server line inherits the comments above it:

        ## xTom (Hong Kong server) (Hong Kong) (ipv4, ipv6, https)
        # Server = https://mirrors.xtom.hk/archlinuxcn/$arch
    """
    mirrors = []
    comments = []
    for raw in text.splitlines():
        line = raw.strip()
        if line.startswith("##"):
            comments.append(line[2:].strip())
            continue
        match = SERVER_RE.match(line)
        if not match:
            continue
        url = match.group(1)
        label = comments[-1] if comments else ""
        comments = []

        flags = []
        flag_match = PAREN_RE.search(label)
        if flag_match:
            candidate = [part.strip() for part in flag_match.group(1).split(",")]
            if candidate and all(
                part in {"ipv4", "ipv6", "http", "https", "rsync", "ftp"}
                for part in candidate
            ):
                flags = candidate
                label = label[: flag_match.start()].strip()

        location = None
        name = label or None
        loc_match = PAREN_RE.search(label)
        if loc_match:
            location = loc_match.group(1).strip()
            name = label[: loc_match.start()].strip() or None

        country, code = country_for(location)
        mirrors.append(
            {
                "url": url,
                "name": name,
                "location": location,
                "country": country or "",
                "country_code": code or "",
                "ipv4": "ipv4" in flags if flags else None,
                "ipv6": "ipv6" in flags if flags else None,
                "protocols": [f for f in flags if f in ("http", "https")]
                or [urllib.parse.urlparse(url).scheme],
            }
        )
    return mirrors


def expand_urls(mirror, arch, mode):
    """Mirror entry -> list of (repo_root, protocol) to check.

    The mirrorlist writes '<root>/$arch', the status document (like
    archlinux.org's) stores '<root>/', because reflector appends the repository
    and architecture itself.
    """
    base = mirror["url"].replace("$arch", arch)
    root = mirror["url"].replace("$arch", "").rstrip("/") + "/"
    scheme = urllib.parse.urlparse(base).scheme
    rest = root.split("://", 1)[1]
    protocols = [scheme]
    if mode == "all":
        declared = [p for p in mirror["protocols"] if p in ("http", "https")]
        # https first, then any other declared protocol
        protocols = [p for p in ("https", "http") if p in declared or p == scheme]
        if scheme not in protocols:
            protocols.append(scheme)
    seen = []
    for protocol in protocols:
        url = f"{protocol}://{rest}"
        if url not in seen:
            seen.append(url)
    return [(url, urllib.parse.urlparse(url).scheme) for url in seen]


# --------------------------------------------------------------------------- #
# checking
# --------------------------------------------------------------------------- #


def check_mirror(repo_root, arch, timeout):
    """Download <repo_root><arch>/archlinuxcn.db and describe the attempt."""
    db_url = f"{repo_root}{arch}/{DB_NAME}"
    request = urllib.request.Request(db_url, headers={"User-Agent": USER_AGENT})
    result = {
        "url": repo_root,
        "db_url": db_url,
        "ok": False,
        "duration": None,
        "last_modified": None,
        "size": None,
        "sha256": None,
        "error": None,
    }
    started = time.monotonic()
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            digest = hashlib.sha256()
            head = b""
            size = 0
            while True:
                chunk = response.read(65536)
                if not chunk:
                    break
                if len(head) < 512:
                    head += chunk[: 512 - len(head)]
                digest.update(chunk)
                size += len(chunk)
            elapsed = time.monotonic() - started
            result.update(
                duration=elapsed,
                size=size,
                sha256=digest.hexdigest(),
                last_modified=parse_http_date(response.headers.get("Last-Modified")),
            )
            if not (head[:2] == b"\x1f\x8b" or head[257:262] == b"ustar"):
                result["error"] = "response is neither gzip nor tar"
            elif size == 0:
                result["error"] = "empty database"
            else:
                result["ok"] = True
    except urllib.error.HTTPError as exc:
        # The mirror answered; the timing is still meaningful (archweb does the
        # same for 404s) but the check counts as failed.
        result["duration"] = time.monotonic() - started
        result["error"] = f"HTTP {exc.code} {exc.reason}"
    except Exception as exc:  # noqa: BLE001 - every transport error is a failure
        result["error"] = f"{type(exc).__name__}: {exc}"[:200]
    return result


def check_all(targets, arch, timeout, workers):
    results = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(check_mirror, url, arch, timeout): url for url in targets
        }
        for future in concurrent.futures.as_completed(futures):
            url = futures[future]
            try:
                results[url] = future.result()
            except Exception as exc:  # noqa: BLE001
                results[url] = {
                    "url": url,
                    "db_url": url,
                    "ok": False,
                    "duration": None,
                    "last_modified": None,
                    "size": None,
                    "sha256": None,
                    "error": f"{type(exc).__name__}: {exc}"[:200],
                }
    return results


# --------------------------------------------------------------------------- #
# history
# --------------------------------------------------------------------------- #


def read_history(path):
    if not os.path.exists(path):
        return []
    records = []
    with open(path, encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except ValueError:
                continue
            if isinstance(record, dict) and "t" in record and "url" in record:
                records.append(record)
    return records


def write_history(path, records):
    directory = os.path.dirname(os.path.abspath(path)) or "."
    os.makedirs(directory, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=directory, prefix=".tmp-", suffix=".jsonl")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            for record in records:
                handle.write(json.dumps(record, separators=(",", ":"), sort_keys=True))
                handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(tmp, 0o644)
        os.replace(tmp, path)
    except BaseException:
        if os.path.exists(tmp):
            os.unlink(tmp)
        raise


# --------------------------------------------------------------------------- #
# aggregation
# --------------------------------------------------------------------------- #


def aggregate(rows):
    """archweb's status aggregation for one URL over the window."""
    rows = sorted(rows, key=lambda record: record["t"])
    successes = [row for row in rows if row.get("ok")]
    durations = [row["duration"] for row in rows if row.get("duration") is not None]
    synced = [row for row in rows if row.get("last_sync") is not None]

    completion = len(successes) / len(rows) if rows else None
    duration_avg = statistics.fmean(durations) if durations else None
    duration_stddev = statistics.stdev(durations) if len(durations) > 1 else (
        0.0 if durations else None
    )
    last_sync = max(row["last_sync"] for row in synced) if synced else None
    delay = (
        statistics.fmean(max(0.0, row["t"] - row["last_sync"]) for row in synced)
        if synced
        else None
    )

    score = None
    if delay is not None and duration_avg is not None:
        divisor = completion if completion and completion > 0 else 0.005
        score = (delay / 3600.0 + duration_avg + (duration_stddev or 0.0)) / divisor

    return {
        "check_count": len(rows),
        "success_count": len(successes),
        "completion_pct": completion,
        "last_sync": last_sync,
        "delay": delay,
        "duration_avg": duration_avg,
        "duration_stddev": duration_stddev,
        "score": score,
    }


def rounded(value, digits=6):
    return None if value is None else round(value, digits)


# --------------------------------------------------------------------------- #
# main
# --------------------------------------------------------------------------- #


def build_status(mirrors, round_results, history, now, cutoff, arch, upstream):
    cutoff_time = now - cutoff
    window = [row for row in history if row["t"] >= cutoff_time]
    per_url = {}
    for row in window:
        per_url.setdefault(row["url"], []).append(row)

    urls = []
    for mirror, base_url, protocol in mirrors:
        stats = aggregate(per_url.get(base_url, []))
        current = round_results.get(base_url) or {}
        last_check = max((row["t"] for row in per_url.get(base_url, [])), default=None)

        last_sync = stats["last_sync"]
        up_to_date = None
        if current.get("ok") and upstream.get("ok"):
            up_to_date = current.get("sha256") == upstream.get("sha256")

        entry = {
            "url": base_url.rstrip("/") + "/",
            "protocol": protocol,
            "last_sync": iso(last_sync) if last_sync is not None else None,
            "completion_pct": rounded(stats["completion_pct"]),
            "delay": int(round(stats["delay"])) if stats["delay"] is not None else None,
            "duration_avg": rounded(stats["duration_avg"]),
            "duration_stddev": rounded(stats["duration_stddev"]),
            "score": rounded(stats["score"]),
            "active": bool(stats["success_count"]),
            "country": mirror["country"],
            "country_code": mirror["country_code"],
            "isos": False,
            "ipv4": mirror["ipv4"],
            "ipv6": mirror["ipv6"],
            "details": None,
            "name": mirror["name"],
            "up_to_date": up_to_date,
            "last_check": iso(last_check) if last_check else None,
        }
        error = current.get("error")
        if error:
            entry["error"] = error
        urls.append(entry)

    urls.sort(key=lambda entry: entry["url"])

    num_checks = max((len(rows) for rows in per_url.values()), default=0)
    timestamps = [row["t"] for row in window]
    check_frequency = None
    if num_checks > 1 and timestamps:
        check_frequency = int(round((max(timestamps) - min(timestamps)) / (num_checks - 1)))

    return {
        "cutoff": cutoff,
        "last_check": iso(now),
        "num_checks": num_checks,
        "check_frequency": check_frequency,
        "urls": urls,
        "version": JSON_VERSION,
        "arch": arch,
        "repo": "archlinuxcn",
    }


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Rebuild the archlinuxcn mirror status JSON (archlinux.org format).",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--mirrorlist",
        default=DEFAULT_MIRRORLIST,
        help="mirrorlist URL or local file to read servers from",
    )
    parser.add_argument("--arch", default=DEFAULT_ARCH, help="repository architecture")
    parser.add_argument("--output", default="mirrors.json", help="status JSON path")
    parser.add_argument(
        "--history",
        default=None,
        help="check history path (default: <output>.history.jsonl)",
    )
    parser.add_argument(
        "--cutoff", type=int, default=86400, help="aggregation window in seconds"
    )
    parser.add_argument(
        "--prune-days", type=float, default=7.0, help="drop history older than this"
    )
    parser.add_argument("--timeout", type=float, default=30.0, help="per-request timeout")
    parser.add_argument("--workers", type=int, default=12, help="parallel checks")
    parser.add_argument(
        "--protocols",
        choices=("listed", "all"),
        default="listed",
        help="'listed' checks only the Server URL, 'all' also the protocols its "
        "mirrorlist comment declares",
    )
    args = parser.parse_args(argv)

    history_path = args.history or f"{args.output}.history.jsonl"
    now = time.time()

    try:
        mirrorlist_text = fetch_text(args.mirrorlist, args.timeout)
    except Exception as exc:  # noqa: BLE001
        print(f"error: cannot read mirrorlist {args.mirrorlist}: {exc}", file=sys.stderr)
        return 1

    parsed = parse_mirrorlist(mirrorlist_text)
    if not parsed:
        print(f"error: no Server lines in {args.mirrorlist}", file=sys.stderr)
        return 1

    targets = []
    for mirror in parsed:
        for base_url, protocol in expand_urls(mirror, args.arch, args.protocols):
            targets.append((mirror, base_url, protocol))

    upstream_url = f"https://{UPSTREAM_HOST}/"
    check_urls = sorted({base_url for _, base_url, _ in targets})
    if upstream_url not in check_urls:
        check_urls.append(upstream_url)

    print(
        f"checking {len(check_urls)} URLs "
        f"({len(targets)} entries from {len(parsed)} mirrors)",
        file=sys.stderr,
    )
    results = check_all(check_urls, args.arch, args.timeout, args.workers)
    upstream = results.get(upstream_url, {})

    history = read_history(history_path)
    round_rows = []
    for mirror, base_url, protocol in targets:
        result = results.get(base_url, {})
        last_sync = result.get("last_modified")
        if (
            last_sync is None
            and result.get("ok")
            and upstream.get("ok")
            and result.get("sha256") == upstream.get("sha256")
        ):
            last_sync = upstream.get("last_modified")
        if last_sync is not None and last_sync > now:
            last_sync = int(now)
        round_rows.append(
            {
                "t": round(now, 3),
                "url": base_url,
                "ok": bool(result.get("ok")),
                "duration": rounded(result.get("duration"), 4),
                "last_sync": int(last_sync) if last_sync is not None else None,
                "error": result.get("error"),
            }
        )

    history = [row for row in history if row["t"] >= now - args.prune_days * 86400]
    history.extend(round_rows)

    document = build_status(
        [(mirror, url, protocol) for mirror, url, protocol in targets],
        results,
        history,
        now,
        args.cutoff,
        args.arch,
        upstream,
    )

    write_history(history_path, history)
    write_atomic(args.output, json.dumps(document, indent=1, ensure_ascii=False) + "\n")

    ok_urls = sum(1 for entry in document["urls"] if entry["active"])
    fresh_urls = sum(1 for entry in document["urls"] if entry["up_to_date"])
    print(
        f"wrote {args.output}: {len(document['urls'])} urls, "
        f"{ok_urls} reachable, {fresh_urls} identical to upstream, "
        f"{len(history)} history rows",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
