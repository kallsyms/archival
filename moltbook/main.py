import argparse
import asyncio
import json
import os
import sys
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Deque, Dict, Iterable, List, Optional

import httpx
import zstandard as zstd
from tqdm import tqdm

API_BASE = "https://www.moltbook.com/api/v1"


@dataclass
class Config:
    output_dir: str
    api_key: Optional[str]
    poll_seconds: float
    comment_min_age: float
    comment_max_age: float
    comment_max_attempts: int
    page_limit: int
    backfill: bool
    progress: bool


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def parse_dt(value: str) -> datetime:
    return datetime.fromisoformat(value)


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def state_path(output_dir: str) -> str:
    return os.path.join(output_dir, "state.json")


def load_state(output_dir: str) -> Dict[str, Any]:
    path = state_path(output_dir)
    if not os.path.exists(path):
        return {
            "last_seen_created_at": None,
            "last_seen_ids": [],
            "oldest_created_at": None,
            "pending_posts": [],
        }
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def save_state(output_dir: str, state: Dict[str, Any]) -> None:
    path = state_path(output_dir)
    tmp_path = path + ".tmp"
    with open(tmp_path, "w", encoding="utf-8") as handle:
        json.dump(state, handle, indent=2, sort_keys=True)
    os.replace(tmp_path, path)


def zstd_append_line(path: str, record: Dict[str, Any]) -> None:
    line = json.dumps(record, ensure_ascii=True, separators=(",", ":")).encode("utf-8") + b"\n"
    compressor = zstd.ZstdCompressor(level=3)
    with open(path, "ab") as handle:
        with compressor.stream_writer(handle) as writer:
            writer.write(line)


def posts_path(output_dir: str, timestamp: datetime) -> str:
    return os.path.join(output_dir, f"posts-{timestamp:%Y%m%d}.ndjson.zst")


def comments_path(output_dir: str, timestamp: datetime) -> str:
    return os.path.join(output_dir, f"comments-{timestamp:%Y%m%d}.ndjson.zst")


def record_post(output_dir: str, post: Dict[str, Any]) -> None:
    created_at = parse_dt(post["created_at"])
    record = {
        "type": "post",
        "fetched_at": utc_now().isoformat(),
        "post": post,
    }
    zstd_append_line(posts_path(output_dir, created_at), record)


def record_comment(output_dir: str, post_id: str, comment: Dict[str, Any], depth: int) -> None:
    created_at = parse_dt(comment["created_at"])
    record = {
        "type": "comment",
        "fetched_at": utc_now().isoformat(),
        "post_id": post_id,
        "depth": depth,
        "comment": comment,
    }
    zstd_append_line(comments_path(output_dir, created_at), record)


def flatten_comments(comments: Iterable[Dict[str, Any]], depth: int = 0) -> Iterable[Dict[str, Any]]:
    for comment in comments:
        yield (comment, depth)
        replies = comment.get("replies") or []
        yield from flatten_comments(replies, depth + 1)


def build_headers(api_key: Optional[str]) -> Dict[str, str]:
    headers = {"Accept": "application/json"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    return headers


async def request_json(client: httpx.AsyncClient, method: str, url: str, **kwargs: Any) -> Dict[str, Any]:
    response = await client.request(method, url, **kwargs)
    if response.status_code == 429:
        retry_after = 60
        try:
            payload = response.json()
            retry_after = int(payload.get("retry_after_minutes", 1)) * 60
        except Exception:
            pass
        await asyncio.sleep(retry_after)
        response = await client.request(method, url, **kwargs)
    response.raise_for_status()
    data = response.json()
    if not data.get("success", True):
        raise RuntimeError(data.get("error") or "Unknown API error")
    return data


async def fetch_posts_page(client: httpx.AsyncClient, limit: int, offset: Optional[int]) -> Dict[str, Any]:
    params: Dict[str, Any] = {"sort": "new", "limit": limit}
    if offset is not None:
        params["offset"] = offset
    return await request_json(client, "GET", f"{API_BASE}/posts", params=params)


async def fetch_post_with_comments(client: httpx.AsyncClient, post_id: str) -> Dict[str, Any]:
    return await request_json(client, "GET", f"{API_BASE}/posts/{post_id}")


def should_accept_post(post: Dict[str, Any], last_seen_created_at: Optional[str], last_seen_ids: List[str]) -> bool:
    if last_seen_created_at is None:
        return True
    created_at = post["created_at"]
    if created_at > last_seen_created_at:
        return True
    if created_at == last_seen_created_at and post["id"] not in set(last_seen_ids):
        return True
    return False


def update_last_seen(state: Dict[str, Any], posts: List[Dict[str, Any]]) -> None:
    if not posts:
        return
    newest = max(posts, key=lambda item: item["created_at"])
    newest_ts = newest["created_at"]
    ids = [post["id"] for post in posts if post["created_at"] == newest_ts]
    state["last_seen_created_at"] = newest_ts
    state["last_seen_ids"] = ids


def enqueue_pending(state: Dict[str, Any], posts: Iterable[Dict[str, Any]]) -> None:
    pending = state.get("pending_posts", [])
    existing = {item["id"] for item in pending}
    for post in posts:
        if post["id"] in existing:
            continue
        pending.append({"id": post["id"], "created_at": post["created_at"], "attempts": 0})
    state["pending_posts"] = pending


def prune_pending(pending: Deque[Dict[str, Any]], max_age: float) -> Deque[Dict[str, Any]]:
    now = utc_now()
    kept: Deque[Dict[str, Any]] = deque()
    for item in pending:
        age = (now - parse_dt(item["created_at"])).total_seconds()
        if age <= max_age:
            kept.append(item)
    return kept


async def backfill_posts(
    client: httpx.AsyncClient,
    config: Config,
    state: Dict[str, Any],
) -> None:
    offset = 0
    seen_ids: set[str] = set()
    previous_oldest = state.get("oldest_created_at")
    oldest_seen = previous_oldest
    pbar = tqdm(desc="Backfill posts", unit="post", disable=not config.progress)
    while True:
        page = await fetch_posts_page(client, config.page_limit, offset)
        posts = page.get("posts", [])
        if not posts:
            break
        new_in_page = False
        for post in posts:
            if post["id"] in seen_ids:
                continue
            new_in_page = True
            seen_ids.add(post["id"])
            record_post(config.output_dir, post)
            pbar.update(1)
        if not new_in_page:
            break
        min_created_at = min(post["created_at"] for post in posts)
        if oldest_seen is None or min_created_at < oldest_seen:
            oldest_seen = min_created_at
        state["oldest_created_at"] = oldest_seen
        save_state(config.output_dir, state)
        if page.get("has_more") is False:
            break
        if previous_oldest and min_created_at <= previous_oldest:
            break
        offset = page.get("next_offset", offset + len(posts))
        await asyncio.sleep(0.4)
    pbar.close()


def drain_ready_pending(pending: Deque[Dict[str, Any]], min_age: float) -> List[Dict[str, Any]]:
    now = utc_now()
    ready: List[Dict[str, Any]] = []
    remaining: Deque[Dict[str, Any]] = deque()
    for item in pending:
        age = (now - parse_dt(item["created_at"])).total_seconds()
        if age >= min_age:
            ready.append(item)
        else:
            remaining.append(item)
    pending.clear()
    pending.extend(remaining)
    return ready


async def process_comments(
    client: httpx.AsyncClient,
    config: Config,
    state: Dict[str, Any],
) -> Dict[str, int]:
    pending_list = state.get("pending_posts", [])
    pending: Deque[Dict[str, Any]] = deque(pending_list)
    pending = prune_pending(pending, config.comment_max_age)
    ready = drain_ready_pending(pending, config.comment_min_age)
    posts_checked = 0
    comments_written = 0
    for item in ready:
        try:
            response = await fetch_post_with_comments(client, item["id"])
            comments = response.get("comments", [])
            for comment, depth in flatten_comments(comments):
                record_comment(config.output_dir, item["id"], comment, depth)
                comments_written += 1
            posts_checked += 1
        except Exception:
            item["attempts"] = item.get("attempts", 0) + 1
            if item["attempts"] < config.comment_max_attempts:
                pending.append(item)
        await asyncio.sleep(0.2)
    state["pending_posts"] = list(pending)
    return {"posts_checked": posts_checked, "comments_written": comments_written, "pending": len(pending)}


async def live_loop(client: httpx.AsyncClient, config: Config, state: Dict[str, Any]) -> None:
    logger = tqdm.write if config.progress else print
    while True:
        page = await fetch_posts_page(client, config.page_limit, 0)
        posts = page.get("posts", [])
        new_posts = [post for post in posts if should_accept_post(post, state.get("last_seen_created_at"), state.get("last_seen_ids", []))]
        new_posts.sort(key=lambda item: item["created_at"])
        for post in new_posts:
            record_post(config.output_dir, post)
        if new_posts:
            enqueue_pending(state, new_posts)
            update_last_seen(state, new_posts)
            save_state(config.output_dir, state)
        stats = await process_comments(client, config, state)
        save_state(config.output_dir, state)
        logger(
            f"{utc_now().isoformat()} new_posts={len(new_posts)} "
            f"comments_written={stats['comments_written']} "
            f"posts_checked={stats['posts_checked']} "
            f"pending={stats['pending']}"
        )
        await asyncio.sleep(config.poll_seconds)


def parse_args(argv: Optional[List[str]] = None) -> Config:
    parser = argparse.ArgumentParser(description="Archive Moltbook posts and comments to Zstd NDJSON.")
    parser.add_argument("output_dir", help="Directory to write NDJSON.zst files and state.json")
    parser.add_argument("--api-key", default=os.getenv("MOLTBOOK_API_KEY"), help="Moltbook API key (optional)")
    parser.add_argument("--poll-seconds", type=float, default=20.0, help="Seconds between polling /posts")
    parser.add_argument("--comment-min-age", type=float, default=180.0, help="Seconds after post creation to fetch comments")
    parser.add_argument("--comment-max-age", type=float, default=600.0, help="Drop pending comments after this age (seconds)")
    parser.add_argument("--comment-max-attempts", type=int, default=3, help="Max retries for comment fetch")
    parser.add_argument("--page-limit", type=int, default=50, help="Page size for /posts feed")
    parser.add_argument("--no-backfill", action="store_true", help="Skip historical backfill")
    parser.add_argument("--no-progress", action="store_true", help="Disable tqdm progress + loop stats")
    args = parser.parse_args(argv)
    return Config(
        output_dir=args.output_dir,
        api_key=args.api_key,
        poll_seconds=args.poll_seconds,
        comment_min_age=args.comment_min_age,
        comment_max_age=args.comment_max_age,
        comment_max_attempts=args.comment_max_attempts,
        page_limit=args.page_limit,
        backfill=not args.no_backfill,
        progress=not args.no_progress,
    )


async def run(config: Config) -> None:
    ensure_dir(config.output_dir)
    state = load_state(config.output_dir)
    headers = build_headers(config.api_key)
    timeout = httpx.Timeout(30.0, connect=10.0)
    async with httpx.AsyncClient(headers=headers, timeout=timeout) as client:
        if config.backfill:
            await backfill_posts(client, config, state)
        await live_loop(client, config, state)


def main() -> int:
    config = parse_args()
    try:
        asyncio.run(run(config))
    except KeyboardInterrupt:
        return 130
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
