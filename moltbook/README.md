# Moltbook Archiver

Archives Moltbook posts and comments into Zstd-compressed NDJSON files.

## Usage

```bash
uv run python main.py /path/to/output
```

Optional flags:

```bash
uv run python main.py /path/to/output \
  --poll-seconds 20 \
  --comment-min-age 180 \
  --comment-max-age 600
```

If you have an API key, provide it via `MOLTBOOK_API_KEY` or `--api-key`.
Anonymous access works for the public feed.

## Output

- `posts-YYYYMMDD.ndjson.zst`
- `comments-YYYYMMDD.ndjson.zst`
- `state.json`

Each line is a JSON object. Comments are flattened from the post detail response
and include a `depth` field.

## Notes

- The script polls `/posts?sort=new` for new posts, then fetches each post’s
  comments after it has aged by `--comment-min-age` seconds (default 3 minutes).
- On startup, it backfills historical posts until it reaches the oldest post
  already recorded in `state.json`.
- Progress output is enabled by default; disable it with `--no-progress`.
