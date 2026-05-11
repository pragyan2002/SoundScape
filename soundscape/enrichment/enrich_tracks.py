"""
OpenRouter enrichment pipeline.

Reads raw Parquet from S3, calls google/gemma-4-26b-a4b-it:free via OpenRouter
in batches of 30, joins enrichment back via pandas merge,
and writes enriched Parquet to S3 enriched/tracks/.
"""
import json
import os
import tempfile
import time
from pathlib import Path

import boto3
from openai import OpenAI
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import load_dotenv
from tqdm import tqdm

load_dotenv(override=True)

S3_BUCKET = os.environ["S3_BUCKET_NAME"]
AWS_PROFILE = os.environ.get("AWS_PROFILE", "default")
OPENROUTER_API_KEY = os.environ["OPENROUTER_API_KEY"]
RAW_PREFIX = "raw/tracks"
ENRICHED_PREFIX = "enriched/tracks"

BATCH_SIZE = 30
MAX_TRACKS = 500
SLEEP_BETWEEN_BATCHES = 12  # seconds; free-tier Meta/Together ~10 RPM = 6s min, 12s gives headroom
MAX_RETRIES = 7
BACKOFF_BASE = 15.0  # seconds; start longer for free-tier rate limits
BACKOFF_CAP = 120.0
MODEL = "openai/gpt-oss-120b:free"


PROMPT_TEMPLATE = """You are a music metadata enrichment system.

Given these tracks, return a JSON array where each element corresponds to one track (same order).
Each element must have exactly these fields:
- "mood_tags": array of 3-5 mood strings (e.g. "melancholic", "uplifting", "tense")
- "theme_tags": array of 3-5 theme strings (e.g. "nature", "urban", "introspection")
- "energy_level": one of "low", "medium", "high"
- "danceability_guess": one of "low", "medium", "high"
- "content_summary": one sentence describing the track

Tracks:
{tracks_json}

Return ONLY the JSON array. No explanation."""


def download_s3_prefix(bucket: str, prefix: str, local_dir: Path, profile: str) -> None:
    session = boto3.Session(profile_name=profile)
    s3 = session.client("s3")
    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
    keys = [obj["Key"] for page in pages for obj in page.get("Contents", []) if obj["Key"].endswith(".parquet")]
    if not keys:
        raise RuntimeError(f"No Parquet files found at s3://{bucket}/{prefix}/")
    print(f"Downloading {len(keys)} Parquet file(s) from s3://{bucket}/{prefix}/")
    for key in tqdm(keys, desc="Downloading raw Parquet"):
        dest = local_dir / Path(key).name
        s3.download_file(bucket, key, str(dest))


def upload_parquet_to_s3(local_dir: Path, bucket: str, prefix: str, profile: str) -> None:
    session = boto3.Session(profile_name=profile)
    s3 = session.client("s3")
    files = list(local_dir.rglob("*.parquet"))
    print(f"Uploading {len(files)} enriched Parquet file(s) to s3://{bucket}/{prefix}/")
    for f in tqdm(files, desc="Uploading to S3"):
        s3.upload_file(str(f), bucket, f"{prefix}/{f.name}")


def call_openrouter_with_backoff(client: OpenAI, prompt: str, retries: int = MAX_RETRIES) -> list[dict]:
    for attempt in range(retries):
        try:
            response = client.chat.completions.create(
                model=MODEL,
                messages=[{"role": "user", "content": prompt}],
            )
            if not response.choices or response.choices[0].message.content is None:
                raise ValueError("Empty/null response from model (free-tier overload)")
            text = response.choices[0].message.content
            text = text.strip()
            if text.startswith("```"):
                text = text.split("\n", 1)[-1].rsplit("```", 1)[0].strip()
            return json.loads(text)
        except Exception as e:
            err = str(e)
            err_lower = err.lower()
            if "429" in err or "quota" in err_lower or "rate" in err_lower or "empty/null" in err_lower:
                wait = min(BACKOFF_BASE * (2 ** attempt), BACKOFF_CAP)
                print(f"  Rate limited / empty response. Sleeping {wait:.0f}s (attempt {attempt+1}/{retries})")
                time.sleep(wait)
            else:
                raise
    raise RuntimeError(f"OpenRouter call failed after {retries} retries")


def build_prompt(batch: list[dict]) -> str:
    tracks_json = json.dumps(
        [
            {
                "title": t.get("title", ""),
                "artist": t.get("artist_name", ""),
                "genres": t.get("genres", ""),
                "tags": t.get("tags", ""),
            }
            for t in batch
        ],
        indent=2,
    )
    return PROMPT_TEMPLATE.format(tracks_json=tracks_json)


def enrich_batch(client: OpenAI, batch: list[dict]) -> list[dict]:
    prompt = build_prompt(batch)
    results = call_openrouter_with_backoff(client, prompt)

    # Defensive: handle length mismatch from model
    enriched = []
    for i, track in enumerate(batch):
        if i < len(results):
            r = results[i]
        else:
            r = {}
        enriched.append(
            {
                "track_id": track["track_id"],
                "mood_tags": r.get("mood_tags") or [],
                "theme_tags": r.get("theme_tags") or [],
                "energy_level": r.get("energy_level", ""),
                "danceability_guess": r.get("danceability_guess", ""),
                "content_summary": r.get("content_summary", ""),
            }
        )
    return enriched


def run() -> None:
    client = OpenAI(
        api_key=OPENROUTER_API_KEY,
        base_url="https://openrouter.ai/api/v1",
        max_retries=0,  # we handle retries ourselves
    )

    with tempfile.TemporaryDirectory() as tmp:
        raw_dir = Path(tmp) / "raw"
        raw_dir.mkdir()

        # --- 1. Download raw Parquet from S3 ---
        download_s3_prefix(S3_BUCKET, RAW_PREFIX, raw_dir, AWS_PROFILE)

        # --- 2. Load into pandas ---
        raw_table = pq.read_table(str(raw_dir))
        tracks_pd = raw_table.to_pandas()
        print(f"Loaded {len(tracks_pd):,} tracks from raw Parquet")

        # --- 3. Batch OpenRouter calls ---
        tracks_pd = tracks_pd.head(MAX_TRACKS)
        print(f"Capped to {len(tracks_pd):,} tracks (MAX_TRACKS={MAX_TRACKS})")
        records = tracks_pd[["track_id", "title", "artist_name", "genres", "tags"]].to_dict("records")
        batches = [records[i : i + BATCH_SIZE] for i in range(0, len(records), BATCH_SIZE)]

        print(f"Enriching {len(records):,} tracks in {len(batches):,} batches of {BATCH_SIZE}...")
        print(f"Estimated time: ~{len(batches) * SLEEP_BETWEEN_BATCHES // 60} min at {SLEEP_BETWEEN_BATCHES}s/batch\n")

        all_enrichments: list[dict] = []
        failed_batches = 0
        i = -1

        try:
            for i, batch in enumerate(tqdm(batches, desc="OpenRouter enrichment")):
                try:
                    enriched = enrich_batch(client, batch)
                    all_enrichments.extend(enriched)
                except KeyboardInterrupt:
                    raise
                except Exception as e:
                    print(f"  Batch {i} failed: {e}. Filling with empty values.")
                    failed_batches += 1
                    for t in batch:
                        all_enrichments.append(
                            {
                                "track_id": t["track_id"],
                                "mood_tags": [],
                                "theme_tags": [],
                                "energy_level": "",
                                "danceability_guess": "",
                                "content_summary": "",
                            }
                        )
                if i < len(batches) - 1:
                    time.sleep(SLEEP_BETWEEN_BATCHES)
        except KeyboardInterrupt:
            print(f"\nInterrupted at batch {i}. Saving {len(all_enrichments)} enriched tracks so far.")

        print(f"\nEnrichment done. Failed batches: {failed_batches}/{len(batches)}")

        # --- 4. Join enrichment back to original ---
        enrichment_pd = pd.DataFrame(all_enrichments)
        enriched_pd = tracks_pd.merge(enrichment_pd, on="track_id", how="left")
        print(f"Enriched DataFrame: {len(enriched_pd):,} rows")

        # --- 5. Write enriched Parquet locally, then upload ---
        out_dir = Path(tmp) / "enriched"
        out_dir.mkdir()
        table = pa.Table.from_pandas(enriched_pd, preserve_index=False)
        pq.write_to_dataset(table, root_path=str(out_dir), compression="snappy")
        upload_parquet_to_s3(out_dir, S3_BUCKET, ENRICHED_PREFIX, AWS_PROFILE)

    print("Enrichment pipeline complete.")


if __name__ == "__main__":
    run()
