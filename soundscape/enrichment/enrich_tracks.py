"""
Gemini enrichment pipeline.

Reads raw Parquet from S3, calls gemini-1.5-flash in batches of 30
with native JSON mode, joins enrichment back to the Spark DataFrame,
and writes enriched Parquet to S3 enriched/tracks/.

Rate limit: Gemini free tier = 15 RPM. Pipeline sleeps 5s between
batches (30 tracks/call) → ~4 RPM, well within limit.
At 438 batches for 13K tracks, expect ~37 min total runtime.
"""
import json
import os
import tempfile
import time
from pathlib import Path

import boto3
from google import genai
from google.genai import types
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, LongType, StringType, StructField, StructType
from tqdm import tqdm

load_dotenv(override=True)

S3_BUCKET = os.environ["S3_BUCKET_NAME"]
AWS_PROFILE = os.environ.get("AWS_PROFILE", "default")
GEMINI_API_KEY = os.environ["GEMINI_API_KEY"]
RAW_PREFIX = "raw/tracks"
ENRICHED_PREFIX = "enriched/tracks"

BATCH_SIZE = 30
SLEEP_BETWEEN_BATCHES = 5  # seconds — keeps us ~4 RPM vs 15 RPM limit
MAX_RETRIES = 5

ENRICHMENT_SCHEMA = StructType([
    StructField("track_id", LongType(), False),
    StructField("mood_tags", ArrayType(StringType()), True),
    StructField("theme_tags", ArrayType(StringType()), True),
    StructField("energy_level", StringType(), True),
    StructField("danceability_guess", StringType(), True),
    StructField("content_summary", StringType(), True),
])

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


def call_gemini_with_backoff(client: genai.Client, prompt: str, retries: int = MAX_RETRIES) -> list[dict]:
    delay = 2.0
    for attempt in range(retries):
        try:
            response = client.models.generate_content(
                model="gemini-1.5-flash",
                contents=prompt,
                config=types.GenerateContentConfig(
                    response_mime_type="application/json",
                ),
            )
            return json.loads(response.text)
        except Exception as e:
            err = str(e).lower()
            if "429" in err or "quota" in err or "rate" in err:
                wait = delay * (2 ** attempt)
                print(f"  Rate limited. Sleeping {wait:.1f}s (attempt {attempt+1}/{retries})")
                time.sleep(wait)
            else:
                raise
    raise RuntimeError(f"Gemini call failed after {retries} retries")


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


def enrich_batch(client: genai.Client, batch: list[dict]) -> list[dict]:
    prompt = build_prompt(batch)
    results = call_gemini_with_backoff(client, prompt)

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
    client = genai.Client(api_key=GEMINI_API_KEY)

    spark = (
        SparkSession.builder
        .appName("soundscape-enrich-tracks")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    with tempfile.TemporaryDirectory() as tmp:
        raw_dir = Path(tmp) / "raw"
        raw_dir.mkdir()

        # --- 1. Download raw Parquet from S3 ---
        download_s3_prefix(S3_BUCKET, RAW_PREFIX, raw_dir, AWS_PROFILE)

        # --- 2. Load into Spark ---
        sdf = spark.read.parquet(str(raw_dir))
        print(f"Loaded {sdf.count():,} tracks from raw Parquet")

        # --- 3. Convert to pandas for batch Gemini calls ---
        tracks_pd = sdf.select(
            "track_id", "title", "artist_name", "genres", "tags"
        ).toPandas()

        records = tracks_pd.to_dict("records")
        batches = [records[i : i + BATCH_SIZE] for i in range(0, len(records), BATCH_SIZE)]

        print(f"Enriching {len(records):,} tracks in {len(batches):,} batches of {BATCH_SIZE}...")
        print(f"Estimated time: ~{len(batches) * SLEEP_BETWEEN_BATCHES // 60} min at {SLEEP_BETWEEN_BATCHES}s/batch\n")

        all_enrichments: list[dict] = []
        failed_batches = 0

        for i, batch in enumerate(tqdm(batches, desc="Gemini enrichment")):
            try:
                enriched = enrich_batch(client, batch)
                all_enrichments.extend(enriched)
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

        print(f"\nEnrichment done. Failed batches: {failed_batches}/{len(batches)}")

        # --- 4. Convert enrichment results to Spark DataFrame ---
        enrichment_pd = pd.DataFrame(all_enrichments)
        enrichment_sdf = spark.createDataFrame(enrichment_pd)

        # --- 5. Join back to original Spark DataFrame ---
        enriched_sdf = sdf.join(enrichment_sdf, on="track_id", how="left")
        print(f"Enriched DataFrame: {enriched_sdf.count():,} rows")
        enriched_sdf.printSchema()

        # Convert to pandas → pyarrow write (Java 17 breaks PySpark file writer on Windows)
        enriched_pd = enriched_sdf.toPandas()
        spark.stop()

        # --- 6. Write enriched Parquet locally, then upload ---
        out_dir = Path(tmp) / "enriched"
        out_dir.mkdir()
        table = pa.Table.from_pandas(enriched_pd, preserve_index=False)
        pq.write_to_dataset(table, root_path=str(out_dir), compression="snappy")
        upload_parquet_to_s3(out_dir, S3_BUCKET, ENRICHED_PREFIX, AWS_PROFILE)

    print("Enrichment pipeline complete.")


if __name__ == "__main__":
    run()
