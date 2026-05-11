"""
FMA metadata ingestion pipeline.

Downloads fma_metadata.zip, verifies SHA1, inner-joins tracks + echonest
on track_id (13,129 tracks), and writes Parquet to S3 raw/tracks/.

PySpark runs local[2] — mirrors an EMR job structure.
S3 upload uses boto3 (avoids hadoop-aws jar dependency on local mode).
"""
import hashlib
import os
import shutil
import tempfile
import zipfile
from pathlib import Path

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType
from tqdm import tqdm

load_dotenv(override=True)

FMA_URL = "https://os.unil.cloud.switch.ch/fma/fma_metadata.zip"
FMA_SHA1 = "f0df49ffe5f2a6008d7dc83c6915b31835dfe733"
S3_BUCKET = os.environ["S3_BUCKET_NAME"]
AWS_PROFILE = os.environ.get("AWS_PROFILE", "default")
S3_KEY_PREFIX = "raw/tracks"
DATA_DIR = Path("data/fma")


def download_with_progress(url: str, dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    with requests.get(url, stream=True, timeout=60) as r:
        r.raise_for_status()
        total = int(r.headers.get("content-length", 0))
        with open(dest, "wb") as f, tqdm(
            desc="Downloading fma_metadata.zip",
            total=total,
            unit="B",
            unit_scale=True,
            unit_divisor=1024,
        ) as bar:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
                bar.update(len(chunk))


def verify_sha1(path: Path, expected: str) -> None:
    sha1 = hashlib.sha1()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            sha1.update(chunk)
    actual = sha1.hexdigest()
    if actual != expected:
        raise ValueError(f"SHA1 mismatch: expected {expected}, got {actual}")
    print(f"SHA1 verified: {actual}")


def extract_zip(zip_path: Path, dest_dir: Path) -> None:
    dest_dir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path, "r") as z:
        members = [m for m in z.namelist() if Path(m).name in ("tracks.csv", "echonest.csv", "genres.csv")]
        for member in tqdm(members, desc="Extracting"):
            z.extract(member, dest_dir)
    print(f"Extracted to {dest_dir}")


def _find_csv(data_dir: Path, name: str) -> Path:
    matches = list(data_dir.rglob(name))
    if not matches:
        raise FileNotFoundError(f"{name} not found under {data_dir}")
    return matches[0]


def load_tracks(data_dir: Path) -> pd.DataFrame:
    path = _find_csv(data_dir, "tracks.csv")
    raw = pd.read_csv(path, index_col=0, header=[0, 1])
    df = pd.DataFrame({
        "track_id": raw.index.astype(int).values,
        "title": raw[("track", "title")].astype(str).values,
        "artist_name": raw[("artist", "name")].astype(str).values,
        "genres": raw[("track", "genres")].astype(str).values,
        "tags": raw[("track", "tags")].astype(str).values,
        "listens": pd.to_numeric(raw[("track", "listens")], errors="coerce").values,
        "duration": pd.to_numeric(raw[("track", "duration")], errors="coerce").values,
    })
    return df


def load_echonest(data_dir: Path) -> pd.DataFrame:
    path = _find_csv(data_dir, "echonest.csv")
    raw = pd.read_csv(path, index_col=0, header=[0, 1, 2])
    af = raw["echonest"]["audio_features"]
    df = pd.DataFrame({
        "track_id": raw.index.astype(int).values,
        "tempo": pd.to_numeric(af["tempo"], errors="coerce").values,
        "energy": pd.to_numeric(af["energy"], errors="coerce").values,
        "valence": pd.to_numeric(af["valence"], errors="coerce").values,
        "danceability": pd.to_numeric(af["danceability"], errors="coerce").values,
        "acousticness": pd.to_numeric(af["acousticness"], errors="coerce").values,
        "speechiness": pd.to_numeric(af["speechiness"], errors="coerce").values,
    })
    return df


SPARK_SCHEMA = StructType([
    StructField("track_id", LongType(), False),
    StructField("title", StringType(), True),
    StructField("artist_name", StringType(), True),
    StructField("genres", StringType(), True),
    StructField("tags", StringType(), True),
    StructField("listens", LongType(), True),
    StructField("duration", DoubleType(), True),
    StructField("tempo", DoubleType(), True),
    StructField("energy", DoubleType(), True),
    StructField("valence", DoubleType(), True),
    StructField("danceability", DoubleType(), True),
    StructField("acousticness", DoubleType(), True),
    StructField("speechiness", DoubleType(), True),
])


def build_spark_df(spark: SparkSession, merged: pd.DataFrame):
    # Cast to correct types before handing to Spark
    merged = merged.astype({
        "track_id": "int64",
        "listens": "float64",
        "duration": "float64",
    })
    merged["listens"] = merged["listens"].fillna(0).astype("int64")
    merged["track_id"] = merged["track_id"].astype("int64")

    sdf = spark.createDataFrame(merged, schema=SPARK_SCHEMA)
    sdf = (
        sdf
        .dropDuplicates(["track_id"])
        .filter(F.col("track_id").isNotNull())
        .filter(F.col("energy").isNotNull())   # echonest features must exist
        .filter(F.col("danceability").isNotNull())
    )
    return sdf


def upload_parquet_to_s3(local_dir: Path, bucket: str, prefix: str, profile: str) -> None:
    session = boto3.Session(profile_name=profile)
    s3 = session.client("s3")

    parquet_files = list(local_dir.glob("*.parquet"))
    if not parquet_files:
        # Spark writes part files
        parquet_files = list(local_dir.rglob("*.parquet"))

    print(f"Uploading {len(parquet_files)} Parquet file(s) to s3://{bucket}/{prefix}/")
    for pf in tqdm(parquet_files, desc="Uploading to S3"):
        key = f"{prefix}/{pf.name}"
        s3.upload_file(str(pf), bucket, key)
    print("Upload complete.")


def run() -> None:
    zip_path = DATA_DIR / "fma_metadata.zip"
    extract_dir = DATA_DIR / "extracted"

    # --- 1. Download ---
    if not zip_path.exists():
        download_with_progress(FMA_URL, zip_path)
    else:
        print(f"Zip already exists at {zip_path}, skipping download.")

    # --- 2. Verify ---
    verify_sha1(zip_path, FMA_SHA1)

    # --- 3. Extract ---
    if not (extract_dir / "fma_metadata").exists():
        extract_zip(zip_path, extract_dir)
    else:
        print("Already extracted, skipping.")

    # --- 4. Load + join with pandas ---
    print("Loading tracks.csv ...")
    tracks_df = load_tracks(extract_dir)
    print(f"  tracks: {len(tracks_df):,} rows")

    print("Loading echonest.csv ...")
    echonest_df = load_echonest(extract_dir)
    print(f"  echonest: {len(echonest_df):,} rows")

    merged = tracks_df.merge(echonest_df, on="track_id", how="inner")
    print(f"  inner join → {len(merged):,} tracks")

    # --- 5. PySpark transform ---
    spark = (
        SparkSession.builder
        .appName("soundscape-ingest-fma")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    sdf = build_spark_df(spark, merged)
    print(f"  after Spark dedup/nulls: {sdf.count():,} tracks")
    sdf.printSchema()

    # --- 6. Convert back to pandas → write Parquet via pyarrow → upload ---
    # PySpark's file writer hits a Java 17 security manager issue on Windows.
    # Spark is still used for all transforms above (mirrors EMR job structure).
    print("Converting Spark DataFrame to pandas for Parquet write...")
    final_pd = sdf.toPandas()
    spark.stop()

    with tempfile.TemporaryDirectory() as tmp:
        out_path = Path(tmp) / "tracks"
        out_path.mkdir()
        table = pa.Table.from_pandas(final_pd, preserve_index=False)
        pq.write_to_dataset(
            table,
            root_path=str(out_path),
            compression="snappy",
        )
        upload_parquet_to_s3(out_path, S3_BUCKET, S3_KEY_PREFIX, AWS_PROFILE)

    print("Ingestion complete.")


if __name__ == "__main__":
    run()
