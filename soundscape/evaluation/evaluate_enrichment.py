"""
Evaluation module — LLM vs Spotify/Echonest features alignment.

Loads enriched Parquet from S3 via DuckDB httpfs, then checks:
- Correlation: do high energy_level tracks have higher mean echonest.energy?
- Correlation: do high danceability_guess tracks have higher mean echonest.danceability?
- Distribution collapse: warn if any single label > 70% of records
- Quality: % tracks with non-null mood_tags, % with 2+ theme_tags
"""
import os

import boto3
import duckdb
from dotenv import load_dotenv

load_dotenv(override=True)

S3_BUCKET = os.environ["S3_BUCKET_NAME"]
AWS_PROFILE = os.environ.get("AWS_PROFILE", "default")
ENRICHED_PREFIX = "enriched/tracks"


def get_aws_credentials(profile: str) -> dict:
    session = boto3.Session(profile_name=profile)
    creds = session.get_credentials().get_frozen_credentials()
    return {
        "access_key": creds.access_key,
        "secret_key": creds.secret_key,
        "token": creds.token,
    }


def build_conn(bucket: str, profile: str) -> duckdb.DuckDBPyConnection:
    creds = get_aws_credentials(profile)
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("SET s3_region='us-east-1';")
    con.execute(f"SET s3_access_key_id='{creds['access_key']}';")
    con.execute(f"SET s3_secret_access_key='{creds['secret_key']}';")
    if creds["token"]:
        con.execute(f"SET s3_session_token='{creds['token']}';")
    return con


def section(title: str) -> None:
    print(f"\n{'=' * 60}")
    print(f"  {title}")
    print('=' * 60)


def run() -> None:
    con = build_conn(S3_BUCKET, AWS_PROFILE)
    parquet_glob = f"s3://{S3_BUCKET}/{ENRICHED_PREFIX}/*.parquet"

    # Verify data loads
    total = con.execute(f"SELECT COUNT(*) FROM read_parquet('{parquet_glob}')").fetchone()[0]

    print("\n" + "=" * 60)
    print("  SoundScape Enrichment Evaluation Report")
    print("=" * 60)
    print(f"  Total enriched tracks: {total:,}")

    # ------------------------------------------------------------------ #
    section("LLM vs Spotify Features Alignment")
    # ------------------------------------------------------------------ #

    # Energy correlation
    energy_agg = con.execute(f"""
        SELECT
            energy_level,
            COUNT(*) AS n,
            ROUND(AVG(energy), 4) AS mean_echonest_energy,
            ROUND(AVG(valence), 4) AS mean_echonest_valence
        FROM read_parquet('{parquet_glob}')
        WHERE energy_level IN ('low', 'medium', 'high')
        GROUP BY energy_level
        ORDER BY mean_echonest_energy
    """).fetchdf()

    print("\n[Energy Level] LLM label vs mean Echonest energy score (0–1):")
    print(energy_agg.to_string(index=False))

    low_e = energy_agg.loc[energy_agg.energy_level == "low", "mean_echonest_energy"]
    high_e = energy_agg.loc[energy_agg.energy_level == "high", "mean_echonest_energy"]
    if not low_e.empty and not high_e.empty:
        aligned = float(high_e.iloc[0]) > float(low_e.iloc[0])
        print(f"\n  Alignment check (high > low): {'PASS ✓' if aligned else 'FAIL ✗'}")

    # Danceability correlation
    dance_agg = con.execute(f"""
        SELECT
            danceability_guess,
            COUNT(*) AS n,
            ROUND(AVG(danceability), 4) AS mean_echonest_danceability
        FROM read_parquet('{parquet_glob}')
        WHERE danceability_guess IN ('low', 'medium', 'high')
        GROUP BY danceability_guess
        ORDER BY mean_echonest_danceability
    """).fetchdf()

    print("\n[Danceability] LLM guess vs mean Echonest danceability score (0–1):")
    print(dance_agg.to_string(index=False))

    low_d = dance_agg.loc[dance_agg.danceability_guess == "low", "mean_echonest_danceability"]
    high_d = dance_agg.loc[dance_agg.danceability_guess == "high", "mean_echonest_danceability"]
    if not low_d.empty and not high_d.empty:
        aligned = float(high_d.iloc[0]) > float(low_d.iloc[0])
        print(f"\n  Alignment check (high > low): {'PASS ✓' if aligned else 'FAIL ✗'}")

    # ------------------------------------------------------------------ #
    section("Distribution Collapse Detection")
    # ------------------------------------------------------------------ #

    for col, label in [("energy_level", "Energy Level"), ("danceability_guess", "Danceability Guess")]:
        dist = con.execute(f"""
            SELECT
                {col} AS label,
                COUNT(*) AS n,
                ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct
            FROM read_parquet('{parquet_glob}')
            WHERE {col} != ''
            GROUP BY {col}
            ORDER BY n DESC
        """).fetchdf()

        print(f"\n[{label}] distribution:")
        print(dist.to_string(index=False))

        max_pct = dist["pct"].max() if not dist.empty else 0
        if max_pct > 70:
            print(f"  WARNING: dominant label at {max_pct:.1f}% — possible prompt collapse")
        else:
            print(f"  Distribution OK (max label: {max_pct:.1f}%)")

    # ------------------------------------------------------------------ #
    section("Enrichment Quality Checks")
    # ------------------------------------------------------------------ #

    quality = con.execute(f"""
        SELECT
            ROUND(100.0 * SUM(CASE WHEN mood_tags IS NOT NULL
                AND len(mood_tags) > 0 THEN 1 ELSE 0 END) / COUNT(*), 1)
                AS pct_with_mood_tags,
            ROUND(100.0 * SUM(CASE WHEN theme_tags IS NOT NULL
                AND len(theme_tags) >= 2 THEN 1 ELSE 0 END) / COUNT(*), 1)
                AS pct_with_2plus_theme_tags,
            ROUND(100.0 * SUM(CASE WHEN content_summary IS NOT NULL
                AND content_summary != '' THEN 1 ELSE 0 END) / COUNT(*), 1)
                AS pct_with_summary,
            ROUND(100.0 * SUM(CASE WHEN energy_level IN ('low','medium','high')
                THEN 1 ELSE 0 END) / COUNT(*), 1)
                AS pct_valid_energy_label
        FROM read_parquet('{parquet_glob}')
    """).fetchdf()

    print("\nQuality metrics:")
    for col in quality.columns:
        print(f"  {col}: {quality[col].iloc[0]}%")

    print("\n" + "=" * 60)
    print("  Evaluation complete.")
    print("=" * 60 + "\n")

    con.close()


if __name__ == "__main__":
    run()
