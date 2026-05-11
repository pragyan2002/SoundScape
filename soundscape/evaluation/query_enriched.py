"""
Analytics layer — DuckDB queries directly against S3 enriched Parquet.

Outputs:
  1. Top 10 most common mood_tags
  2. Avg Echonest energy grouped by LLM energy_level
  3. Genre distribution in enriched set
  4. 5 sample tracks with full enrichment + Echonest features side-by-side
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


def divider(title: str) -> None:
    print(f"\n{'─' * 60}")
    print(f"  {title}")
    print('─' * 60)


def run() -> None:
    con = build_conn(S3_BUCKET, AWS_PROFILE)
    src = f"s3://{S3_BUCKET}/{ENRICHED_PREFIX}/*.parquet"

    # ------------------------------------------------------------------ #
    divider("1. Top 10 Most Common Mood Tags")
    # ------------------------------------------------------------------ #
    top_moods = con.execute(f"""
        SELECT
            mood_tag,
            COUNT(*) AS track_count
        FROM (
            SELECT UNNEST(mood_tags) AS mood_tag
            FROM read_parquet('{src}')
            WHERE mood_tags IS NOT NULL
        )
        GROUP BY mood_tag
        ORDER BY track_count DESC
        LIMIT 10
    """).fetchdf()
    print(top_moods.to_string(index=False))

    # ------------------------------------------------------------------ #
    divider("2. Avg Echonest Energy by LLM Energy Level")
    # ------------------------------------------------------------------ #
    energy_table = con.execute(f"""
        SELECT
            energy_level,
            COUNT(*) AS n,
            ROUND(AVG(energy), 4)       AS avg_energy,
            ROUND(AVG(valence), 4)      AS avg_valence,
            ROUND(AVG(tempo), 2)        AS avg_tempo
        FROM read_parquet('{src}')
        WHERE energy_level IN ('low', 'medium', 'high')
        GROUP BY energy_level
        ORDER BY avg_energy
    """).fetchdf()
    print(energy_table.to_string(index=False))

    # ------------------------------------------------------------------ #
    divider("3. Genre Distribution (top 15)")
    # ------------------------------------------------------------------ #
    genre_dist = con.execute(f"""
        SELECT
            genre,
            COUNT(*) AS track_count,
            ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
        FROM (
            SELECT TRIM(UNNEST(STRING_SPLIT(
                REGEXP_REPLACE(genres, '[\\[\\]'']', '', 'g'), ','
            ))) AS genre
            FROM read_parquet('{src}')
            WHERE genres IS NOT NULL AND genres != 'nan'
        )
        WHERE genre != ''
        GROUP BY genre
        ORDER BY track_count DESC
        LIMIT 15
    """).fetchdf()
    print(genre_dist.to_string(index=False))

    # ------------------------------------------------------------------ #
    divider("4. Sample Tracks — LLM Enrichment + Echonest Side-by-Side")
    # ------------------------------------------------------------------ #
    samples = con.execute(f"""
        SELECT
            track_id,
            title,
            artist_name,
            -- LLM outputs
            mood_tags,
            theme_tags,
            energy_level,
            danceability_guess,
            content_summary,
            -- Echonest ground truth
            ROUND(energy, 3)       AS echonest_energy,
            ROUND(danceability, 3) AS echonest_danceability,
            ROUND(valence, 3)      AS echonest_valence,
            ROUND(tempo, 1)        AS echonest_tempo
        FROM read_parquet('{src}')
        WHERE energy_level != ''
          AND mood_tags IS NOT NULL
        LIMIT 5
    """).fetchdf()

    for _, row in samples.iterrows():
        print(f"\n  Track {int(row.track_id)}: {row.title} — {row.artist_name}")
        print(f"    LLM  | energy={row.energy_level}, dance={row.danceability_guess}")
        print(f"    LLM  | moods={row.mood_tags}")
        print(f"    LLM  | themes={row.theme_tags}")
        print(f"    LLM  | \"{row.content_summary}\"")
        print(f"    Spot | energy={row.echonest_energy}, dance={row.echonest_danceability}, "
              f"valence={row.echonest_valence}, tempo={row.echonest_tempo}")

    print(f"\n{'─' * 60}\n")
    con.close()


if __name__ == "__main__":
    run()
