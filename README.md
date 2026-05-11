# SoundScape

Music content enrichment pipeline — ingests raw FMA metadata, enriches it with
Gemini LLM (mood, theme, energy tags), stores structured outputs in S3 Parquet,
and evaluates LLM labels against ground-truth Spotify/Echonest audio features.

Built to mirror Spotify's internal music understanding pipelines (Minesweeper squad).

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         SoundScape Pipeline                             │
└─────────────────────────────────────────────────────────────────────────┘

  FMA Dataset (.zip)
  tracks.csv (106K)  ──┐
  echonest.csv (13K) ──┤
  genres.csv         ──┘
          │
          ▼
  ┌───────────────────┐
  │  PySpark Ingest   │  local[2] mode (mirrors EMR job structure)
  │  ingestion/       │  - SHA1 verify zip
  │  ingest_fma.py    │  - inner join on track_id → 13,129 tracks
  └────────┬──────────┘  - clean, cast, drop dupes
           │ Parquet (snappy)
           ▼
  ┌─────────────────────────────────┐
  │  S3 Data Lake                   │
  │  s3://{bucket}/raw/tracks/      │  ← raw Parquet partitions
  └────────┬────────────────────────┘
           │ read via PySpark
           ▼
  ┌───────────────────────────────────────┐
  │  Gemini Enrichment                    │
  │  enrichment/enrich_tracks.py          │
  │  - batch 10 tracks → gemini-1.5-flash │
  │  - JSON mode: mood, theme, energy,    │
  │    danceability, content_summary      │
  │  - exponential backoff (15 RPM limit) │
  └────────┬──────────────────────────────┘
           │ Parquet (snappy)
           ▼
  ┌──────────────────────────────────────┐
  │  S3 Data Lake                        │
  │  s3://{bucket}/enriched/tracks/      │  ← enriched Parquet
  └────────┬─────────────────────────────┘
           │ DuckDB httpfs S3 scan
           ▼
  ┌──────────────────────────────────────────────────────┐
  │  DuckDB Analytics & Evaluation                       │
  │  evaluation/evaluate_enrichment.py                   │
  │  - LLM vs Spotify Features Alignment report          │
  │  - energy_level correlation vs echonest.energy       │
  │  - danceability_guess correlation vs echonest values │
  │  - distribution collapse detection                   │
  │  evaluation/query_enriched.py                        │
  │  - top mood_tags, genre dist, side-by-side samples   │
  └──────────────────────────────────────────────────────┘
```

## Key Narrative

`echonest.csv` contains Spotify/Echonest audio features (tempo, energy, valence,
danceability, acousticness, speechiness) for 13,129 FMA tracks. Echonest was
acquired by Spotify in 2014 — these are effectively Spotify's internal audio
features on a public dataset. The evaluation module compares LLM-generated labels
against these ground-truth features, producing a concrete alignment story.

## Stack

| Component | Tech | Why |
|-----------|------|-----|
| Batch compute | PySpark local[2] | Mirrors EMR job structure |
| Data lake | AWS S3 (free tier) | Parquet partitioned storage |
| LLM enrichment | Gemini 1.5 Flash | Free tier, native JSON mode |
| Analytics | DuckDB + httpfs | Serverless S3 queries, no Athena cost |
| Orchestration | Python scripts | Simple, no infra overhead |

## Setup

See [SETUP.md](SETUP.md) for AWS configuration.

```bash
cp .env.example .env
# fill in S3_BUCKET_NAME and GEMINI_API_KEY

pip install -r requirements.txt

# 1. ingest
python -m soundscape.ingestion.ingest_fma

# 2. enrich
python -m soundscape.enrichment.enrich_tracks

# 3. evaluate
python -m soundscape.evaluation.evaluate_enrichment

# 4. query
python -m soundscape.evaluation.query_enriched
```

## Tests

```bash
pytest soundscape/tests/
```
