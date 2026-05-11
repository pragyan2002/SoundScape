# SoundScape

Music content enrichment pipeline — ingests raw FMA metadata, enriches it with
an OpenRouter LLM (mood, theme, energy tags), stores structured outputs in S3 Parquet,
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
  ┌─────────────────────────────────────────────────┐
  │  OpenRouter Enrichment                          │
  │  enrichment/enrich_tracks.py                    │
  │  - pandas reads raw Parquet (no Spark needed)   │
  │  - batch 30 tracks → openai/gpt-oss-120b:free   │
  │  - JSON mode: mood, theme, energy,              │
  │    danceability, content_summary                │
  │  - exponential backoff (free-tier ~10 RPM)      │
  └────────┬────────────────────────────────────────┘
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
| LLM enrichment | OpenRouter (`gpt-oss-120b:free`) | Free tier, OpenAI-compatible API |
| Analytics | DuckDB + httpfs | Serverless S3 queries, no Athena cost |
| Orchestration | Python scripts | Simple, no infra overhead |

## Pipeline Results

Last run enriched **500 tracks** (`MAX_TRACKS` cap) via OpenRouter.

### Alignment (LLM labels vs Echonest ground truth)

| Dimension | Low | Medium | High | Check |
|-----------|-----|--------|------|-------|
| Energy (mean Echonest energy) | 0.400 | 0.484 | 0.584 | PASS ✓ |
| Danceability (mean Echonest danceability) | 0.415 | 0.407 | 0.447 | PASS ✓ |

Energy labels are monotonically ordered against Echonest ground truth. Danceability
passes the high > low check but medium sits slightly below low — weak signal, not a
failure.

### Distribution

| Label | Energy % | Danceability % |
|-------|----------|----------------|
| low | 36.7 | 46.4 |
| medium | 33.1 | 30.2 |
| high | 30.2 | 23.5 |

No distribution collapse (threshold: 70%). Danceability skews low-heavy.

### Coverage note

Evaluation reads the full enriched Parquet (26,758 rows). Only the 500 enriched tracks
carry LLM labels — the rest are unenriched rows from previous runs still present in S3.
Quality metrics (`pct_with_mood_tags` etc.) reflect this: ~1.9% = 500 / 26,758.

## Setup

See [SETUP.md](SETUP.md) for AWS configuration.

```bash
cp .env.example .env
# fill in S3_BUCKET_NAME and OPENROUTER_API_KEY

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
