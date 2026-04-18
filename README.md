# Plombery Scraper

A scheduled data pipeline platform built on [Plombery](https://github.com/luciano-fiandesiro/plombery) for scraping, enriching, and publishing structured data. It exposes a web UI for monitoring and triggering pipelines, and runs multiple independent scrapers on cron/interval schedules.

## Quick Start

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

Copy the config template and fill in your credentials:

```bash
cp src/config/config.ini.template src/config/config.ini
```

Run the server:

```bash
python src/app.py
```

The dashboard is available at **http://localhost:8080**.

## Pipelines

| Pipeline | File | Schedule | Description |
|---|---|---|---|
| **Jobs Scraper** | `src/jobs_scrape_pipeline.py` | Every 8 hours | Scrapes LinkedIn for Data Engineer / Data Architect roles in UAE, Saudi Arabia, and Qatar via `python-jobspy`. Enriches listings with Gemini AI (skills, company info, job metadata). Persists to PostgreSQL and Elasticsearch, exports JSON to Cloudflare R2. |
| **Jobs Alerts** | `src/jobs_alerts_pipeline.py` | Daily at 07:45 GST | Queries subscriber preferences and sends matching job alert emails via SMTP. |
| **Dimension Standardization** | `src/standardization_pipeline.py` | Every 24 hours | Uses Gemini AI to map raw inferred values (job titles, countries) to standardized reference entries in the database. |
| **Dubizzle Cars** | `src/dubzl_crs.py` | Daily at 05:45 GST | Scrapes used-car listings from Dubizzle UAE (paginated Next.js SSR). Stores in Elasticsearch and exports to R2. |
| **Carswitch Cars** | `src/crswth_crs.py` | Scheduled | Scrapes car listings from Carswitch. Parses streaming HTML payloads, enriches with detail-page data, indexes into Elasticsearch, and exports to R2. |
| **Allsopp Property** | `src/allsopp_crs.py` | Scheduled | Scrapes residential sales listings from allsoppandallsopp.com (Dubai). Stores in Elasticsearch and exports to R2. |
| **99acres Property** | `src/acres99_crs.py` | Scheduled | Scrapes property listings from 99acres (India). Stores in Elasticsearch and exports to R2. |

## Architecture

```
┌─────────────────────────────────────────────────┐
│              Plombery Web UI (:8080)             │
│          (FastAPI + Uvicorn + WebSocket)         │
└──────────────────────┬──────────────────────────┘
                       │
          ┌────────────┼────────────────┐
          ▼            ▼                ▼
   ┌────────────┐ ┌──────────┐  ┌──────────────┐
   │   Jobs     │ │  Cars    │  │  Property    │
   │ Pipelines  │ │ Pipelines│  │  Pipelines   │
   └─────┬──────┘ └────┬─────┘  └──────┬───────┘
         │              │               │
         ▼              ▼               ▼
   ┌───────────┐  ┌───────────────────────────┐
   │ Gemini AI │  │      Data Stores          │
   │ (2.0 /    │  │  PostgreSQL (Neon DB)     │
   │  1.5)     │  │  Elasticsearch 7.x        │
   └───────────┘  │  Cloudflare R2 (exports)  │
                  └───────────────────────────┘
```

### Data Flow (Jobs Pipeline)

1. **Scrape** - `python-jobspy` fetches LinkedIn job listings for configured locations.
2. **Deduplicate** - New jobs are filtered against existing hashes in PostgreSQL.
3. **Persist Raw** - Raw records are appended to PostgreSQL and bulk-indexed into Elasticsearch.
4. **AI Enrichment** - Gemini extracts structured fields (skills, seniority, company info) from raw descriptions.
5. **Persist Enriched** - Enriched records are saved to PostgreSQL and Elasticsearch.
6. **Export** - A public JSON snapshot is uploaded to Cloudflare R2 for downstream dashboards.
7. **Alert** - Subscribers receive matching job alerts via email.

## Configuration

All credentials are managed through `src/config/config.ini` (INI format). See `src/config/config.ini.template` for the full list of sections:

- **`[PostgresDB]`** - Neon PostgreSQL connection string
- **`[GeminiPro]`** - Google Gemini API keys
- **`[openrouter]`** - OpenRouter API key (optional)
- **`[elasticsearch]`** - Elasticsearch hosts, auth, index names
- **`[cloudflare]`** - Cloudflare R2 credentials and bucket config
- **`[Sendgrid]`** - SMTP credentials for alert emails
- **`[core]`** - Shared settings (base URLs, etc.)

## Project Structure

```
plombery-scraper/
├── src/
│   ├── app.py                    # Entry point - starts Uvicorn server
│   ├── jobs_scrape_pipeline.py   # Jobs scraping + AI enrichment pipeline
│   ├── jobs_alerts_pipeline.py   # Job alert email pipeline
│   ├── standardization_pipeline.py # Dimension standardization pipeline
│   ├── dubzl_crs.py              # Dubizzle car listings pipeline
│   ├── crswth_crs.py             # Carswitch car listings pipeline
│   ├── allsopp_crs.py            # Allsopp property listings pipeline
│   ├── acres99_crs.py            # 99acres property listings pipeline
│   ├── config/
│   │   ├── __init__.py           # Config reader (configparser)
│   │   ├── config.ini            # Local config (gitignored)
│   │   └── config.ini.template   # Template with placeholder values
│   ├── connections/
│   │   └── neondb_client.py      # Neon DB connection helper
│   └── utils/
│       ├── ai_infer.py           # Gemini API inference helper
│       └── rdbms_conn.py         # Database query executor
├── deploy/
│   └── plomberly.service         # systemd unit file for production
├── .github/workflows/
│   └── deploy.yml                # CI/CD - rsync to Oracle Cloud
├── requirements.txt
└── .gitignore
```

## Deployment

### Production (Oracle Cloud)

The GitHub Actions workflow (`.github/workflows/deploy.yml`) handles deployment:

1. Triggers on push/merge to `main` or `master`, releases, and manual dispatch.
2. Syncs files to the remote server via `rsync` over SSH.
3. Installs dependencies if `requirements.txt` changed.
4. The app runs as a systemd service (`plomberly.service`).

Required GitHub secrets: `SSH_HOST`, `SSH_USER`, `SSH_PORT`, `REMOTE_PATH`, `SSH_PRIVATE_KEY`.

The systemd service runs:

```
/home/ubuntu/plombery/venv/bin/python /home/ubuntu/plombery/src/app.py
```

To manage the service on the remote server:

```bash
sudo systemctl start plomberly
sudo systemctl stop plomberly
sudo systemctl restart plomberly
sudo systemctl status plomberly
```

### Local Development

```bash
source venv/bin/activate
python src/app.py
```

The server starts with hot-reload enabled (`--reload`), watching for changes in the parent directory.

## Dependencies

Key dependencies:

- **plombery** - Pipeline orchestration with web UI
- **python-jobspy** - LinkedIn / Indeed / Glassdoor job scraper
- **elasticsearch 7.x** - Search and indexing
- **sqlalchemy + psycopg2** - PostgreSQL access (Neon DB)
- **boto3** - Cloudflare R2 (S3-compatible) uploads
- **requests-html** - JavaScript-rendered page loading
- **curl-cffi** - TLS-fingerprint-resistant HTTP client
- **lxml[html_clean]** - Fast HTML parsing
- **google-gemini** (via REST) - LLM-based data extraction
- **json-repair / dirtyjson / python-rapidjson** - Robust JSON parsing for LLM outputs
