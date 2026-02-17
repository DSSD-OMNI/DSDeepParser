# DSDeepParser

**Production-ready async data collection engine** for the DSSD (Deep Sports Data) analytics platform.

Collects FPL, Elo ratings, Understat xG, FCI player stats, and calendar/fixture data.  Runs an ETL pipeline to compute ML features and the **LRI (League Rating Index)** per FPL manager.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│  DSDeepParser (24/7 service on Railway)                      │
│                                                              │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌────────────┐  │
│  │ FPL API  │  │ ClubElo  │  │Understat │  │  Calendar   │  │
│  │ Source   │  │ Source   │  │ Source   │  │  Source     │  │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬───────┘  │
│       │              │              │              │          │
│       └──────────────┼──────────────┼──────────────┘          │
│                      ▼                                       │
│              ┌───────────────┐                                │
│              │  SQLite DB    │                                │
│              │  (WAL mode)   │                                │
│              └───────┬───────┘                                │
│                      │                                       │
│              ┌───────▼───────┐                                │
│              │  ML Feature   │                                │
│              │  Engine (ETL) │                                │
│              └───────┬───────┘                                │
│                      │                                       │
│              ┌───────▼───────┐    ┌──────────────┐           │
│              │  features +   │───►│  DSFPLBot    │           │
│              │  lri_scores   │    │  (Telegram)  │           │
│              └───────────────┘    └──────────────┘           │
│                                                              │
│  ┌────────────┐  ┌────────────────┐  ┌──────────────────┐   │
│  │ Prometheus │  │ TG Notifier    │  │ APScheduler      │   │
│  │ :8000      │  │ (alerts)       │  │ (cron triggers)  │   │
│  └────────────┘  └────────────────┘  └──────────────────┘   │
└─────────────────────────────────────────────────────────────┘
```

## Quick Start

### Local development

```bash
# Clone
git clone https://github.com/DSSD-OMNI/DSDeepParser.git
cd DSDeepParser

# Install deps
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# Configure
cp .env.example .env          # Edit with your tokens
# config.yaml is ready to use — adjust sources/schedules as needed

# Run
DB_PATH=./data/fpl_data.db python main.py
```

### Deploy to Railway

1. Push to GitHub (`main` branch).
2. CI runs tests → deploys automatically via GitHub Actions.
3. Set environment variables in Railway dashboard:
   - `DB_PATH=/app/data/fpl_data.db`
   - `TELEGRAM_BOT_TOKEN=...`
   - `TELEGRAM_CHAT_ID=...`
4. Attach a **persistent volume** mounted at `/app/data`.

---

## Data Sources

| Source | Schedule | Table(s) | Description |
|--------|----------|----------|-------------|
| **FPL Bootstrap** | Every 12 h | `bootstrap` | Teams, players, events |
| **FPL Leagues** | Every 30 min | `league_standings` | Classic league standings |
| **FPL Manager History** | Every 6 h | `manager_history` | Per-GW points, transfers |
| **Elo Ratings** | Daily 06:00 | `raw_team_elo` | ClubElo EPL team ratings |
| **Understat xG** | Daily 07:00 | `raw_understat_teams`, `raw_understat_matches` | Expected goals data |
| **FCI Stats** | Daily 08:00 | `raw_fci_stats` | Extended player stats (CSV) |
| **Calendar** | Weekly Mon | `raw_calendar`, `raw_calendar_fixtures` | Int'l breaks + fixtures |

## ML Features (ETL)

The `MLFeatureEngine` runs every 6 hours and computes these features per manager:

| Feature | Description |
|---------|-------------|
| `form_5gw` | Average points over last 5 gameweeks |
| `rank_percentile` | Rank position as 0–1 percentile |
| `total_points_norm` | Normalized total season points |
| `transfer_efficiency` | Points gained per transfer made |
| `captain_accuracy` | (Requires picks data) |
| `bench_utilization` | (Requires picks data) |
| `consistency_score` | Inverse of GW score standard deviation |
| `xg_team_trend` | Average league xG from Understat |
| `opponent_elo_norm` | Normalized opponent Elo strength |

**LRI (League Rating Index)** = weighted sum of all features, stored in `lri_scores` table.

---

## Adding a New Source

### Option A: Config-driven (HTTP → JSON/CSV → SQLite)

Add a block to `config.yaml`:

```yaml
- name: my_new_source
  type: fpl               # uses generic FPLSource (HTTP fetcher)
  enabled: true
  schedule: "0 */4 * * *" # cron expression
  fetcher:
    url: "https://api.example.com/data/{param}/"
    method: GET
    headers:
      Accept: application/json
    params:
      param: [value1, value2]
  parser:
    type: json
    extract: "$.data.items[*]"
  transformer:
    - operation: rename
      mapping:
        old_name: new_name
  storage:
    - table: my_table
      type: sqlite
      unique_columns: [id]
```

### Option B: Custom source class

1. Create `sources/my_source.py` inheriting from `DataSource`:

```python
from core.base import DataSource

class MySource(DataSource):
    async def fetch(self):
        ...
    async def parse(self, raw):
        ...
    async def transform(self, records):
        return records
    async def store(self, records):
        ...
```

2. Register in `main.py`:

```python
from sources.my_source import MySource
SOURCE_REGISTRY["my"] = MySource
```

3. Add to `config.yaml`:

```yaml
- name: my_custom
  type: my
  enabled: true
  schedule: "0 */6 * * *"
```

---

## Configuration

### Environment variables in config.yaml

Use `${VAR_NAME}` or `${VAR_NAME:default}`:

```yaml
notifications:
  telegram:
    token: "${TELEGRAM_BOT_TOKEN:YOUR_TOKEN}"
```

### Key config sections

| Section | Purpose |
|---------|---------|
| `global` | Logging level, rotation, timezone |
| `network` | Session pool, cache TTL, proxies, user agents |
| `rate_limiter` | Adaptive delay parameters |
| `notifications` | Telegram alerts on errors |
| `metrics` | Prometheus port |
| `default_storage` | SQLite DB path |
| `ml` | Feature recalculation interval |
| `sources` | Data source definitions |

---

## Monitoring

- **Prometheus** metrics at `http://localhost:8000/`
  - `fetcher_requests_total` — HTTP request count by source/status
  - `fetcher_errors_total` — Error count by source/type
  - `scheduler_tasks_completed_total` — Completed jobs
  - `scheduler_active_tasks` — Currently running tasks
- **Telegram** notifications on critical errors (when enabled)
- **Logs** — rotated daily, 7-day retention

---

## Testing

```bash
pip install pytest pytest-asyncio
pytest tests/ -v
```

## Project Structure

```
dsdeepparser/
├── main.py                 # Entry point
├── config.yaml             # All configuration
├── requirements.txt
├── Dockerfile
├── railway.json
├── core/
│   ├── base.py             # DataSource ABC
│   ├── fetcher.py          # HTTP fetcher (templates, pagination, cache)
│   ├── parser.py           # JSON / HTML / CSV parsers
│   ├── transformer.py      # Rename, compute, filter, flatten, etc.
│   ├── storage.py          # BaseStorage ABC + MultiStorage
│   ├── scheduler.py        # APScheduler wrapper
│   └── config.py           # Global config loader
├── sources/
│   ├── fpl.py              # Config-driven FPL source
│   ├── elo.py              # ClubElo scraper
│   ├── understat.py        # Understat xG scraper
│   ├── fci.py              # FCI CSV reader
│   └── calendar_source.py  # International breaks + fixtures
├── storage/
│   ├── sqlite_storage.py   # SQLite backend (WAL, upsert, auto-schema)
│   └── file_storage.py     # CSV/JSONL/Parquet exporter
├── ml/
│   ├── features.py         # ETL engine (feature calc + LRI)
│   └── export.py           # Scheduled data exports
├── tests/
│   └── test_core.py
├── rate_limiter.py          # Adaptive rate limiting
├── notifier.py              # Telegram alerts
├── metrics.py               # Prometheus metrics
├── circuit_breaker.py       # Fault tolerance
├── transformations.py       # Custom transform functions
└── utils.py                 # Config, logging, env helpers
```

## Key Bug Fixes (from original)

1. **league_standings empty table** — `JsonParser` now extracts from each paginated response individually before combining, instead of trying to navigate a list of full response dicts.
2. **URL template substitution** — `HttpFetcher._render_url()` replaces `{league_id}` etc. in the URL path before making requests, instead of sending them as query params.
3. **Empty first record** — `SQLiteStorage` filters out empty dicts before table creation.
4. **KeyError: 'table'** — Storage config validation ensures `table` key exists.
5. **Import errors** — All sources use absolute imports; no broken relative imports.
6. **Source routing** — `main.py` uses `SOURCE_REGISTRY` to instantiate the correct class per source type, instead of using `FPLSource` for everything.
