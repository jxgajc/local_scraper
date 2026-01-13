# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Run Commands

```bash
# Install dependencies
pip install -r requirements.txt
playwright install chromium

# Run all spiders
python run.py

# Run specific spider (use spider name from SPIDER_MAP in run.py)
python run.py fujian_drug_store

# Debug mode (verbose logging)
python run.py debug
python run.py fujian_drug_store debug
```

## Database Configuration

Set via environment variable or modify `hybrid_crawler/settings.py`:
```bash
export DATABASE_URL="mysql+pymysql://user:pass@host:3306/dbname"
```

## Architecture

Hybrid Scrapy framework with dual-mode spider architecture for scraping Chinese regional medical insurance drug databases.

**Two Base Spider Classes** (`hybrid_crawler/spiders/base_spiders.py`):
- `BaseRequestSpider` - HTTP-only, high concurrency (32 requests). Implement `parse_logic(response)`.
- `BasePlaywrightSpider` - Browser-based, low concurrency (4 requests). Implement async `parse_logic(response, page)`.

**Smart Retry Strategy** (`hybrid_crawler/middlewares.py`):
- Network errors (ConnectionRefused, Timeout) → Exponential backoff
- Logic errors (ElementNotFound, BrowserCrash) → Clean Slate Retry (destroy context, clear cookies)

**Pipeline Flow** (`hybrid_crawler/pipelines.py`):
1. `DataCleaningPipeline` - Strip whitespace
2. `CrawlStatusPipeline` - Record progress to DB
3. `UniversalBatchWritePipeline` - Async batch writes (configurable via BUFFER_THRESHOLD/BUFFER_TIMEOUT_SEC), falls back to single-record on failure

**Item/Model Pattern**:
Each spider has a paired Item class and SQLAlchemy Model in `hybrid_crawler/models/`. Items must:
1. Call `generate_md5_id()` before yielding
2. Implement `get_model_class()` returning the SQLAlchemy model

**Status Reporting** (`hybrid_crawler/spiders/mixins.py`):
Inherit `SpiderStatusMixin` and use `report_list_page()`, `report_detail_page()`, `report_error()` to track progress in `CrawlStatus` and `SpiderProgress` tables.

**Recrawl Support**:
Spiders can define `recrawl_config` dict with `table_name` and `unique_id`, then implement `fetch_all_ids_from_api()` and `recrawl_by_ids()` classmethods for data gap recovery.

## Adding a New Spider

1. Create model file in `hybrid_crawler/models/` with Item class (with `generate_md5_id()` and `get_model_class()`) and SQLAlchemy Model
2. Create spider in `hybrid_crawler/spiders/` inheriting from `SpiderStatusMixin` and `scrapy.Spider` (or base classes)
3. Register spider in `SPIDER_MAP` dict in `run.py`

## Key Files

- `run.py` - Entry point, spider registry (SPIDER_MAP)
- `hybrid_crawler/spiders/base_spiders.py` - Core base classes
- `hybrid_crawler/middlewares.py` - Request routing and retry logic
- `hybrid_crawler/pipelines.py` - Async batch writing and degradation
- `hybrid_crawler/models/__init__.py` - Database engine and session factory
- `hybrid_crawler/exceptions.py` - Custom exception hierarchy (guides retry strategy)
- `hybrid_crawler/spiders/mixins.py` - SpiderStatusMixin for progress tracking

## Debug Tips

- See browser UI: Set `headless: False` in `settings.py` PLAYWRIGHT_LAUNCH_OPTIONS
- See SQL: Set `echo=True` in engine creation (`models/__init__.py`)
- Playwright crashes: Lower `CONCURRENT_REQUESTS` in settings
