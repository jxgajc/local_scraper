# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Run Commands

```bash
# Install dependencies
pip install -r hybrid_crawler/requirements.txt
playwright install chromium

# Run all spiders
python hybrid_crawler/run.py

# Run specific spider
python hybrid_crawler/run.py fujian_drug_store

# Debug mode (verbose logging)
python hybrid_crawler/run.py debug
python hybrid_crawler/run.py fujian_drug_store debug

# Initialize database tables
python hybrid_crawler/init_test_tables.py

# Run Spider Studio web UI
streamlit run spider_studio_app.py
```

## Database Configuration

Set via environment variable or modify `hybrid_crawler/hybrid_crawler/settings.py`:
```bash
export DATABASE_URL="mysql+pymysql://user:pass@host:3306/dbname"
```

## Architecture

Hybrid Scrapy framework with dual-mode spider architecture for scraping Chinese regional medical insurance drug databases.

**Two Base Spider Classes** (`hybrid_crawler/hybrid_crawler/spiders/base_spiders.py`):
- `BaseRequestSpider` - HTTP-only, high concurrency (32 requests). Implement `parse_logic(response)`.
- `BasePlaywrightSpider` - Browser-based, low concurrency (4 requests). Implement async `parse_logic(response, page)`.

**Smart Retry Strategy** (`hybrid_crawler/hybrid_crawler/middlewares.py`):
- Network errors (ConnectionRefused, Timeout) → Exponential backoff
- Logic errors (ElementNotFound, BrowserCrash) → Clean Slate Retry (destroy context, clear cookies)

**Pipeline Flow** (`hybrid_crawler/hybrid_crawler/pipelines.py`):
1. `DataCleaningPipeline` - Strip whitespace
2. `CrawlStatusPipeline` - Record progress to DB
3. `UniversalBatchWritePipeline` - Async batch writes (500 items / 1.5s timeout), falls back to single-record on failure

**Item/Model Pattern**:
Each spider has a paired Item class (`items.py`) and SQLAlchemy Model (`models/`). Items must call `generate_md5_id()` before yielding and implement `get_model_class()`.

**Status Reporting** (`hybrid_crawler/hybrid_crawler/spiders/mixins.py`):
Use `report_list_page()`, `report_detail_page()`, `report_error()` to track progress in `CrawlStatus` and `SpiderProgress` tables.

## Key Files

- `hybrid_crawler/run.py` - Entry point for running spiders
- `hybrid_crawler/hybrid_crawler/spiders/base_spiders.py` - Core base classes
- `hybrid_crawler/hybrid_crawler/middlewares.py` - Request routing and retry logic
- `hybrid_crawler/hybrid_crawler/pipelines.py` - Async batch writing and degradation
- `hybrid_crawler/hybrid_crawler/models/__init__.py` - Database engine and session factory
- `hybrid_crawler/hybrid_crawler/exceptions.py` - Custom exception hierarchy (guides retry strategy)
- `spider_studio_app.py` - Streamlit web UI for spider code generation
- `hybrid_crawler/recrawl_checker.py` - Data recrawl validation utility
- `hybrid_crawler/hybrid_crawler/dashboard.py` - Web dashboard for monitoring

## Debug Tips

- See browser UI: Set `headless: False` in `settings.py` PLAYWRIGHT_LAUNCH_OPTIONS
- See SQL: Set `echo=True` in engine creation (`models/__init__.py`)
- Playwright crashes: Lower `CONCURRENT_REQUESTS` in settings
