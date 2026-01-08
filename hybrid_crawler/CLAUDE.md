# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Run Commands

```bash
# Install dependencies
pip install -r requirements.txt
playwright install chromium

# Run all spiders
python run.py

# Run specific spider
python run.py fujian_drug_store

# Debug mode (verbose logging)
python run.py debug
python run.py fujian_drug_store debug
```

## Database Configuration

Set via environment variable or modify `settings.py`:
```bash
export DATABASE_URL="mysql+pymysql://user:pass@host:3306/dbname"
```

## Architecture

This is a hybrid Scrapy framework with dual-mode spider architecture:

**Two Base Spider Classes:**
- `BaseRequestSpider` - HTTP-only, high concurrency (32 requests). Implement `parse_logic(response)`.
- `BasePlaywrightSpider` - Browser-based, low concurrency (4 requests). Implement async `parse_logic(response, page)`.

**Smart Retry Strategy (SmartRetryMiddleware):**
- Network errors (ConnectionRefused, Timeout) → Exponential backoff
- Logic errors (ElementNotFound, BrowserCrash) → Clean Slate Retry (destroy context, clear cookies)

**Pipeline Flow:**
1. `DataCleaningPipeline` - Strip whitespace
2. `CrawlStatusPipeline` - Record progress to DB
3. `UniversalBatchWritePipeline` - Async batch writes (500 items / 1.5s timeout), falls back to single-record on failure

**Item/Model Pattern:**
Each spider has a paired Item class and SQLAlchemy Model. Items must call `generate_md5_id()` before yielding and implement `get_model_class()`.

**Status Reporting (SpiderStatusMixin):**
Use `report_list_page()`, `report_detail_page()`, `report_error()` to track progress in `CrawlStatus` and `SpiderProgress` tables.

## Key Files

- `spiders/base_spiders.py` - Core base classes
- `middlewares.py` - Request routing and retry logic
- `pipelines.py` - Async batch writing and degradation
- `models/__init__.py` - Database engine and session factory
- `exceptions.py` - Custom exception hierarchy (guides retry strategy)

## Debug Tips

- See browser UI: Set `headless: False` in `settings.py` PLAYWRIGHT_LAUNCH_OPTIONS
- See SQL: Set `echo=True` in engine creation (`models/__init__.py`)
- Playwright crashes: Lower `CONCURRENT_REQUESTS` in settings
