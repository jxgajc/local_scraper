I have identified the root causes for the task management issues and missing logs:

### 1. Task Management Issue ("Task state is messy")
The `dashboard.py` maintains `RECRAWL_TASKS` in memory, but there's a race condition or logic gap where tasks might finish or error out without properly cleaning up, or the frontend doesn't get the updated status quickly enough.

Specifically, the "One-Click Check & Recrawl" (`/api/recrawl/start-all`) logic is too simplistic:
- It iterates through spiders sequentially.
- It **does not register tasks in `RECRAWL_TASKS`**, so the "Stop" button doesn't know what to stop, and the frontend thinks nothing is running (or gets confused).
- It calls `recrawl_spider` directly, which logs to console/file but doesn't integrate well with the new `RECRAWL_TASKS` management.

**Fix**:
- Refactor `/api/recrawl/start-all` to register each sub-task into `RECRAWL_TASKS` as it runs.
- Ensure `RECRAWL_TASKS` is checked/cleaned up correctly.

### 2. Missing Logs ("Log file not created yet")
The `recrawl_checker.py` uses `logging.basicConfig` which sets up a root logger writing to `recrawl.log`. However, `dashboard.py` expects logs to be in `logs/{spider_name}.log` to display them in the monitor window.

When `recrawl_checker` runs inside the dashboard process (via `BackgroundTasks`), it writes to the main application log or `recrawl.log` in the root, **not** the individual spider log file that the frontend polls.

**Fix**:
- In `recrawl_checker.py`, update `BaseRecrawler` to configure a file handler specifically for `logs/{spider_name}.log`.
- This ensures that when a recrawl task runs, its output goes exactly where the dashboard looks for it.

### Execution Plan
1.  **Update `recrawl_checker.py`**:
    - Modify `__init__` to setup a file handler for `logs/{self.spider_name}.log`.
2.  **Update `dashboard.py`**:
    - Improve `/api/recrawl/start-all` to properly register tasks in `RECRAWL_TASKS`.
    - Ensure `recrawl_spider` calls within the loop respect the global task tracking.
