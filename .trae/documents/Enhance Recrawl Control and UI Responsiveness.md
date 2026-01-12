I will enhance the backend and frontend to support stopping recrawl tasks and improve UI responsiveness.

### 1. Backend Enhancements (`dashboard.py` & `recrawl_checker.py`)
- **Add Task Cancellation Mechanism**:
    - Introduce a global `RECRAWL_TASKS` dictionary to track running recrawl tasks.
    - Implement a `stop_flag` or cancellation token in `BaseRecrawler` that is checked during loops (e.g., in `fetch_api_total`).
- **New API Endpoint**:
    - `POST /api/recrawl/stop`: Accepts a spider name (or "all") to signal the running task to abort.
- **Optimize `check_single_recrawl`**:
    - Currently, it runs synchronously (`await crawler.find_missing()`) which blocks the main thread and causes the "page freeze" issue.
    - I will refactor this to run in a `ThreadPoolExecutor` or `BackgroundTasks` properly, returning an immediate "Checking started" response, and letting the frontend poll for status via the existing monitor API.

### 2. Frontend Enhancements (`index.html`)
- **Add "Stop" Button for Recrawl**:
    - In the monitor modal, add a "Stop Recrawl/Check" button that calls the new stop API.
- **Improve UI Feedback**:
    - When "Check" or "Recrawl" is clicked, immediately disable buttons and show a loading state.
    - Poll the status more gracefully to avoid UI freezing.

### Execution Plan:
1.  **Modify `recrawl_checker.py`**: Add `stop_requested` flag and check it in loops.
2.  **Update `dashboard.py`**:
    - Add `RECRAWL_TASKS` management.
    - Implement `/api/recrawl/stop`.
    - Refactor `/api/recrawl/check/{spider_name}` to be non-blocking.
3.  **Update `index.html`**: Add the stop button and connect it to the API.
