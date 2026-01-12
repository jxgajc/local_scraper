I have identified the issues causing the "Loading..." logs and the Stop button confusion.

### 1. Fix "Logs not connecting" (Loading...)
The `recrawl_checker.py` file currently defines `SessionLocal` twice:
1.  **Line 43**: `SessionLocal = sessionmaker(...)` using a local `create_engine` call with `DATABASE_URL`.
2.  **Line 52**: `from hybrid_crawler.models import SessionLocal` which imports it from the project models.

This ambiguity is dangerous. More importantly, if `dashboard.py` imports `recrawl_checker`, and `recrawl_checker` tries to import `hybrid_crawler.models`, it might fail if `hybrid_crawler` package is not in the path correctly when running from different contexts.

**Fix**:
- Remove the local `create_engine` and `SessionLocal` definition in `recrawl_checker.py`.
- Rely entirely on `from hybrid_crawler.models import SessionLocal`.
- Ensure `sys.path` is correctly set up at the very top of `recrawl_checker.py` to allow this import, regardless of where it's run from.

Additionally, I will update `index.html` to handle the "Loading..." state better. If the fetch fails, it should show an error message instead of hanging on "Loading...".

### 2. Fix "Where is the Stop button?"
The Stop button I implemented only appears when a task is *active*. If the user is confused, it's better UX to show the button *disabled* when inactive, so they know the functionality exists.

**Fix**:
- Modify `index.html` to always show the Stop button.
- If no task is running, the button will be disabled and grayed out.
- If a task is running, it becomes active (red/pulse).

### Execution Plan
1.  **Refactor `recrawl_checker.py`**: Remove duplicate DB connection code and ensure robust imports.
2.  **Update `index.html`**:
    - Improve Stop button visibility (Always visible, disabled when idle).
    - Improve Log viewer (Show error if fetch fails).
