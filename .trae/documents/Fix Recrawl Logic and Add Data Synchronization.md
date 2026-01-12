I will implement the fix in three parts to ensure the `_test` table is correctly used as the staging area and data is synchronized safely.

### 1. Create Initialization Script (`init_test_tables.py`)
This script will ensure the `_test` tables are populated before you start checking/recrawling.
- **Logic**: Iterate through all spider models.
- **Check**: Query the `_test` table count.
- **Action**: If count is 0, execute SQL to copy all data from the production table (table name without `_test` suffix) to the `_test` table.
- **Safety**: Use `INSERT IGNORE` or checking column compatibility to prevent errors.

### 2. Modify `recrawl_checker.py`
I will fix the logic error where it was checking the wrong table, and add the synchronization logic.
- **Fix `get_existing_ids`**: Remove the code that strips `_test`. It will now correctly query the `_test` table to determine what is missing.
- **Add `sync_to_production` method**:
    - **Logic**: Execute an SQL statement to copy data from the `_test` table back to the production table.
    - **Deduplication**: Use `INSERT IGNORE INTO prod_table SELECT * FROM test_table` to ensure only new/non-duplicate records are added to the production environment.
- **Update `recrawl_spider`**: Call `sync_to_production()` automatically after a successful recrawl task.

### 3. Update Dashboard (Optional but Recommended)
- I can verify if you want this triggered via the UI, but for now, I will provide the script and update the backend logic as requested.

### Execution Steps:
1.  **Create** `init_test_tables.py` in `hybrid_crawler/`.
2.  **Edit** `hybrid_crawler/recrawl_checker.py` to fix the table name logic and add the sync function.
3.  **Run** `init_test_tables.py` to prepare your environment.
