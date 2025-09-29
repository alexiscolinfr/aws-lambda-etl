# Legacy Pipelines

This folder contains **deprecated or replaced data pipeline code**.  
The scripts are kept here only for **reference and fallback** purposes — they should **not** be used for new development or active production runs.

## Purpose
- Preserve historical implementations of certain data pipelines.
- Provide reference logic while migrating to or maintaining newer versions.
- Document business rules that were once required but are no longer needed due to process or system changes.

## Rules for Using This Folder
1. **Do not add new pipelines here.**  
   All new code should go in the appropriate domain folder (`facts/`, `dimensions/`, etc.).
   
2. **Do not run legacy pipelines in production.**  
   They may not be maintained, optimized, or compatible with current schemas.

3. **Add a note when moving files here.**  
   Include in the file header or commit message:
   - Which new pipeline replaced it.
   - Why it was deprecated.
   - Date of deprecation.

## Current Legacy Files

### `fact_employee.py`

- **Data domain:** `facts/`
- **Deprecation date:** YYYY-MM-DD 
- **Reason:** Change of data source
- **Status:** No longer used
- **Replaced by:** `fact_employee_v2.py`