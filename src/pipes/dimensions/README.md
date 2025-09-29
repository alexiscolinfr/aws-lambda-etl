# Dimension Script Naming and Execution Conventions

This directory contains multiple Python scripts (pipes) that generate all dimension tables. Below are the naming conventions, descriptions, and execution guidelines to ensure consistency and proper data refresh timing:

## 1. General Naming Rules:
- All file names are in **lowercase**.
- Words are separated by **underscores (_)** for better readability.
- File names should be **descriptive** and reflect the script's purpose or function.

## 2. File Types:
- `rpd_*.py` : Role-playing Dimension
- `scd_*.py` : Slowly Changing Dimension
- `sd_*.py` : Static Dimension

## 3. Dimension Types Explained:

- **Role-playing Dimension (`rpd_*.py`)**: A dimension that can play multiple roles in different contexts of the data model, usually linked to multiple facts in different ways (e.g., date dimension acting as order date and ship date).
  
- **Slowly Changing Dimension (`scd_*.py`)**: A dimension that tracks changes over time in its attributes (e.g., customer addresses or product details), either by overwriting old values or keeping historical data.
  
- **Static Dimension (`sd_*.py`)**: A dimension that remains relatively constant over time (e.g., geographical locations or product categories) and rarely changes.

## 4. Script Execution Frequency:
Depending on the dimension type:

- **Role-playing Dimensions (`rpd_*.py`)**: Run **daily** (except `rpd_dates.py`).
- **Slowly Changing Dimensions (`scd_*.py`)**: Run **daily**.
- **Static Dimensions (`sd_*.py`)**: Run **monthly** or **on demand**.

By adhering to these execution intervals, the dimension tables will stay synchronized with their respective data changes.
