# yipit_work

This project (yipit_work) implements an end-to-end ETL and AI enrichment pipeline.

## Exported CVS file location

**$project_root/output/ai_articles_enriched.csv**

## Prerequisite

- [python 3.11]()

## Install

- create a virtual environment with  [python 3.11.]()  see example in **create_venv.sh**
- run 
  ```shellscript
  pip install -r requirements.txt
  ```
   see example in install_deps,s

## Run Tests:

- pytest

## Run workflow

- start the pipleline: ./run_all.sh
- python entry point: [pipline.py](http://pipline.py)
- to see usage run: PYTHONPATH=$PYTHONPATH:$(pwd) python src/[pipeline.py](http://pipeline.py) -h

## Code Usage and Testing

Each module (`<file>.py`) is accompanied by a corresponding test file (`test_<file>.py`), which serves both to validate functionality and to document the intended usage and behavior.

## Query Functions:

- Function to execute SQL queries on DuckDB

```python
def read(table_name: str = TABLE_NAME, *, database: str | Path | None = None) -> pd.DataFrame:
    """Read all rows from ``table_name``."""
    with duckdb_session(database) as conn:
        return conn.execute(f"SELECT * FROM {table_name}").fetchdf()


def sql(query: str, *, database: str | Path | None = None) -> pd.DataFrame:
    """Run a SQL query and return the result as a DataFrame."""
    with duckdb_session(database) as conn:
        return conn.execute(query).fetchdf()
```

- Function to perform vector similarity search

```python
def find_similar_articles(query_text, filtered_df, top_k=5)
```

- Export function to Csv with embeddings

```python
def export_with_top_similar_articles(sql_query, output_path):
```

