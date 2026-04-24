#!/usr/bin/env bash
source .venv/bin/activate

export PYTHONPATH=$PYTHONPATH:$(pwd)

MODE=${1:-non}

if [[ "$MODE" == "etl" ]]; then
    rm -rf data/processed/processed_news.csv
    python src/pipeline.py etl
elif [[ "$MODE" == "ai" ]]; then
    echo "Running AI pipeline"
    rm -rf data/db/tech_news.duckdb
    rm -rf output/final_articles.csv
    python src/pipeline.py ai
else
    rm -rf data/processed/processed_news.csv
    rm -rf data/db/tech_news.duckdb
    rm -rf output/final_articles.csv
    python src/pipeline.py all
fi