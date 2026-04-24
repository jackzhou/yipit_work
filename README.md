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

