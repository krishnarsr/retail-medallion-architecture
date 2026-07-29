.PHONY: setup generate bronze silver gold run test lint clean

setup:
	python -m pip install -r requirements.txt

generate:
	python -m src.pipeline generate

bronze:
	python -m src.pipeline bronze

silver:
	python -m src.pipeline silver

gold:
	python -m src.pipeline gold

run:
	python -m src.pipeline run-all

test:
	pytest -q

lint:
	ruff check src tests

clean:
	python scripts/clean_generated.py
