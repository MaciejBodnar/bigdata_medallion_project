.PHONY: install run-sample run-full ci-smoke run-watcher run-consumer visualize-metrics run-user-test

install:
	python -m pip install --upgrade pip
	pip install -r requirements.txt

run-sample:
	python -m pipelines.taxi.main --sample --years 2025 --months 1 --services yellow green

run-full:
	python -m pipelines.taxi.main --years 2024 2025 --months 1 2 3 4 5 6 7 8 9 10 11 12 --services yellow green

ci-smoke:
	python -m pipelines.taxi.main --skip-download --sample --years 2025 --months 1 --services yellow green

run-watcher:
	python -m streaming.watcher

run-consumer:
	python -m streaming.consumer

visualize-metrics:
	python scripts/generate_quality_report.py

run-user-test:
	python scripts/user_data_product_test.py
