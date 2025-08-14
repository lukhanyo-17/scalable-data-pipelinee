.PHONY: venv install run db down test
venv:
	python -m venv .venv
install:
	pip install -r requirements.txt
run:
	python -m pipeline.main --run-local
db:
	docker compose up -d
down:
	docker compose down
test:
	python -m pipeline.main --run-local --dry-run
