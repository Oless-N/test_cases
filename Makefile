run:
	uvicorn app.main:app --host 0.0.0.0 --port 5789 --reload

run_docker:
	docker-compose down
	docker-compose -f docker-compose.yml up

install_dev:
	pip install -e .

