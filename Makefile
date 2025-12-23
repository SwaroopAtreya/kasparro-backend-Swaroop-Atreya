.PHONY: up down test migrate

up:
	docker-compose up --build -d

down:
	docker-compose down

test:
	docker-compose run --rm api pytest app/tests

migrate:
	docker-compose run --rm api alembic upgrade head

logs:
	docker-compose logs -f