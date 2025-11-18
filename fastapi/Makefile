.PHONY: build up down logs shell test clean docker-up docker-down docker-build docker-logs docker-run

docker-up: ## Sobe containers com Docker Compose
	docker compose up $(ARGS)

docker-down: ## Para containers
	docker compose down --remove-orphans $(ARGS)

docker-build: ## Constrói imagem da aplicação
	docker compose build $(ARGS) fastapi

docker-logs: ## Mostra logs dos containers
	docker compose logs $(ARGS) -f fastapi

docker-run: ## Executa container interativamente
	docker compose down --remove-orphans && docker compose build fastapi && docker compose run --rm $(ARGS) fastapi

docker-shell: ## Abre shell no container
	docker compose exec fastapi /bin/bash

clean: ## Limpa arquivos temporários
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -delete
	find . -type d -name "*.egg-info" -exec rm -rf {} +
	rm -rf .pytest_cache
	rm -rf .coverage
	rm -rf htmlcov
	rm -rf .mypy_cache

help: ## Mostra esta mensagem de ajuda
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'
