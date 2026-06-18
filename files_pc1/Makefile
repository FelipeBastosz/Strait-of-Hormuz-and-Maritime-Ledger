# ==============================================================================
# Makefile — Host 1 (172.16.201.14)
# ==============================================================================

.PHONY: up-test up-full down client logs

COMPOSE=docker compose -f docker-compose.yml

up-test:
	$(COMPOSE) up --build -d broker1 broker2 drone1 drone2 tester
	@echo "✅ Host 1 iniciado."

up-full:
	$(COMPOSE) up --build -d
	@echo "🔥 Host 1 completo."

client:
	docker attach $$($(COMPOSE) ps -q client)

logs:
	$(COMPOSE) logs -f broker1 broker2

down:
	$(COMPOSE) down -v
	@echo "🧹 Host 1 finalizado."

tester:
	docker attach $$(docker-compose ps -q tester)