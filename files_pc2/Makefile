# ==============================================================================
# Makefile — Host 2 (172.16.201.13)
# ==============================================================================

.PHONY: up-test up-full down tester logs

COMPOSE=docker compose -f docker-compose.yml

up-test:
	$(COMPOSE) up --build -d broker3 broker4 broker5 drone3 tester
	@echo "✅ Host 2 iniciado."

up-full:
	$(COMPOSE) up --build -d
	@echo "🔥 Host 2 completo."

tester:
	docker attach $$($(COMPOSE) ps -q tester)

logs:
	$(COMPOSE) logs -f broker3 broker4 broker5

down:
	$(COMPOSE) down -v
	@echo "🧹 Host 2 finalizado."
