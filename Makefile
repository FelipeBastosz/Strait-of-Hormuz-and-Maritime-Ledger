# ==============================================================================
# Makefile — Estreito de Ormuz P3 (Ambiente de Testes Controlado)
# ==============================================================================

.PHONY: up-test up-full down tester client logs

# 1. Sobe APENAS a infraestrutura base (Brokers, Drones, Client e Tester)
# Sem os sensores automáticos. Ambiente perfeito para depurar manualmente.
up-test:
	docker-compose up --build -d broker1 broker2 broker3 broker4 drone1 drone2 drone3 drone4 client tester
	@echo "✅ Ambiente controlado iniciado em background (Sensores OFF)."
	@echo "🔍 Use 'make logs' para ver a rede ou 'make tester' para injetar comandos."

# 2. Sobe o ecossistema completo (incluindo todos os 8 sensores em modo caótico)
up-full:
	docker-compose up --build -d
	@echo "🔥 Sistema completo rodando (Sensores ON)."

# 3. Acessa o terminal do Tester interativo
tester:
	docker attach $$(docker-compose ps -q tester)

# 4. Acessa o terminal do Cliente interativo
client:
	docker attach $$(docker-compose ps -q client)

# 5. Acompanha os logs apenas dos Brokers (limpo e direto)
logs:
	docker-compose logs -f broker1 broker2 broker3 broker4

# 6. Derruba a rede e LIMPA OS VOLUMES, zerando a blockchain e os saldos
down:
	docker-compose down -v
	@echo "🧹 Rede derrubada e volumes de estado limpos. A Blockchain foi zerada."