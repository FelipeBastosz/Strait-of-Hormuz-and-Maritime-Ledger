# Estreito de Ormuz — Problema 3

Sistema distribuído de coordenação de drones com blockchain P2P para registro imutável de transações e laudos de missão.

## Arquitetura

```
estreito-ormuz-p3/
├── protocol/      # Envelope universal de mensagens + structs de domínio
├── blockchain/    # Ledger distribuído (blocos, hashes, consenso PoA, saldos)
├── state/         # Fila de prioridade com Aging + snapshot de estado global
├── broker/        # Nó do cluster (RA + heartbeat + TLS + blockchain)
├── drone/         # Drone autônomo (TLS, laudo → bloco)
├── sensor/        # Sensor automático de incidentes
├── client/        # Terminal de comando interativo
├── tester/        # Ferramenta de diagnóstico e stress-test
├── config.json    # Mapa de rede do cluster
├── docker-compose.yml
└── gen-certs.sh   # Gerador de certificado TLS
```

## Pré-requisitos

- Docker + Docker Compose
- OpenSSL (para gerar os certificados)

## Configuração inicial

```bash
# 1. Gera os certificados TLS (apenas uma vez)
chmod +x gen-certs.sh
./gen-certs.sh

# 2. Sobe o cluster completo
docker-compose up --build
```

## Uso

### Terminal de comando (cliente)
```bash
docker attach ormuz-p3-client-1
```
Comandos disponíveis:
- `1` — Enviar ocorrência com prioridade e companhia
- `2` — Consultar saldo de créditos na blockchain
- `3` — Trocar companhia ativa

### Tester (diagnóstico e stress-test)
```bash
docker attach ormuz-p3-tester-1
```
Comandos disponíveis:
```
ping   broker1:9081              # mede RTT
req    broker1:9081 3 5          # envia 5 requisições de prioridade 3
req    broker2:9082 2 3 companhia-b   # com companhia específica
autoOK off                       # modo manual de RA
ok     broker1                   # envia OK manualmente
```

### Subir apenas parte do sistema
```bash
# Apenas os brokers
docker-compose up broker1 broker2 broker3 broker4

# Adicionar drones depois
docker-compose up drone1 drone2 drone3 drone4
```

## Decisões de projeto

| Componente | Origem | Justificativa |
|---|---|---|
| Protocolo de mensagens | Felipe | Envelope tipado + ACK garantido |
| Ricart-Agrawala | Daniel | Exclusão mútua sem coordenador único |
| TLS + Heartbeat | Daniel | Segurança na camada de transporte |
| Fila com Aging | Felipe + Daniel | O(log n) + prevenção de starvation |
| config.json + Docker | Felipe | Configuração declarativa, sem IPs hardcoded |
| Blockchain PoA | Novo | Imutabilidade + anti-duplo gasto |

## Companhias cadastradas

| ID | Créditos iniciais |
|---|---|
| companhia-a | 100 |
| companhia-b | 100 |
| companhia-c | 100 |
| companhia-d | 100 |

Cada escolta custa **10 créditos**. O saldo é debitado somente após o bloco de transação ser aprovado pelo consenso PoA (maioria dos brokers vivos).
