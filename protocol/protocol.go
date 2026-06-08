// ============================================================
// PROTOCOL — Envelope universal de mensagens
//
// Toda comunicação TCP do sistema usa este pacote.
// O campo Tipo define o handler no switch-case do broker.
// O campo Payload carrega o JSON específico de cada mensagem.
//
// Origem: envelope e domínio de Felipe; tipos de blockchain novos.
// ============================================================

package protocol

import "time"

// ============================================================
// CONSTANTES DE TIPO DE MENSAGEM
// ============================================================

const (
	// --- Operação de drones (Felipe) ---
	TipoOcorrencia    = "NOVA_TAREFA"    // Sensor/cliente reportando incidente
	TipoStatusDrone   = "STATUS_DRONE"   // Drone concluiu missão
	TipoComandoDrone  = "COMANDO_DRONE"  // Broker ordenando drone a se mover
	TipoRegistroDrone = "REGISTRO_DRONE" // Drone se apresentando ao cluster
	TipoReservaDrone  = "RESERVA_DRONE"  // Drone reservado para um Broker específico
	TipoACK           = "ACK"            // Confirmação de recebimento

	// --- Sincronização de estado (Felipe) ---
	TipoSyncEstado   = "SYNC_GLOBAL" // Coordenador enviando snapshot de estado
	TipoPedidoEstado = "REQ_SYNC"    // Broker recém-iniciado pedindo estado

	// --- Exclusão mútua Ricart-Agrawala (Daniel) ---
	TipoRARequest = "RA_REQUEST" // Pedido de entrada na região crítica
	TipoRAOK      = "RA_OK"      // Permissão concedida ao solicitante

	// --- Handshake de identificação (Daniel) ---
	TipoHandshake = "HANDSHAKE" // Primeira mensagem ao conectar

	// --- Heartbeat (Daniel) ---
	TipoHeartbeat = "HEARTBEAT" // Sinal de vida periódico
	TipoPong      = "PONG"      // Resposta ao heartbeat

	// --- blockchain (Problema 3 — novos) ---
	TipoTransacao     = "TRANSACAO"       // Companhia paga créditos para requisitar drone
	TipoLaudo         = "LAUDO_MISSAO"    // Drone concluiu — laudo registrado em bloco
	TipoNovoBloco     = "NOVO_BLOCO"      // Broadcast de bloco proposto para peers
	TipoAceiteBloco   = "ACEITE_BLOCO"    // Voto de aceite de um peer (consenso PoA)
	TipoReqChain      = "REQ_BLOCKCHAIN"  // Nó novo pedindo a chain completa
	TipoRespChain     = "RESP_BLOCKCHAIN" // Resposta com a chain serializada
	TipoConsultaSaldo = "CONSULTA_SALDO"  // Cliente consultando saldo de créditos
	TipoRespSaldo     = "RESP_SALDO"      // Broker respondendo com saldo atual
)

// ============================================================
// ENVELOPE UNIVERSAL
// ============================================================

// Mensagem é o "envelope" de toda comunicação TCP.
// O receptor lê Tipo primeiro e sabe qual struct deserializar de Payload.
type Mensagem struct {
	Tipo      string    `json:"tipo"`
	IDOrigem  string    `json:"id_origem"` // ID do remetente (broker, drone, sensor, "cliente")
	Timestamp time.Time `json:"timestamp"` // Relógio lógico / criação da mensagem
	Payload   string    `json:"payload"`   // JSON da struct específica do tipo
}

// ============================================================
// DOMÍNIO: OPERAÇÕES DE DRONES
// ============================================================

// Ocorrencia representa uma emergência detectada por sensor ou injetada por cliente.
type Ocorrencia struct {
	ID          string    `json:"id"`         // Ex: "S1A-OC0001" ou "USER-host-OC0001"
	Prioridade  int       `json:"prioridade"` // 3=Crítico, 2=Alerta, 1=Aviso
	Timestamp   time.Time `json:"timestamp"`  // Desempate na heap (quem chegou primeiro)
	Descricao   string    `json:"descricao"`
	Setor       string    `json:"setor"`
	Solicitante string    `json:"solicitante"` // ID da companhia que requisitou (Problema 3)
	Creditos    int       `json:"creditos"`    // Créditos a debitar (Problema 3)
}

// Drone é a representação de um drone físico registrado no cluster.
type Drone struct {
	ID       string `json:"id"`
	Posicao  string `json:"posicao"` // Endereço TCP onde recebe comandos
	Status   string `json:"status"`  // "disponivel" | "em_missao" | "recarregando"
	Bateria  int    `json:"bateria"`
	MissaoID string `json:"missao_id"` // ID da ocorrência em atendimento (vazio se livre)
}

// ComandoMissao é o payload enviado no TipoComandoDrone.
type ComandoMissao struct {
	OcorrenciaID string `json:"ocorrencia_id"`
	Descricao    string `json:"descricao"`
	Prioridade   int    `json:"prioridade"`
}

// Laudo é o relatório final de uma missão concluída pelo drone.
// É registrado como dado de um Bloco na blockchain.
type Laudo struct {
	MissaoID  string    `json:"missao_id"`
	DroneID   string    `json:"drone_id"`
	Setor     string    `json:"setor"`
	Resultado string    `json:"resultado"` // Ex: "rota segura", "obstáculo detectado"
	Descricao string    `json:"descricao"`
	Timestamp time.Time `json:"timestamp"`
}

// ============================================================
// DOMÍNIO: BLOCKCHAIN
// ============================================================

// Transacao representa o pagamento de créditos por uma companhia de navegação.
// É registrada como dado de um Bloco para garantir imutabilidade e anti-duplo gasto.
type Transacao struct {
	ID           string    `json:"id"`            // UUID gerado pelo broker
	De           string    `json:"de"`            // ID da companhia pagante
	Para         string    `json:"para"`          // "sistema" (fundo operacional)
	Creditos     int       `json:"creditos"`      // Quantidade debitada
	OcorrenciaID string    `json:"ocorrencia_id"` // Missão associada
	Timestamp    time.Time `json:"timestamp"`
	Assinatura   string    `json:"assinatura"` // HMAC-SHA256 do conteúdo
}

// ============================================================
// RICART-AGRAWALA (Daniel)
// ============================================================

// RequisicaoRA é o payload enviado em TipoRARequest / TipoRAOK.
type RequisicaoRA struct {
	BrokerID   string    `json:"broker_id"`
	Relogio    int64     `json:"relogio"` // Relógio de Lamport do solicitante
	Timestamp  time.Time `json:"timestamp"`
	Origem     string    `json:"origem"` // ID da ocorrência disputada
	Prioridade int       `json:"prioridade"`
}

// ============================================================
// HANDSHAKE / IDENTIFICAÇÃO
// ============================================================

// InfoConexao é enviada como primeira mensagem por qualquer componente ao conectar.
type InfoConexao struct {
	Tipo string `json:"tipo"` // "broker" | "drone" | "sensor" | "cliente"
	ID   string `json:"id"`
}
