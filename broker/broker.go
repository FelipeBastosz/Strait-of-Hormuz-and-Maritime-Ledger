// ============================================================
// BROKER — Nó do cluster totalmente descentralizado
//
// Arquitetura P2P pura — sem coordenador, sem líder, sem eleição.
// Cada broker é igual aos demais. Todo o controle de concorrência
// sobre os drones é feito exclusivamente pelo Ricart-Agrawala.
// ============================================================

package main

import (
	"bufio"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
	"math/rand"

	"Strait-of-Hormuz-and-Maritime-Ledger/blockchain"
	"Strait-of-Hormuz-and-Maritime-Ledger/protocol"
	"Strait-of-Hormuz-and-Maritime-Ledger/state"
)

type connSegura struct {
	mu      sync.Mutex
	conn    net.Conn
	encoder *json.Encoder
}

func novaConnSegura(conn net.Conn) *connSegura {
	return &connSegura{
		conn:    conn,
		encoder: json.NewEncoder(conn),
	}
}

func (c *connSegura) enviar(msg protocol.Mensagem) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.encoder.Encode(msg)
}

func (c *connSegura) fechar() {
	_ = c.conn.Close()
}

const (
	intervaloGossip    = 2 * time.Second
	intervaloHeartbeat = 5 * time.Second
	timeoutHeartbeat   = 12 * time.Second
	creditosIniciais   = 100
	custoEscolta       = 10
)

// ============================================================
// STRUCT BROKER
// ============================================================

type Broker struct {
	mu sync.Mutex

	id       string
	endereco string

	mapaRede map[string]string

	drones map[string]*protocol.Drone
	fila   state.FilaComAging
	missoesAtivas map[string]bool

	encerrando bool


	// ---- Ricart-Agrawala ----------------------------------------
	relogioLocal int64
	requesting   bool
	inCS         bool
	currentReqID string
	currentReqRA protocol.RequisicaoRA
	respostasOK  map[string]bool
	deferred     map[string]bool

	// ---- Conexões ativo-vivas ------------------------------------
	connBrokers  map[string]net.Conn
	connDrones   map[net.Conn]string
	connClientes map[net.Conn]string
	ultimoHB     map[string]time.Time

	// ---- Blockchain (P2P) ----------------------------------------
	chain           *blockchain.Chain
	votosBloco      map[string]int
	blocosPendentes map[string]blockchain.Bloco
}

// ============================================================
// INICIALIZAÇÃO
// ============================================================

func novoBroker(id string, mapaRede map[string]string) *Broker {
	saldosIniciais := map[string]int{
		"companhia-a": creditosIniciais,
		"companhia-b": creditosIniciais,
		"companhia-c": creditosIniciais,
		"companhia-d": creditosIniciais,
	}

	b := &Broker{
		id:              id,
		endereco:        mapaRede[id],
		mapaRede:        mapaRede,
		drones:          make(map[string]*protocol.Drone),
		missoesAtivas:   make(map[string]bool), 
		respostasOK:     make(map[string]bool),
		deferred:        make(map[string]bool),
		connBrokers:     make(map[string]net.Conn),
		connDrones:      make(map[net.Conn]string),
		connClientes:    make(map[net.Conn]string),
		ultimoHB:        make(map[string]time.Time),
		votosBloco:      make(map[string]int),
		blocosPendentes: make(map[string]blockchain.Bloco),
		chain:           blockchain.NovaChain(id, saldosIniciais),
	}
	b.fila.Inicializar()

	fmt.Printf("[Broker %s] Iniciado em %s | %d nós no cluster\n",
		id, mapaRede[id], len(mapaRede))
	return b
}

// ============================================================
// UTILS
// ============================================================

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func (b *Broker) peersAtivos() map[string]net.Conn {
	b.mu.Lock()
	defer b.mu.Unlock()
	copia := make(map[string]net.Conn, len(b.connBrokers))
	for id, c := range b.connBrokers {
		copia[id] = c
	}
	return copia
}

func (b *Broker) quorumRA() int {
	return len(b.connBrokers)
}

func (b *Broker) dronesLivres() int {
	n := 0
	for _, d := range b.drones {
		if d.Status == "disponivel" {
			n++
		}
	}
	return n
}

// ============================================================
// SERVIDOR TLS
// ============================================================

func (b *Broker) iniciarServidor() {
	cert, err := tls.LoadX509KeyPair("cert.pem", "key.pem")
	if err != nil {
		fmt.Printf("[Broker %s] ERRO: certificados TLS não encontrados: %v\n", b.id, err)
		os.Exit(1)
	}

	cfg := &tls.Config{Certificates: []tls.Certificate{cert}}

	partes := strings.Split(b.endereco, ":")
	if len(partes) != 2 {
		fmt.Printf("[Broker %s] ERRO endereço mal formatado: %s\n", b.id, b.endereco)
		os.Exit(1)
	}
	porta := partes[1]

	listener, err := tls.Listen("tcp", ":"+porta, cfg)
	if err != nil {
		fmt.Printf("[Broker %s] ERRO ao iniciar listener: %v\n", b.id, err)
		os.Exit(1)
	}
	fmt.Printf("[Broker %s] Escutando em :%s (TLS)\n", b.id, porta)

	go b.conectarPeers()
	go b.heartbeatLoop()
	go b.monitorarPeers()

	for {
		conn, err := listener.Accept()
		if err != nil {
			if b.encerrando {
				return
			}
			continue
		}
		go b.handleConexao(conn)
	}
}

// ============================================================
// CONEXÃO COM PEERS
// ============================================================

func (b *Broker) conectarPeers() {
	time.Sleep(3 * time.Second)

	for peerID, peerAddr := range b.mapaRede {
		if peerID == b.id {
			continue
		}
		go b.mantenerConexaoPeer(peerID, peerAddr)
	}

	time.Sleep(2 * time.Second)
	b.solicitarChain()
}

func (b *Broker) mantenerConexaoPeer(peerID, peerAddr string) {
	cfg := &tls.Config{InsecureSkipVerify: true}
	for {
		b.mu.Lock()
		_, jaConectado := b.connBrokers[peerID]
		b.mu.Unlock()

		if jaConectado {
			time.Sleep(5 * time.Second)
			continue
		}

		conn, err := tls.Dial("tcp", peerAddr, cfg)
		if err != nil {
			time.Sleep(3 * time.Second)
			continue
		}

		hs := protocol.InfoConexao{Tipo: "broker", ID: b.id}
		payload, _ := json.Marshal(hs)
		msg := protocol.Mensagem{
			Tipo:      protocol.TipoHandshake,
			IDOrigem:  b.id,
			Timestamp: time.Now(),
			Payload:   string(payload),
		}
		if err := json.NewEncoder(conn).Encode(msg); err != nil {
			conn.Close()
			time.Sleep(3 * time.Second)
			continue
		}

		b.mu.Lock()
		b.connBrokers[peerID] = conn
		b.ultimoHB[peerID] = time.Now()
		b.mu.Unlock()

		fmt.Printf("[Broker %s] Peer %s conectado\n", b.id, peerID)
		b.lerMensagens(conn, peerID)
	}
}

// ============================================================
// HANDLER DE NOVA CONEXÃO
// ============================================================

func (b *Broker) handleConexao(conn net.Conn) {
	scanner := bufio.NewScanner(conn)
	if !scanner.Scan() {
		conn.Close()
		return
	}

	var msg protocol.Mensagem
	if err := json.Unmarshal(scanner.Bytes(), &msg); err != nil {
		conn.Close()
		return
	}

	if msg.Tipo == protocol.TipoHandshake {
		var info protocol.InfoConexao
		json.Unmarshal([]byte(msg.Payload), &info)

		switch info.Tipo {
		case "broker":
			b.mu.Lock()
			b.connBrokers[info.ID] = conn
			b.ultimoHB[info.ID] = time.Now()
			b.mu.Unlock()
			fmt.Printf("[Broker %s] Peer %s conectou (passivo)\n", b.id, info.ID)
			b.lerMensagens(conn, info.ID)

		case "drone":
			b.mu.Lock()
			b.connDrones[conn] = info.ID
			if _, existe := b.drones[info.ID]; !existe {
				b.drones[info.ID] = &protocol.Drone{
					ID:      info.ID,
					Status:  "disponivel",
					Bateria: 100,
				}
			} else {
				if info.Endereco != "" {
					d.Posicao = info.Endereco
				}
				// Força o status para disponivel independente de estar como "recarregando" ou "indisponivel"
				// Isso garante que o Broker limpe o estado local assim que o drone se reconectar ativo
				if d.Status != "disponivel" {
					d.Status = "disponivel"
					fmt.Printf("[Broker %s] Drone %s está pronto e disponível novamente\n", b.id, info.ID)
				}
			}
			b.mu.Unlock()
			fmt.Printf("[Broker %s] Drone %s registrado\n", b.id, info.ID)
			go b.tentarDespachar()
			b.lerMensagens(conn, info.ID)

		default:
			b.mu.Lock()
			b.connClientes[conn] = info.ID
			b.mu.Unlock()
			b.lerMensagens(conn, info.ID)
		}
	} else {
		// FALLBACK: Sensores enviam a ocorrência direto sem handshake
		b.mu.Lock()
		b.connClientes[conn] = msg.IDOrigem
		b.mu.Unlock()

		b.despachar(conn, msg)
		b.lerMensagens(conn, msg.IDOrigem)
	}
}

// ============================================================
// LOOP DE LEITURA POR CONEXÃO
// ============================================================

func (b *Broker) lerMensagens(conn net.Conn, remetenteID string) {
	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		var msg protocol.Mensagem
		if err := json.Unmarshal(scanner.Bytes(), &msg); err != nil {
			continue
		}
		b.despachar(conn, msg)
	}
	b.removerConexao(conn, remetenteID)
}

// ============================================================
// DISPATCHER
// ============================================================

func (b *Broker) despachar(conn net.Conn, msg protocol.Mensagem) {
	b.mu.Lock()
	b.relogioLocal = maxInt64(b.relogioLocal, msg.Timestamp.UnixNano()) + 1
	b.mu.Unlock()

	switch msg.Tipo {
	case protocol.TipoHeartbeat:
		b.mu.Lock()
		b.ultimoHB[msg.IDOrigem] = time.Now()
		b.mu.Unlock()
		pong := protocol.Mensagem{
			Tipo: protocol.TipoPong, IDOrigem: b.id, Timestamp: time.Now(),
		}
		json.NewEncoder(conn).Encode(pong)

	case protocol.TipoPong:
		b.mu.Lock()
		b.ultimoHB[msg.IDOrigem] = time.Now()
		b.mu.Unlock()
		
	case protocol.TipoReservaDrone:
		b.mu.Lock()
		if d, ok := b.drones[msg.Payload]; ok {
			d.Status = "em_missao"
		}
		b.mu.Unlock()

	case protocol.TipoOcorrencia:
		go b.handleOcorrencia(conn, msg)

	case protocol.TipoStatusDrone:
		go b.handleStatusDrone(conn, msg)


	case protocol.TipoRARequest:
		go b.handleRARequest(conn, msg)

	case protocol.TipoRAOK:
		go b.handleRAOK(msg)

	case protocol.TipoNovoBloco:
		go b.handleNovoBloco(conn, msg)

	case protocol.TipoAceiteBloco:
		go b.handleAceiteBloco(msg)

	case protocol.TipoReqChain:
		go b.handleReqChain(conn)

	case protocol.TipoRespChain:
		go b.handleRespChain(msg)

	case protocol.TipoConsultaSaldo:
		go b.handleConsultaSaldo(conn, msg)
	}
}

// ============================================================
// OCORRÊNCIA
// ============================================================

func (b *Broker) handleOcorrencia(conn net.Conn, msg protocol.Mensagem) {
	var oc protocol.Ocorrencia
	if err := json.Unmarshal([]byte(msg.Payload), &oc); err != nil {
		fmt.Printf("[Broker %s] Ocorrência malformada: %v\n", b.id, err)
		return
	}
	if oc.Timestamp.IsZero() {
		oc.Timestamp = time.Now()
	}
	if oc.Creditos <= 0 {
		oc.Creditos = custoEscolta
	}

	if oc.Solicitante != "" {
		if err := b.chain.ValidarTransacao(oc.Solicitante, oc.Creditos); err != nil {
			fmt.Printf("[Broker %s] Créditos insuficientes: %v\n", b.id, err)
			nack := protocol.Mensagem{
				Tipo:      "NACK",
				IDOrigem:  b.id,
				Timestamp: time.Now(),
				Payload:   fmt.Sprintf(`{"erro":"%s"}`, err.Error()),
			}
			json.NewEncoder(conn).Encode(nack)
			return
		}
		fmt.Printf("[Broker %s] Saldo validado: %s tem %d crédito(s)\n",
			b.id, oc.Solicitante, b.chain.ConsultarSaldo(oc.Solicitante))
	}

	b.mu.Lock()
	b.fila.Push(&oc)
	b.mu.Unlock()

	ack := protocol.Mensagem{
		Tipo: protocol.TipoACK, IDOrigem: b.id, Timestamp: time.Now(),
	}
	json.NewEncoder(conn).Encode(ack)
	fmt.Printf("[Broker %s] Ocorrência %s enfileirada (P%d) | fila local: %d\n",
		b.id, oc.ID, oc.Prioridade, b.fila.Len())

	go b.tentarDespachar()
}

// ============================================================
// STATUS DO DRONE
// ============================================================

func (b *Broker) handleStatusDrone(conn net.Conn, msg protocol.Mensagem) {
	var laudo protocol.Laudo
	if err := json.Unmarshal([]byte(msg.Payload), &laudo); err != nil {
		return
	}

	b.mu.Lock()
	droneID := msg.IDOrigem 
	if d, ok := b.drones[droneID]; ok {
		d.Status = "disponivel"
		d.MissaoID = ""
	}

	// Despachando OKs adiados
	adiados := make([]string, 0, len(b.deferred))
	for id := range b.deferred {
		adiados = append(adiados, id)
	}
	b.deferred = make(map[string]bool) // Limpa a lista completamente
	b.mu.Unlock()

	fmt.Printf("[Broker %s] ✅ Missão %s concluída | drone=%s | resultado: %s\n",
		b.id, laudo.MissaoID, laudo.DroneID, laudo.Resultado)

	// Dispara o OK para todos que estavam esperando, eles que se desempatem!
	for _, peerID := range adiados {
		fmt.Printf("[Broker %s] [RA] Recurso liberado! Enviando OK adiado para %s\n", b.id, peerID)
		b.enviarRAOK(peerID)
	}

	b.mu.Lock()
	eraMinhaMissao := b.missoesAtivas[laudo.MissaoID]
	if eraMinhaMissao {
		delete(b.missoesAtivas, laudo.MissaoID) // Limpa da memória
	}
	b.mu.Unlock()

	// Apenas o Broker que despachou o drone tem o direito de propor o bloco
	if eraMinhaMissao {
		go b.proporBloco(blockchain.TipoBloco_Laudo, laudo)
	}

	go b.tentarDespachar()
}

// ============================================================
// DESPACHO
// ============================================================

func (b *Broker) tentarDespachar() {
	b.mu.Lock()

	if b.fila.Len() == 0 {
		b.mu.Unlock()
		return
	}

	livres := b.dronesLivres()
	totalNos := len(b.mapaRede)

	if livres >= totalNos {
		oc := b.fila.Pop()
		b.mu.Unlock()
		if oc != nil {
			b.despacharDrone(oc)
			go b.tentarDespachar()
		}
		return
	}

	if b.requesting || b.inCS || livres == 0 {
		b.mu.Unlock()
		return
	}

	oc := b.fila.Pop()
	if oc == nil {
		b.mu.Unlock()
		return
	}

	b.relogioLocal++
	b.requesting = true
	b.currentReqID = oc.ID
	b.currentReqRA = protocol.RequisicaoRA{
		BrokerID:   b.id,
		Relogio:    b.relogioLocal,
		Timestamp:  time.Now(),
		Origem:     oc.ID,
		Prioridade: oc.Prioridade,
	}
	b.respostasOK = make(map[string]bool)
	b.deferred = make(map[string]bool)

	peers := make(map[string]net.Conn, len(b.connBrokers))
	for id, c := range b.connBrokers {
		peers[id] = c
	}
	b.mu.Unlock()

	fmt.Printf("[Broker %s] [RA] REQUEST ocorrência=%s prioridade=%d relógio=%d\n",
		b.id, oc.ID, oc.Prioridade, b.currentReqRA.Relogio)

	payload, _ := json.Marshal(b.currentReqRA)
	reqMsg := protocol.Mensagem{
		Tipo:      protocol.TipoRARequest,
		IDOrigem:  b.id,
		Timestamp: time.Now(),
		Payload:   string(payload),
	}
	for _, c := range peers {
		json.NewEncoder(c).Encode(reqMsg)
	}

	if len(peers) == 0 {
		b.entrarCS(oc)
	}
}

// ============================================================
// RICART-AGRAWALA
// ============================================================

func (b *Broker) handleRARequest(conn net.Conn, msg protocol.Mensagem) {
	var req protocol.RequisicaoRA
	if err := json.Unmarshal([]byte(msg.Payload), &req); err != nil {
		return
	}

	b.mu.Lock()
	b.relogioLocal = maxInt64(b.relogioLocal, req.Relogio) + 1
	meuRelogio := b.currentReqRA.Relogio
	idLocal, err1 := strconv.Atoi(b.id)
	idReq, err2 := strconv.Atoi(req.BrokerID)

	var deveAdiar bool
	if err1 == nil && err2 == nil {
		deveAdiar = b.inCS || (b.requesting && (meuRelogio < req.Relogio || (meuRelogio == req.Relogio && idLocal < idReq)))
	} else {
		deveAdiar = b.inCS || (b.requesting && (meuRelogio < req.Relogio || (meuRelogio == req.Relogio && b.id < req.BrokerID)))
	}

	// === Descarte de adiamento se houver drones extras ===
	if deveAdiar {
		livres := b.dronesLivres()
		vagasReservadas := 0
		if b.requesting {
			vagasReservadas = 1 // Garanto pelo menos 1 para a minha própria requisição
		}
		
		if livres > vagasReservadas {
			deveAdiar = false
			fmt.Printf("[Broker %s] [RA] Vaga extra disponível! Liberando OK imediato para %s\n", b.id, req.BrokerID)
		}
	}

	if deveAdiar {
		b.deferred[req.BrokerID] = true
		b.mu.Unlock()
		fmt.Printf("[Broker %s] [RA] Adiando OK para %s (sem drones extras)\n", b.id, req.BrokerID)
	} else {
		delete(b.deferred, req.BrokerID) // Garante que foi descartado
		b.mu.Unlock()
		b.enviarRAOK(req.BrokerID)
	}
}

func (b *Broker) handleRAOK(msg protocol.Mensagem) {
	b.mu.Lock()
	if !b.requesting {
		b.mu.Unlock()
		return
	}

	b.respostasOK[msg.IDOrigem] = true
	recebidos := len(b.respostasOK)
	necessario := b.quorumRA()
	ocID := b.currentReqID
	oc := &protocol.Ocorrencia{
		ID:         ocID,
		Prioridade: b.currentReqRA.Prioridade,
		Timestamp:  b.currentReqRA.Timestamp,
	}
	b.mu.Unlock()

	fmt.Printf("[Broker %s] [RA] OK de %s (%d/%d)\n",
		b.id, msg.IDOrigem, recebidos, necessario)

	if recebidos >= necessario {
		b.entrarCS(oc)
	}
}

func (b *Broker) entrarCS(oc *protocol.Ocorrencia) {
	b.mu.Lock()
	b.inCS = true
	b.requesting = false
	b.mu.Unlock()

	fmt.Printf("[Broker %s] [RA] → CS | ocorrência=%s\n", b.id, oc.ID)
	b.despacharDrone(oc)

	b.mu.Lock()
	b.inCS = false
	
	livres := b.dronesLivres()
	var peersParaLiberar []string
	
	// Só libera os OKs adiados se, após despachar o meu drone, ainda houver vaga na garagem.
	if livres > 0 {
		for id := range b.deferred {
			peersParaLiberar = append(peersParaLiberar, id)
		}
		b.deferred = make(map[string]bool) // Limpa tudo
	}
	b.mu.Unlock()

	// Se liberou, manda para todos e deixa o RA desempatar
	for _, peerID := range peersParaLiberar {
		fmt.Printf("[Broker %s] [RA] ← CS | Vagas sobrando. Enviando OK adiado para %s\n", b.id, peerID)
		b.enviarRAOK(peerID)
	}

	go b.tentarDespachar()
}

func (b *Broker) enviarRAOK(peerID string) {
	b.mu.Lock()
	conn, ok := b.connBrokers[peerID]
	b.mu.Unlock()
	if !ok {
		return
	}
	msg := protocol.Mensagem{
		Tipo: protocol.TipoRAOK, IDOrigem: b.id, Timestamp: time.Now(),
	}
	json.NewEncoder(conn).Encode(msg)
	fmt.Printf("[Broker %s] [RA] OK → %s\n", b.id, peerID)
}

// ============================================================
// DESPACHO EFETIVO DO DRONE
// ============================================================

func (b *Broker) despacharDrone(oc *protocol.Ocorrencia) {
	b.mu.Lock()

	var droneID string
	var disponiveis []string

	// Levanta todos os drones livres
	for id, d := range b.drones {
		if d.Status == "disponivel" {
			disponiveis = append(disponiveis, id)
		}
	}

	// Escolhe um drone aleatoriamente para evitar que Brokers paralelos peguem o mesmo
	if len(disponiveis) > 0 {
		droneID = disponiveis[rand.Intn(len(disponiveis))] 
		d := b.drones[droneID]
		d.Status = "em_missao"
		d.MissaoID = oc.ID
	}

	reservaMsg := protocol.Mensagem{
		Tipo:      protocol.TipoReservaDrone,
		IDOrigem:  b.id,
		Timestamp: time.Now(),
		Payload:   droneID, 
	}
	for _, c := range b.connBrokers {
		json.NewEncoder(c).Encode(reservaMsg)
	}

	if droneID == "" {
		oc.Prioridade = 3
		b.fila.Push(oc)
		b.mu.Unlock()
		fmt.Printf("[Broker %s] Nenhum drone local disponível. %s voltou à fila.\n",
			b.id, oc.ID)
		return
	}
	b.mu.Unlock()

	if oc.Solicitante != "" {
		tx := protocol.Transacao{
			ID:           fmt.Sprintf("TX-%s-%d", oc.ID, time.Now().UnixNano()),
			De:           oc.Solicitante,
			Para:         "sistema",
			Creditos:     oc.Creditos,
			OcorrenciaID: oc.ID,
			Timestamp:    time.Now(),
		}
		go b.proporBloco(blockchain.TipoBloco_Transacao, tx)
	}

	cmd := protocol.ComandoMissao{
		OcorrenciaID: oc.ID,
		Descricao:    oc.Descricao,
		Prioridade:   oc.Prioridade,
	}
	payload, _ := json.Marshal(cmd)
	msg := protocol.Mensagem{
		Tipo:      protocol.TipoComandoDrone,
		IDOrigem:  b.id,
		Timestamp: time.Now(),
		Payload:   string(payload),
	}


	b.mu.Lock()
	b.missoesAtivas[oc.ID] = true // Registra que este broker é o responsável
	b.mu.Unlock()

	// Corrigido: O Broker age como cliente conectando à porta do servidor interno do Drone!
	porta := "909" + string(droneID[len(droneID)-1])
	addr := droneID + ":" + porta
	var conn net.Conn
	var err error
	

	cfg := &tls.Config{InsecureSkipVerify: true}
	conn, err = tls.DialWithDialer(&net.Dialer{Timeout: 2 * time.Second}, "tcp", addr, cfg)
	if err != nil {
		conn, err = net.DialTimeout("tcp", addr, 2*time.Second)
	}
	if err == nil {
		defer conn.Close()
		json.NewEncoder(conn).Encode(msg)

		// Aguarda até 3 segundos pela resposta do drone
		conn.SetReadDeadline(time.Now().Add(3 * time.Second))
		var resposta map[string]interface{}
		errDecode := json.NewDecoder(conn).Decode(&resposta)
		
		rejeitado := false
		if errDecode == nil {
			if acao, ok := resposta["acao"].(string); ok && acao == "rejeitado" {
				rejeitado = true
			}
		}

		// Se o drone mandou "rejeitado" ou a conexão caiu/deu erro de leitura
		if rejeitado || errDecode != nil {
			motivo := "rejeição do drone"
			if errDecode != nil {
				motivo = "falha na resposta (timeout/EOF)"
			}
			
			fmt.Printf("[Broker %s] Falha ao despachar para drone %s (%s). Devolvendo à fila.\n", b.id, droneID, motivo)
			
			// Reverte o status local urgentemente
			b.mu.Lock()
			if d, ok := b.drones[droneID]; ok {
				d.Status = "disponivel"
				d.MissaoID = ""
			}
			oc.Prioridade = 3 
			b.fila.Push(oc)
			b.mu.Unlock()

			// Roda a roleta de novo
			go b.tentarDespachar()
			return // Sai para não printar a mensagem de sucesso
		}

		fmt.Printf("[Broker %s] ✈  Drone %s → ocorrência %s (P%d)\n", b.id, droneID, oc.ID, oc.Prioridade)
	} else {
        // Se falhar a conexão, a ocorrência também deve voltar para a fila!
		fmt.Printf("[Broker %s] Falha de rede com drone %s: %v. Devolvendo à fila.\n", b.id, droneID, err)
		b.mu.Lock()
		b.fila.Push(oc)
		b.mu.Unlock()

		go b.tentarDespachar()
	}

	fmt.Printf("[Broker %s] ✈  Drone %s → ocorrência %s (P%d)\n",
		b.id, droneID, oc.ID, oc.Prioridade)
}

// ============================================================
// BLOCKCHAIN HANDLERS
// ============================================================

func (b *Broker) proporBloco(tipoDados blockchain.TipoDados, dados interface{}) {
	bloco, err := b.chain.ProporBloco(tipoDados, dados, b.id)
	if err != nil {
		fmt.Printf("[Broker %s] [Chain] Erro ao propor bloco: %v\n", b.id, err)
		return
	}

	b.mu.Lock()
	b.blocosPendentes[bloco.Hash] = bloco
	b.votosBloco[bloco.Hash] = 1
	peers := make(map[string]net.Conn, len(b.connBrokers))
	for id, c := range b.connBrokers {
		peers[id] = c
	}
	total := len(peers) + 1
	b.mu.Unlock()

	if len(peers) == 0 {
		b.commitarBloco(bloco)
		return
	}

	blocoJSON, _ := json.Marshal(bloco)
	msg := protocol.Mensagem{
		Tipo:      protocol.TipoNovoBloco,
		IDOrigem:  b.id,
		Timestamp: time.Now(),
		Payload:   string(blocoJSON),
	}
	for _, c := range peers {
		json.NewEncoder(c).Encode(msg)
	}

	fmt.Printf("[Broker %s] [Chain] Bloco #%d proposto (tipo=%s) | quórum: %d/%d\n",
		b.id, bloco.Indice, tipoDados, total/2+1, total)
}

func (b *Broker) handleNovoBloco(conn net.Conn, msg protocol.Mensagem) {
	var bloco blockchain.Bloco
	if err := json.Unmarshal([]byte(msg.Payload), &bloco); err != nil {
		return
	}

	ultimoBloco := b.chain.UltimoBloco()
	if bloco.HashAnterior != ultimoBloco.Hash {
		fmt.Printf("[Broker %s] [Chain] Desincronizado. Solicitando chain atualizada...\n", b.id)
		b.solicitarChain()
		return
	}

	fmt.Printf("[Broker %s] [Chain] Bloco #%d recebido de %s. Votando...\n", b.id, bloco.Indice, msg.IDOrigem)

	b.mu.Lock()
	// Garante que o bloco está inicializado nos pendentes
	if _, ok := b.blocosPendentes[bloco.Hash]; !ok {
		b.blocosPendentes[bloco.Hash] = bloco
	}

	// SOMA VOTOS: O do propositor (msg.IDOrigem) + o meu voto local (já validado)
	b.votosBloco[bloco.Hash] += 2
	votos := b.votosBloco[bloco.Hash]
	total := len(b.connBrokers) + 1
	b.mu.Unlock()

	aceite := protocol.Mensagem{
		Tipo:      protocol.TipoAceiteBloco,
		IDOrigem:  b.id,
		Timestamp: time.Now(),
		Payload:   fmt.Sprintf(`{"hash":"%s"}`, bloco.Hash),
	}

	peers := b.peersAtivos()
	for _, peerConn := range peers {
		json.NewEncoder(peerConn).Encode(aceite)
	}

	quorum := total/2 + 1
	if votos >= quorum {
		b.commitarBloco(bloco)
	}
}

func (b *Broker) handleAceiteBloco(msg protocol.Mensagem) {
	var payload struct {
		Hash string `json:"hash"`
	}
	if err := json.Unmarshal([]byte(msg.Payload), &payload); err != nil {
		return
	}

	b.mu.Lock()
	b.votosBloco[payload.Hash]++
	votos := b.votosBloco[payload.Hash]
	total := len(b.connBrokers) + 1
	bloco, existe := b.blocosPendentes[payload.Hash]
	b.mu.Unlock()

	// Se não existe, o bloco já foi commitado
	if !existe {
		return
	}

	quorum := total/2 + 1
	fmt.Printf("[Broker %s] [Chain] Aceite de %s para hash %s (%d/%d)\n",
		b.id, msg.IDOrigem, payload.Hash[:8], votos, quorum)

	if votos == quorum {
		b.commitarBloco(bloco)
	}
}

func (b *Broker) commitarBloco(bloco blockchain.Bloco) {
	b.mu.Lock()
	// Se o bloco não estiver mais nos pendentes, é porque já foi commitado. Aborta.
	if _, pendente := b.blocosPendentes[bloco.Hash]; !pendente {
		b.mu.Unlock()
		return 
	}
	// Remove das listas para garantir que o commit aconteça estritamente UMA vez
	delete(b.votosBloco, bloco.Hash)
	delete(b.blocosPendentes, bloco.Hash)
	b.mu.Unlock()

	if err := b.chain.CommitarBloco(bloco); err != nil {
		fmt.Printf("[Broker %s] [Chain] Erro ao commitar bloco #%d: %v\n",
			b.id, bloco.Indice, err)
		return
	}

	if bloco.TipoDados == blockchain.TipoBloco_Transacao {
		var tx protocol.Transacao
		if err := json.Unmarshal([]byte(bloco.Dados), &tx); err == nil {
			b.chain.DebitarCreditos(tx.De, tx.Creditos)
		}
	}
}

func (b *Broker) handleReqChain(conn net.Conn) {
	chainJSON, err := b.chain.SerializarChain()
	if err != nil {
		return
	}
	json.NewEncoder(conn).Encode(protocol.Mensagem{
		Tipo:      protocol.TipoRespChain,
		IDOrigem:  b.id,
		Timestamp: time.Now(),
		Payload:   chainJSON,
	})
}

func (b *Broker) handleRespChain(msg protocol.Mensagem) {
	var blocos []blockchain.Bloco
	if err := json.Unmarshal([]byte(msg.Payload), &blocos); err != nil {
		return
	}
	saldosBase := map[string]int{
		"companhia-a": creditosIniciais,
		"companhia-b": creditosIniciais,
		"companhia-c": creditosIniciais,
		"companhia-d": creditosIniciais,
	}
	if b.chain.SubstituirChain(blocos, saldosBase) {
		fmt.Printf("[Broker %s] [Chain] Sincronizada: %d blocos\n",
			b.id, b.chain.Tamanho())
	}
}

func (b *Broker) handleConsultaSaldo(conn net.Conn, msg protocol.Mensagem) {
	saldo := b.chain.ConsultarSaldo(msg.IDOrigem)
	json.NewEncoder(conn).Encode(protocol.Mensagem{
		Tipo:      protocol.TipoRespSaldo,
		IDOrigem:  b.id,
		Timestamp: time.Now(),
		Payload:   fmt.Sprintf(`{"companhia":"%s","saldo":%d}`, msg.IDOrigem, saldo),
	})
}

func (b *Broker) solicitarChain() {
	peers := b.peersAtivos()
	for _, c := range peers {
		json.NewEncoder(c).Encode(protocol.Mensagem{
			Tipo: protocol.TipoReqChain, IDOrigem: b.id, Timestamp: time.Now(),
		})
		return
	}
}

// ============================================================
// HEARTBEAT E MONITORAMENTO DE PEERS
// ============================================================

func (b *Broker) heartbeatLoop() {
	ticker := time.NewTicker(intervaloHeartbeat)
	for range ticker.C {
		peers := b.peersAtivos()
		msg := protocol.Mensagem{
			Tipo: protocol.TipoHeartbeat, IDOrigem: b.id, Timestamp: time.Now(),
		}
		for _, c := range peers {
			json.NewEncoder(c).Encode(msg)
		}
	}
}

func (b *Broker) monitorarPeers() {
	ticker := time.NewTicker(intervaloHeartbeat)
	for range ticker.C {
		b.mu.Lock()
		var mortos []string
		for id, t := range b.ultimoHB {
			if time.Since(t) > timeoutHeartbeat {
				mortos = append(mortos, id)
			}
		}
		b.mu.Unlock()

		for _, id := range mortos {
			fmt.Printf("[Broker %s] Peer %s não responde. Removendo.\n", b.id, id)
			b.mu.Lock()
			if c, ok := b.connBrokers[id]; ok {
				c.Close()
			}
			delete(b.connBrokers, id)
			delete(b.ultimoHB, id)

			if b.requesting {
				delete(b.respostasOK, id)
				delete(b.deferred, id)

				totalAtivos := 0
				for key := range b.connBrokers {
					if !strings.HasPrefix(key, "tester") {
						totalAtivos++
					}
				}

				// ---: Previne Double-CS em caso de queda simultânea de rede ---
				if len(b.respostasOK) >= totalAtivos && totalAtivos > 0 {
					b.requesting = false
					oc := &protocol.Ocorrencia{
						ID:         b.currentReqID,
						Prioridade: b.currentReqRA.Prioridade,
						Timestamp:  b.currentReqRA.Timestamp,
					}
					go b.entrarCS(oc)
				}
			}
			b.mu.Unlock()
		}
	}
}

// ============================================================
// REMOÇÃO DE CONEXÕES
// ============================================================

func (b *Broker) removerConexao(conn net.Conn, id string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	fmt.Printf("[Broker %s] Conexão removida: ID %s - %s\n", b.id, id, conn.RemoteAddr())

	if _, ok := b.connDrones[conn]; ok {
		delete(b.connDrones, conn)
		// Alteração Vital: NUNCA apagamos o drone do mapa b.drones.
		// A conexão curta serviu só para ele reportar presença.
	}

	delete(b.connBrokers, id)
	delete(b.connClientes, conn)
	delete(b.ultimoHB, id)

	if b.requesting {
		delete(b.respostasOK, id)
		delete(b.deferred, id)
	}
}

// ============================================================
// ENCERRAMENTO GRACIOSO
// ============================================================

func (b *Broker) encerrarSistema() {
	sc := make(chan os.Signal, 1)
	signal.Notify(sc, os.Interrupt, syscall.SIGTERM)
	<-sc

	b.mu.Lock()
	b.encerrando = true
	b.mu.Unlock()

	fmt.Printf("\n[Broker %s] Encerrando...\n", b.id)

	if err := b.chain.SalvarChain(); err != nil {
		fmt.Printf("[Broker %s] Aviso: falha ao persistir chain no encerramento: %v\n", b.id, err)
	} else {
		fmt.Printf("[Broker %s] Chain persistida com sucesso (%d blocos).\n", b.id, b.chain.Tamanho())
	}

	b.mu.Lock()
	for _, c := range b.connBrokers {
		c.Close()
	}
	b.mu.Unlock()
	os.Exit(0)
}

// ============================================================
// CONFIGURAÇÃO
// ============================================================

func carregarConfiguracao(caminho string) (map[string]string, error) {
	dados, err := os.ReadFile(caminho)
	if err != nil {
		return nil, fmt.Errorf("não foi possível ler %s: %w", caminho, err)
	}
	var mapa map[string]string
	if err := json.Unmarshal(dados, &mapa); err != nil {
		return nil, fmt.Errorf("config.json malformado: %w", err)
	}
	return mapa, nil
}

// ============================================================
// MAIN
// ============================================================

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Uso: broker [ID_BROKER]")
		fmt.Println("Exemplo: broker broker1")
		os.Exit(1)
	}

	brokerID := os.Args[1]

	configPath := os.Getenv("CONFIG_PATH")
	if configPath == "" {
		configPath = "/app/config.json"
	}

	mapaRede, err := carregarConfiguracao(configPath)
	if err != nil {
		fmt.Printf("ERRO: %v\n", err)
		os.Exit(1)
	}
	if _, ok := mapaRede[brokerID]; !ok {
		fmt.Printf("ERRO: broker '%s' não encontrado no config.json\n", brokerID)
		fmt.Printf("Brokers conhecidos: %v\n", mapaRede)
		os.Exit(1)
	}

	b := novoBroker(brokerID, mapaRede)
	go b.encerrarSistema()

	fmt.Printf("[Broker %s] Cluster:\n", brokerID)
	for id, addr := range mapaRede {
		fmt.Printf("  %s → %s\n", id, addr)
	}

	b.iniciarServidor()
}

var _ = strconv.Itoa
