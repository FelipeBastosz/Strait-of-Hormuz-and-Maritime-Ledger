// ============================================================
// BROKER — Nó do cluster totalmente descentralizado
//
// Arquitetura P2P pura — sem coordenador, sem líder, sem eleição.
// Cada broker é igual aos demais. Todo o controle de concorrência
// sobre os drones é feito exclusivamente pelo Ricart-Agrawala.
// ============================================================

package main

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

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

type Broker struct {
	mu sync.Mutex

	id       string
	endereco string
	mapaRede map[string]string

	drones        map[string]*protocol.Drone
	fila          state.FilaComAging
	missoesAtivas map[string]bool
	encerrando    bool

	// Ricart-Agrawala
	relogioLocal int64
	requesting   bool
	inCS         bool
	currentReqOc *protocol.Ocorrencia
	currentReqID string
	currentReqRA protocol.RequisicaoRA
	respostasOK  map[string]bool
	deferred     map[string]bool

	// Conexões
	connBrokers  map[string]*connSegura
	connDrones   map[net.Conn]string
	connClientes map[net.Conn]string
	ultimoHB     map[string]time.Time

	// Blockchain
	chain           *blockchain.Chain
	votosBloco      map[string]int
	blocosPendentes map[string]blockchain.Bloco
}

func obterPaisesPorBroker(numBroker string) []string {
	prefixo := "b" + numBroker + "-"
	switch numBroker {
	case "1":
		return []string{prefixo + "alemanha", prefixo + "franca", prefixo + "italia", prefixo + "inglaterra"}
	case "2":
		return []string{prefixo + "china", prefixo + "japao", prefixo + "india", prefixo + "emirados"}
	case "3":
		return []string{prefixo + "eua", prefixo + "canada", prefixo + "brasil", prefixo + "argentina"}
	case "4":
		return []string{prefixo + "egito", prefixo + "somalia", prefixo + "djibuti", prefixo + "africadosul"}
	case "5":
		return []string{prefixo + "australia", prefixo + "novazelandia", prefixo + "indonesia", prefixo + "filipinas"}
	default:
		return []string{prefixo + "desconhecido"}
	}
}

func gerarSaldosIniciais(mapaRede map[string]string) map[string]int {
	saldos := make(map[string]int)
	for brokerNome := range mapaRede {
		num := strings.Replace(brokerNome, "broker", "", 1)
		paises := obterPaisesPorBroker(num)
		for _, pais := range paises {
			saldos[pais] = creditosIniciais
		}
	}
	return saldos
}

func novoBroker(id string, mapaRede map[string]string) *Broker {
	saldosIniciais := gerarSaldosIniciais(mapaRede)

	b := &Broker{
		id:              id,
		endereco:        mapaRede[id],
		mapaRede:        mapaRede,
		drones:          make(map[string]*protocol.Drone),
		missoesAtivas:   make(map[string]bool),
		respostasOK:     make(map[string]bool),
		deferred:        make(map[string]bool),
		connBrokers:     make(map[string]*connSegura),
		connDrones:      make(map[net.Conn]string),
		connClientes:    make(map[net.Conn]string),
		ultimoHB:        make(map[string]time.Time),
		votosBloco:      make(map[string]int),
		blocosPendentes: make(map[string]blockchain.Bloco),
		chain:           blockchain.NovaChain(id, saldosIniciais),
	}
	b.fila.Inicializar()

	fmt.Printf("[Broker %s] Iniciado em %s | %d nós no cluster\n", id, mapaRede[id], len(mapaRede))
	return b
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func (b *Broker) peersAtivos() map[string]*connSegura {
	b.mu.Lock()
	defer b.mu.Unlock()
	copia := make(map[string]*connSegura, len(b.connBrokers))
	for id, c := range b.connBrokers {
		copia[id] = c
	}
	return copia
}

func (b *Broker) dronesLivres() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	n := 0
	for _, d := range b.drones {
		if d.Status == "disponivel" {
			n++
		}
	}
	return n
}

func (b *Broker) iniciarServidor() {
	cert, err := tls.LoadX509KeyPair("cert.pem", "key.pem")
	if err != nil {
		fmt.Printf("[Broker %s] ERRO TLS: %v\n", b.id, err)
		os.Exit(1)
	}

	cfg := &tls.Config{Certificates: []tls.Certificate{cert}}
	partes := strings.Split(b.endereco, ":")
	porta := partes[1]

	listener, err := tls.Listen("tcp", ":"+porta, cfg)
	if err != nil {
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

func (b *Broker) conectarPeers() {
	time.Sleep(3 * time.Second)
	for peerID, peerAddr := range b.mapaRede {
		if peerID == b.id {
			continue
		}
		go b.manterConexaoPeer(peerID, peerAddr)
	}
	time.Sleep(2 * time.Second)
	b.solicitarChain()
}

func (b *Broker) manterConexaoPeer(peerID, peerAddr string) {
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
		cs := novaConnSegura(conn)
		if err := cs.enviar(msg); err != nil {
			conn.Close()
			time.Sleep(3 * time.Second)
			continue
		}

		b.mu.Lock()
		if _, existe := b.connBrokers[peerID]; !existe {
			b.connBrokers[peerID] = cs
			b.ultimoHB[peerID] = time.Now()
		} else {
			b.connBrokers[peerID] = cs
			b.ultimoHB[peerID] = time.Now()
		}
		b.mu.Unlock()

		fmt.Printf("[Broker %s] Peer %s conectado (ativo)\n", b.id, peerID)

		dec := json.NewDecoder(conn)
		b.lerMensagens(cs, peerID, dec)
	}
}

func (b *Broker) handleConexao(conn net.Conn) {
	dec := json.NewDecoder(conn)
	var msg protocol.Mensagem
	if err := dec.Decode(&msg); err != nil {
		conn.Close()
		return
	}

	cs := novaConnSegura(conn)

	if msg.Tipo == protocol.TipoHandshake {
		var info protocol.InfoConexao
		_ = json.Unmarshal([]byte(msg.Payload), &info)

		switch info.Tipo {
		case "broker":
			b.mu.Lock()
			// --- CORREÇÃO: Desambiguação de conexões simultâneas ---
			// Se já existe uma conexão ativa e meu ID é menor que o dele, recuso a duplicada
			if _, existe := b.connBrokers[info.ID]; existe && b.id < info.ID {
				b.mu.Unlock()
				conn.Close()
				return
			}
			b.connBrokers[info.ID] = cs
			b.ultimoHB[info.ID] = time.Now()
			b.mu.Unlock()
			fmt.Printf("[Broker %s] Peer %s conectou (passivo)\n", b.id, info.ID)
			b.lerMensagens(cs, info.ID, dec)

		case "drone":
			b.mu.Lock()
			b.connDrones[conn] = info.ID

			if d, existe := b.drones[info.ID]; !existe {
				b.drones[info.ID] = &protocol.Drone{
					ID:      info.ID,
					Posicao: info.Endereco,
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

			fmt.Printf("[Broker %s] Drone %s registrado (addr=%s)\n", b.id, info.ID, info.Endereco)

			// Notifica o cluster/fila local que há um novo recurso livre para trabalhar
			go b.tentarDespachar()
			b.lerMensagens(cs, info.ID, dec)

		default:
			b.mu.Lock()
			b.connClientes[conn] = info.ID
			b.mu.Unlock()
			b.lerMensagens(cs, info.ID, dec)
		}
		return
	}

	b.mu.Lock()
	b.connClientes[conn] = msg.IDOrigem
	b.mu.Unlock()

	b.despachar(cs, msg)
	b.lerMensagens(cs, msg.IDOrigem, dec)
}

func (b *Broker) lerMensagens(cs *connSegura, remetenteID string, dec *json.Decoder) {
	for {
		var msg protocol.Mensagem
		if err := dec.Decode(&msg); err != nil {
			break
		}
		b.despachar(cs, msg)
	}
	b.removerConexao(cs.conn, remetenteID)
}

func (b *Broker) despachar(cs *connSegura, msg protocol.Mensagem) {
	b.mu.Lock()
	b.relogioLocal = maxInt64(b.relogioLocal, msg.Timestamp.UnixNano()) + 1
	b.mu.Unlock()

	switch msg.Tipo {
	case protocol.TipoHeartbeat:
		b.mu.Lock()
		b.ultimoHB[msg.IDOrigem] = time.Now()
		b.mu.Unlock()
		_ = cs.enviar(protocol.Mensagem{
			Tipo:      protocol.TipoPong,
			IDOrigem:  b.id,
			Timestamp: time.Now(),
		})

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
		go b.handleOcorrencia(cs, msg)

	case protocol.TipoStatusDrone:
		go b.handleStatusDrone(msg)

	case protocol.TipoRARequest:
		go b.handleRARequest(msg)

	case protocol.TipoRAOK:
		go b.handleRAOK(msg)

	case protocol.TipoNovoBloco:
		go b.handleNovoBloco(msg)

	case protocol.TipoAceiteBloco:
		go b.handleAceiteBloco(msg)

	case protocol.TipoReqChain:
		go b.handleReqChain(cs)

	case protocol.TipoRespChain:
		go b.handleRespChain(msg)

	case protocol.TipoConsultaSaldo:
		go b.handleConsultaSaldo(cs, msg)
	}
}

func (b *Broker) handleOcorrencia(cs *connSegura, msg protocol.Mensagem) {
	var oc protocol.Ocorrencia
	if err := json.Unmarshal([]byte(msg.Payload), &oc); err != nil {
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
			_ = cs.enviar(protocol.Mensagem{
				Tipo:      "NACK",
				IDOrigem:  b.id,
				Timestamp: time.Now(),
				Payload:   fmt.Sprintf(`{"erro":"%s"}`, err.Error()),
			})
			return
		}
	}

	b.mu.Lock()
	b.fila.Push(&oc)
	b.mu.Unlock()

	_ = cs.enviar(protocol.Mensagem{
		Tipo:      protocol.TipoACK,
		IDOrigem:  b.id,
		Timestamp: time.Now(),
	})
	fmt.Printf("[Broker %s] Ocorrência %s enfileirada (P%d) | fila local: %d\n",
		b.id, oc.ID, oc.Prioridade, b.fila.Len())

	go b.tentarDespachar()
}

func (b *Broker) handleStatusDrone(msg protocol.Mensagem) {
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

	adiados := make([]string, 0, len(b.deferred))
	for id := range b.deferred {
		adiados = append(adiados, id)
	}
	b.deferred = make(map[string]bool)
	eraMinhaMissao := b.missoesAtivas[laudo.MissaoID]
	if eraMinhaMissao {
		delete(b.missoesAtivas, laudo.MissaoID)
	}
	b.mu.Unlock()

	fmt.Printf("[Broker %s] ✅ Missão %s concluída | drone=%s | resultado: %s\n",
		b.id, laudo.MissaoID, laudo.DroneID, laudo.Resultado)

	paises := obterPaisesPorBroker(b.id)
	if len(paises) >= 4 {
		fmt.Printf("[Broker %s] Saldo atual das minhas companhias: %s=%d %s=%d %s=%d %s=%d\n", b.id,
			paises[0], b.chain.ConsultarSaldo(paises[0]),
			paises[1], b.chain.ConsultarSaldo(paises[1]),
			paises[2], b.chain.ConsultarSaldo(paises[2]),
			paises[3], b.chain.ConsultarSaldo(paises[3]))
	}

	for _, peerID := range adiados {
		b.enviarRAOK(peerID)
	}

	if eraMinhaMissao {
		// Pequeno "Jitter" de tempo para evitar que missões terminando exatamento no mesmo ms causem fork na blockchain
		go func() {
			time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
			b.proporBloco(blockchain.TipoBloco_Laudo, laudo)
		}()
	}

	go b.tentarDespachar()
}

func (b *Broker) tentarDespachar() {
	b.mu.Lock()

	if b.fila.Len() == 0 {
		b.mu.Unlock()
		return
	}

	// Usando dronesLivres() atualizado para não dar deadlock
	livres := 0
	for _, d := range b.drones {
		if d.Status == "disponivel" {
			livres++
		}
	}
	if livres == 0 {
		b.mu.Unlock()
		return
	}

	if b.requesting || b.inCS {
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
	b.currentReqOc = oc
	b.currentReqRA = protocol.RequisicaoRA{
		BrokerID:   b.id,
		Relogio:    b.relogioLocal,
		Timestamp:  time.Now(),
		Origem:     oc.ID,
		Prioridade: oc.Prioridade,
	}
	b.respostasOK = make(map[string]bool)
	b.deferred = make(map[string]bool)

	peers := make(map[string]*connSegura, len(b.connBrokers))
	for id, c := range b.connBrokers {
		if !strings.HasPrefix(id, "tester") {
			peers[id] = c
		}
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
		_ = c.enviar(reqMsg)
	}

	if len(peers) == 0 {
		b.entrarCS(oc)
	}
}

func (b *Broker) handleRARequest(msg protocol.Mensagem) {
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
	if b.inCS {
		deveAdiar = true
	} else if b.requesting {
		minhaPrio := b.currentReqRA.Prioridade
		reqPrio := req.Prioridade

		// REGRA 1: Desempate por Prioridade (Maior valor = Maior Urgência)
		if minhaPrio > reqPrio {
			deveAdiar = true // Minha urgência é maior, eu adio o pedido dele
		} else if reqPrio > minhaPrio {
			deveAdiar = false // Urgência dele é maior, eu cedo a vez
		} else {
			// REGRA 2: Empate de prioridade, resolve pelo Relógio de Lamport (Menor = Mais Antigo)
			if meuRelogio < req.Relogio {
				deveAdiar = true
			} else if meuRelogio > req.Relogio {
				deveAdiar = false
			} else {
				// REGRA 3: Empate de relógio, resolve pelo ID (menor ID ganha)
				if err1 == nil && err2 == nil {
					deveAdiar = idLocal < idReq
				} else {
					deveAdiar = b.id < req.BrokerID
				}
			}
		}
	} else {
		deveAdiar = false
	}

	if deveAdiar {
		b.deferred[req.BrokerID] = true
		b.mu.Unlock()
		return
	}

	// ============================================================
	// CEDENDO A VEZ: RENOVAÇÃO DE CRÉDITOS E AGING DA FILA
	// ============================================================
	if b.requesting {
		// 1. Renovação: propõe transação de 5 créditos do "sistema" para a companhia
		if b.currentReqOc != nil && b.currentReqOc.Solicitante != "" {
			txReward := protocol.Transacao{
				ID:           fmt.Sprintf("RENOVA-%s-%d", b.currentReqOc.ID, time.Now().UnixNano()),
				De:           "sistema",
				Para:         b.currentReqOc.Solicitante,
				Creditos:     5, 
				OcorrenciaID: b.currentReqOc.ID,
				Timestamp:    time.Now(),
			}
			go b.proporBloco(blockchain.TipoBloco_Transacao, txReward)
		}

		// 2. Aging: envelhece a prioridade das ocorrências da fila local
		qtdEnvelhecida := b.fila.AplicarAging()
		if qtdEnvelhecida > 0 {
			fmt.Printf("[Broker %s] [AGING] %d ocorrências na fila subiram de prioridade ao ceder a vez.\n", b.id, qtdEnvelhecida)
		}
	}

	delete(b.deferred, req.BrokerID)
	b.mu.Unlock()
	b.enviarRAOK(req.BrokerID)
}

func (b *Broker) handleRAOK(msg protocol.Mensagem) {
	if strings.HasPrefix(msg.IDOrigem, "tester") {
		return
	}

	b.mu.Lock()
	// --- CORREÇÃO: Ignorar mensagens perdidas/atrasadas se não estivermos esperando ---
	if !b.requesting || b.inCS {
		b.mu.Unlock()
		return
	}

	b.respostasOK[msg.IDOrigem] = true
	recebidos := len(b.respostasOK)

	necessario := 0
	for id := range b.connBrokers {
		if !strings.HasPrefix(id, "tester") {
			necessario++
		}
	}

	oc := b.currentReqOc

	fmt.Printf("[Broker %s] [RA] OK de %s (%d/%d)\n",
		b.id, msg.IDOrigem, recebidos, necessario)


	// --- CORREÇÃO: Voltar ao >= e travar o state imediatamente ---
	if recebidos >= necessario {
		b.requesting = false // Trava pra evitar Double-CS em caso de mensagens duplicadas
		b.mu.Unlock()
		b.entrarCS(oc)
		return
	}

	b.mu.Unlock()
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
	// --- CORREÇÃO: Limpar os dados estaduais do RA para a próxima requisição ---
	b.currentReqOc = nil
	b.currentReqID = ""
	b.respostasOK = make(map[string]bool)

	var peersParaLiberar []string
	for id := range b.deferred {
		peersParaLiberar = append(peersParaLiberar, id)
	}
	b.deferred = make(map[string]bool)
	b.mu.Unlock()

	for _, peerID := range peersParaLiberar {
		b.enviarRAOK(peerID)
	}

	// Dá um trigger para o caso de haver ocorrências pendentes na fila
	go b.tentarDespachar()
}

func (b *Broker) enviarRAOK(peerID string) {
	b.mu.Lock()
	cs, ok := b.connBrokers[peerID]
	b.mu.Unlock()
	if !ok {
		return
	}
	_ = cs.enviar(protocol.Mensagem{
		Tipo:      protocol.TipoRAOK,
		IDOrigem:  b.id,
		Timestamp: time.Now(),
	})
}

func (b *Broker) despacharDrone(oc *protocol.Ocorrencia) {
	var conn net.Conn
	var err error
	b.mu.Lock()

	var droneID string
	var disponiveis []string
	for id, d := range b.drones {
		if d.Status == "disponivel" {
			disponiveis = append(disponiveis, id)
		}
	}

	if len(disponiveis) == 0 {
		oc.Prioridade = 3
		b.fila.Push(oc)
		b.mu.Unlock()
		fmt.Printf("[Broker %s] Nenhum drone local disponível. %s voltou à fila.\n", b.id, oc.ID)
		return
	}

	droneID = disponiveis[rand.Intn(len(disponiveis))]
	d := b.drones[droneID]

	d.Status = "em_missao"
	d.MissaoID = oc.ID
	b.missoesAtivas[oc.ID] = true
	droneAddr := d.Posicao

	reservaMsg := protocol.Mensagem{
		Tipo:      protocol.TipoReservaDrone,
		IDOrigem:  b.id,
		Timestamp: time.Now(),
		Payload:   droneID,
	}

	peers := make([]*connSegura, 0, len(b.connBrokers))
	for _, c := range b.connBrokers {
		peers = append(peers, c)
	}
	b.mu.Unlock()

	go func() {
		for _, c := range peers {
			_ = c.enviar(reservaMsg)
		}
	}()

	if oc.Solicitante != "" {
		tx := protocol.Transacao{
			ID:           fmt.Sprintf("TX-%s-%d", oc.ID, time.Now().UnixNano()),
			De:           oc.Solicitante,
			Para:         "sistema",
			Creditos:     oc.Creditos,
			OcorrenciaID: oc.ID,
			Timestamp:    time.Now(),
		}
		go func() {
			time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
			b.proporBloco(blockchain.TipoBloco_Transacao, tx)
		}()
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

	if droneAddr == "" {
		porta := "909" + string(droneID[len(droneID)-1])
		droneAddr = droneID + ":" + porta
	}

	go func(dID string, addr string, ocorrencia *protocol.Ocorrencia, m protocol.Mensagem) {
		cfg := &tls.Config{InsecureSkipVerify: true}
		conn, err = tls.DialWithDialer(&net.Dialer{Timeout: 2 * time.Second}, "tcp", addr, cfg)
		if err != nil {
			conn, err = net.DialTimeout("tcp", addr, 2*time.Second)
		}

		if err == nil {
			defer conn.Close()
			if err := json.NewEncoder(conn).Encode(m); err != nil {
				b.recolocarOcorrenciaNaFila(ocorrencia, dID, false)
				return
			}

			conn.SetReadDeadline(time.Now().Add(3 * time.Second))
			var resposta map[string]interface{}
			errDecode := json.NewDecoder(conn).Decode(&resposta)

			rejeitado := false
			if errDecode == nil && resposta["acao"] == "rejeitado" {
				rejeitado = true
			}

			if rejeitado || errDecode != nil {
				fmt.Printf("[Broker %s] Falha ao despachar para drone %s. Devolvendo à fila.\n", b.id, dID)
				b.recolocarOcorrenciaNaFila(ocorrencia, dID, rejeitado)
				return
			}

			fmt.Printf("[Broker %s] ✈  Drone %s → ocorrência %s (P%d)\n", b.id, dID, ocorrencia.ID, ocorrencia.Prioridade)
		} else {
			fmt.Printf("[Broker %s] Falha de rede com drone %s (%s). Devolvendo à fila.\n", b.id, dID, addr)
			b.recolocarOcorrenciaNaFila(ocorrencia, dID, false)
		}
	}(droneID, droneAddr, oc, msg)
}

func (b *Broker) recolocarOcorrenciaNaFila(oc *protocol.Ocorrencia, droneID string, rejeitado bool) {
	b.mu.Lock()
	if dr, ok := b.drones[droneID]; ok {
		if rejeitado {
			// Mantém o status como em_missao para que o loop do DDoS não aconteça,
			// permitindo que o drone gerencie sua própria transição para recarga.
			dr.Status = "em_missao"
		} else {
			dr.Status = "indisponivel"
		}
		dr.MissaoID = ""
	}

	// Altera a prioridade para o nível mais baixo para evitar starvation das novas
	oc.Prioridade = 3
	b.fila.Push(oc)

	// Protegido com segurança total pelo mutex principal do Broker
	delete(b.missoesAtivas, oc.ID)
	b.mu.Unlock()

	// Aciona o agendador em background para reavaliar a fila imediatamente
	go b.tentarDespachar()
}

// ============================================================
// BLOCKCHAIN HANDLERS
// ============================================================

func (b *Broker) proporBloco(tipoDados blockchain.TipoDados, dados interface{}) {
	// --- CORREÇÃO: Fila de Consenso com Escape Hatch (Timeout) ---
	inicioEspera := time.Now()
	for {
		b.mu.Lock()
		emConsenso := len(b.blocosPendentes) > 0
		b.mu.Unlock()
		
		if !emConsenso {
			break
		}
		
		// Se a rede travar por mais de 3 segundos, limpamos o gargalo para não congelar o sistema
		if time.Since(inicioEspera) > 3*time.Second {
			b.mu.Lock()
			b.blocosPendentes = make(map[string]blockchain.Bloco)
			b.votosBloco = make(map[string]int)
			b.mu.Unlock()
			fmt.Printf("[Broker %s] [ALERTA] Timeout na rede. Limpando fila de consenso.\n", b.id)
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	bloco, err := b.chain.ProporBloco(tipoDados, dados, b.id)
	if err != nil {
		return
	}

	b.mu.Lock()
	b.blocosPendentes[bloco.Hash] = bloco
	b.votosBloco[bloco.Hash] = 1
	peers := make(map[string]*connSegura, len(b.connBrokers))
	for id, c := range b.connBrokers {
		peers[id] = c
	}
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
		_ = c.enviar(msg)
	}
}

func (b *Broker) handleNovoBloco(msg protocol.Mensagem) {
	var bloco blockchain.Bloco
	if err := json.Unmarshal([]byte(msg.Payload), &bloco); err != nil {
		return
	}

	ultimoBloco := b.chain.UltimoBloco()
	if bloco.HashAnterior != ultimoBloco.Hash {
		b.solicitarChain()
		return
	}

	b.mu.Lock()
	if _, ok := b.blocosPendentes[bloco.Hash]; !ok {
		b.blocosPendentes[bloco.Hash] = bloco
		// --- CORREÇÃO: Adiciona o voto implícito de quem propôs o bloco ---
		b.votosBloco[bloco.Hash] = 1 
	}
	// Adiciona o nosso próprio voto de aceite
	b.votosBloco[bloco.Hash]++
	votos := b.votosBloco[bloco.Hash]
	total := len(b.connBrokers) + 1
	b.mu.Unlock()

	aceite := protocol.Mensagem{
		Tipo:      protocol.TipoAceiteBloco,
		IDOrigem:  b.id,
		Timestamp: time.Now(),
		Payload:   fmt.Sprintf(`{"hash":"%s"}`, bloco.Hash),
	}

	for _, peerConn := range b.peersAtivos() {
		_ = peerConn.enviar(aceite)
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

	if !existe {
		return
	}

	quorum := total/2 + 1
	if votos >= quorum {
		b.commitarBloco(bloco)
	}
}

func (b *Broker) commitarBloco(bloco blockchain.Bloco) {
	b.mu.Lock()
	if _, pendente := b.blocosPendentes[bloco.Hash]; !pendente {
		b.mu.Unlock()
		return
	}
	delete(b.votosBloco, bloco.Hash)
	delete(b.blocosPendentes, bloco.Hash)
	b.mu.Unlock()

	if err := b.chain.CommitarBloco(bloco); err != nil {
		return
	}

	if bloco.TipoDados == blockchain.TipoBloco_Transacao {
		var tx protocol.Transacao
		if err := json.Unmarshal([]byte(bloco.Dados), &tx); err == nil {
			
			// Débitos (Pagamento padrão de missão)
			if tx.De != "sistema" && tx.De != "" {
				fmt.Printf("[Broker %s] Créditos debitados da companhia: %s\n", b.id, tx.De)
				b.chain.DebitarCreditos(tx.De, tx.Creditos)
			}
			
			// Créditos (Recompensas de Ricart-Agrawala)
			if tx.Para != "sistema" && tx.Para != "" {
				b.chain.CreditarSaldo(tx.Para, tx.Creditos)
			}
		}
	}
}

func (b *Broker) handleReqChain(cs *connSegura) {
	chainJSON, err := b.chain.SerializarChain()
	if err != nil {
		return
	}
	_ = cs.enviar(protocol.Mensagem{
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
	
	// Gera a base de dados dinamicamente usando os países configurados
	saldosBase := gerarSaldosIniciais(b.mapaRede)

	if b.chain.SubstituirChain(blocos, saldosBase) {
		fmt.Printf("[Broker %s] [Chain] Sincronizada: %d blocos\n", b.id, b.chain.Tamanho())
	}
}

func (b *Broker) handleConsultaSaldo(cs *connSegura, msg protocol.Mensagem) {
	saldo := b.chain.ConsultarSaldo(msg.IDOrigem)
	_ = cs.enviar(protocol.Mensagem{
		Tipo:      protocol.TipoRespSaldo,
		IDOrigem:  b.id,
		Timestamp: time.Now(),
		Payload:   fmt.Sprintf(`{"companhia":"%s","saldo":%d}`, msg.IDOrigem, saldo),
	})
}

func (b *Broker) solicitarChain() {
	peers := b.peersAtivos()
	for _, c := range peers {
		_ = c.enviar(protocol.Mensagem{
			Tipo:      protocol.TipoReqChain,
			IDOrigem:  b.id,
			Timestamp: time.Now(),
		})
	}
}

// ============================================================
// HEARTBEAT E MONITORAMENTO DE PEERS
// ============================================================

func (b *Broker) heartbeatLoop() {
	ticker := time.NewTicker(intervaloHeartbeat)
	defer ticker.Stop()

	for range ticker.C {
		peers := b.peersAtivos()
		msg := protocol.Mensagem{
			Tipo:      protocol.TipoHeartbeat,
			IDOrigem:  b.id,
			Timestamp: time.Now(),
		}
		for _, c := range peers {
			_ = c.enviar(msg)
		}
	}
}

func (b *Broker) monitorarPeers() {
	ticker := time.NewTicker(intervaloHeartbeat)
	defer ticker.Stop()

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
				c.fechar()
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

func (b *Broker) removerConexao(conn net.Conn, id string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	delete(b.connDrones, conn)
	delete(b.connBrokers, id)
	delete(b.connClientes, conn)
	delete(b.ultimoHB, id)

	if b.requesting {
		delete(b.respostasOK, id)
		delete(b.deferred, id)
	}
}

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
		c.fechar()
	}
	b.mu.Unlock()
	os.Exit(0)
}

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

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Uso: broker [ID_BROKER]")
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