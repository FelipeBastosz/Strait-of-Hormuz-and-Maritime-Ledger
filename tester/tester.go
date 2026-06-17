package main

import (
	"bufio"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"Strait-of-Hormuz-and-Maritime-Ledger/blockchain"
	"Strait-of-Hormuz-and-Maritime-Ledger/protocol"
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

type Tester struct {
	mu        sync.Mutex
	id        string
	conns     map[string]*connSegura
	relogio   int64
	incoming  chan mensagemRecebida
	autoOK    bool
	pong      chan mensagemRecebida
	pendentes map[string]string

	watchBloco bool
	respChain  chan mensagemRecebida
	respSaldo  chan mensagemRecebida
}

type mensagemRecebida struct {
	addr string
	msg  protocol.Mensagem
}

func novoTester(addrs []string) *Tester {
	t := &Tester{
		id:         fmt.Sprintf("tester-%d", time.Now().UnixNano()%10000),
		conns:      make(map[string]*connSegura),
		incoming:   make(chan mensagemRecebida, 256),
		autoOK:     true,
		pong:       make(chan mensagemRecebida, 1),
		pendentes:  make(map[string]string),
		respChain:  make(chan mensagemRecebida, 4),
		respSaldo:  make(chan mensagemRecebida, 4),
		watchBloco: false,
	}
	for _, addr := range addrs {
		t.conectar(addr)
	}
	return t
}

func (t *Tester) conectar(addr string) {
	cfg := &tls.Config{InsecureSkipVerify: true}
	conn, err := tls.DialWithDialer(&net.Dialer{Timeout: 3 * time.Second}, "tcp", addr, cfg)
	if err != nil {
		conn2, err2 := net.DialTimeout("tcp", addr, 3*time.Second)
		if err2 != nil {
			fmt.Printf("[TESTER] Não foi possível conectar a %s: %v\n", addr, err2)
			return
		}
		t.registrarConn(addr, conn2)
		return
	}
	t.registrarConn(addr, conn)
}

func (t *Tester) registrarConn(addr string, rawConn net.Conn) {
	cs := novaConnSegura(rawConn)

	info := protocol.InfoConexao{Tipo: "broker", ID: t.id}
	payload, _ := json.Marshal(info)
	_ = cs.enviar(protocol.Mensagem{
		Tipo:      protocol.TipoHandshake,
		IDOrigem:  t.id,
		Timestamp: time.Now(),
		Payload:   string(payload),
	})

	t.mu.Lock()
	t.conns[addr] = cs
	t.mu.Unlock()

	fmt.Printf("[TESTER] Conectado a %s\n", addr)

	go t.receberMensagens(addr, cs)
	go t.heartbeat(addr, cs)
}

func (t *Tester) heartbeat(addr string, cs *connSegura) {
	ticker := time.NewTicker(4 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		err := cs.enviar(protocol.Mensagem{
			Tipo:      protocol.TipoHeartbeat,
			IDOrigem:  t.id,
			Timestamp: time.Now(),
		})
		if err != nil {
			fmt.Printf("[TESTER] Conexão com %s perdida\n", addr)
			t.mu.Lock()
			delete(t.conns, addr)
			t.mu.Unlock()
			cs.fechar()
			return
		}
	}
}

// CORRIGIDO: Usa Decoder para não comer o arquivo inteiro do RespChain no Buffer
func (t *Tester) receberMensagens(addr string, cs *connSegura) {
	dec := json.NewDecoder(cs.conn)
	for {
		var msg protocol.Mensagem
		if err := dec.Decode(&msg); err != nil {
			break
		}
		t.incoming <- mensagemRecebida{addr: addr, msg: msg}
	}
	fmt.Printf("[TESTER] Conexão com %s encerrada\n", addr)
}

func (t *Tester) processar() {
	for recv := range t.incoming {
		msg := recv.msg
		addr := recv.addr

		switch msg.Tipo {
		case protocol.TipoRARequest:
			var req protocol.RequisicaoRA
			_ = json.Unmarshal([]byte(msg.Payload), &req)
			fmt.Printf("\n[RA] REQUEST  broker=%-10s  prio=%-3d  relógio=%-8d  origem=%s  (via %s)\n",
				req.BrokerID, req.Prioridade, req.Relogio, req.Origem, addr)

			t.mu.Lock()
			auto := t.autoOK
			if !auto {
				t.pendentes[req.BrokerID] = addr
				fmt.Printf("[RA] REQUEST pendente — use: ok %s\n", req.BrokerID)
			}
			t.mu.Unlock()

			if auto {
				go t.enviarRAOK(addr, req.BrokerID)
			}

		case protocol.TipoRAOK:
			fmt.Printf("[RA] OK  de broker=%-10s  (via %s)\n", msg.IDOrigem, addr)

		case protocol.TipoPong:
			select {
			case t.pong <- recv:
			default:
			}

		case protocol.TipoHeartbeat:
			t.mu.Lock()
			cs, ok := t.conns[addr]
			t.mu.Unlock()
			if ok {
				go cs.enviar(protocol.Mensagem{
					Tipo:      protocol.TipoPong,
					IDOrigem:  t.id,
					Timestamp: time.Now(),
				})
			}

		case protocol.TipoSyncEstado:
			fmt.Printf("[TESTER] SYNC recebido de %s\n", msg.IDOrigem)

		case protocol.TipoNovoBloco:
			t.mu.Lock()
			watch := t.watchBloco
			t.mu.Unlock()
			if watch {
				var bloco blockchain.Bloco
				if err := json.Unmarshal([]byte(msg.Payload), &bloco); err == nil {
					hashCurto := bloco.Hash
					if len(hashCurto) > 12 {
						hashCurto = hashCurto[:12] + "…"
					}
					fmt.Printf("\n[CHAIN] NOVO_BLOCO  #%-4d  tipo=%-12s  hash=%s  validador=%s\n",
						bloco.Indice, bloco.TipoDados, hashCurto, bloco.Validador)
				}
			}

		case protocol.TipoAceiteBloco:
			t.mu.Lock()
			watch := t.watchBloco
			t.mu.Unlock()
			if watch {
				var voto struct {
					Hash string `json:"hash"`
				}
				if err := json.Unmarshal([]byte(msg.Payload), &voto); err == nil {
					hashCurto := voto.Hash
					if len(hashCurto) > 12 {
						hashCurto = hashCurto[:12] + "…"
					}
					fmt.Printf("[CHAIN] ACEITE      hash=%s  votante=%s\n",
						hashCurto, msg.IDOrigem)
				}
			}

		case protocol.TipoRespChain:
			select {
			case t.respChain <- recv:
			default:
			}

		case protocol.TipoRespSaldo:
			select {
			case t.respSaldo <- recv:
			default:
			}
		}
	}
}

func (t *Tester) connPara(addr string) (*connSegura, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	cs, ok := t.conns[addr]
	return cs, ok
}

func (t *Tester) ping(addr string) {
	cs, ok := t.connPara(addr)
	if !ok {
		fmt.Printf("[TESTER] sem conexão com %s\n", addr)
		return
	}

	for len(t.pong) > 0 {
		<-t.pong
	}

	const count = 3
	var rtts []float64

	for i := 0; i < count; i++ {
		t0 := time.Now()
		_ = cs.enviar(protocol.Mensagem{
			Tipo:      protocol.TipoHeartbeat,
			IDOrigem:  t.id,
			Timestamp: time.Now(),
		})

		select {
		case <-t.pong:
			rtt := float64(time.Since(t0).Microseconds()) / 1000.0
			rtts = append(rtts, rtt)
			fmt.Printf("[PING] %s: seq=%d time=%.3f ms\n", addr, i+1, rtt)
		case <-time.After(3 * time.Second):
			fmt.Printf("[PING] %s: seq=%d timeout\n", addr, i+1)
		}
		time.Sleep(time.Second)
	}

	if len(rtts) > 0 {
		min, max, sum := rtts[0], rtts[0], 0.0
		for _, r := range rtts {
			if r < min {
				min = r
			}
			if r > max {
				max = r
			}
			sum += r
		}
		avg := sum / float64(len(rtts))
		fmt.Printf("--- %s ping statistics: min=%.3f avg=%.3f max=%.3f ms ---\n",
			addr, min, avg, max)
	}
}

func (t *Tester) enviarRequisicao(addr string, prioridade int, n int, companhia string) {
	cs, ok := t.connPara(addr)
	if !ok {
		fmt.Printf("[TESTER] sem conexão com %s\n", addr)
		return
	}

	for i := 0; i < n; i++ {
		t.mu.Lock()
		t.relogio++
		ts := t.relogio
		t.mu.Unlock()

		oc := protocol.Ocorrencia{
			ID:          fmt.Sprintf("TST-%s-OC%04d", t.id, ts),
			Prioridade:  prioridade,
			Timestamp:   time.Now(),
			Descricao:   fmt.Sprintf("Requisição de teste #%d", ts),
			Setor:       "tester",
			Solicitante: companhia,
			Creditos:    10,
		}
		payload, _ := json.Marshal(oc)
		if err := cs.enviar(protocol.Mensagem{
			Tipo:      protocol.TipoOcorrencia,
			IDOrigem:  t.id,
			Timestamp: time.Now(),
			Payload:   string(payload),
		}); err != nil {
			fmt.Printf("[TESTER] erro ao enviar req %d: %v\n", i+1, err)
			return
		}
		fmt.Printf("[REQ] enviada #%d  prio=%d  broker=%s  solicitante=%s\n",
			i+1, prioridade, addr, companhia)
			
		// 🐌 DESACELERADOR: 2 segundos de respiro para visualizar os consensos e a decolagem.
		time.Sleep(2000 * time.Millisecond)
	}
}
		
func (t *Tester) enviarRAOK(addr, brokerID string) {
	cs, ok := t.connPara(addr)
	if !ok {
		return
	}
	_ = cs.enviar(protocol.Mensagem{
		Tipo:      protocol.TipoRAOK,
		IDOrigem:  t.id,
		Timestamp: time.Now(),
	})
	fmt.Printf("[RA] OK enviado para broker=%s\n", brokerID)
}

func (t *Tester) consultarChain(addr string) {
	cs, ok := t.connPara(addr)
	if !ok {
		fmt.Printf("[TESTER] sem conexão com %s\n", addr)
		return
	}

	for len(t.respChain) > 0 {
		<-t.respChain
	}

	if err := cs.enviar(protocol.Mensagem{
		Tipo:      protocol.TipoReqChain,
		IDOrigem:  t.id,
		Timestamp: time.Now(),
	}); err != nil {
		fmt.Printf("[TESTER] erro ao solicitar chain: %v\n", err)
		return
	}

	select {
	case recv := <-t.respChain:
		t.exibirChain(recv.msg)
	case <-time.After(5 * time.Second):
		fmt.Println("[CHAIN] timeout aguardando resposta do broker")
	}
}

func (t *Tester) exibirChain(msg protocol.Mensagem) {
	var blocos []blockchain.Bloco
	if err := json.Unmarshal([]byte(msg.Payload), &blocos); err != nil {
		fmt.Printf("[CHAIN] Erro ao decodificar chain: %v\n", err)
		return
	}

	fmt.Printf("\n╔══════════════════════════════════════════════════════════════════╗\n")
	fmt.Printf("║  BLOCKCHAIN — %d bloco(s)  (broker=%s)\n", len(blocos), msg.IDOrigem)
	fmt.Printf("╠══════════════════════════════════════════════════════════════════╣\n")

	for _, b := range blocos {
		hashCurto := b.Hash
		if len(hashCurto) > 16 {
			hashCurto = hashCurto[:16] + "…"
		}
		hashAnterior := b.HashAnterior
		if len(hashAnterior) > 16 {
			hashAnterior = hashAnterior[:16] + "…"
		}

		fmt.Printf("║  #%-4d  tipo=%-14s  validador=%-10s\n",
			b.Indice, b.TipoDados, b.Validador)
		fmt.Printf("║         hash=%s  anterior=%s\n", hashCurto, hashAnterior)
		fmt.Printf("║         ts=%s\n", b.Timestamp.Format("2006-01-02 15:04:05.000"))

		switch b.TipoDados {
		case blockchain.TipoBloco_Transacao:
			var tx protocol.Transacao
			if err := json.Unmarshal([]byte(b.Dados), &tx); err == nil {
				fmt.Printf("║         tx: %s → %s  créditos=%d  oc=%s\n",
					tx.De, tx.Para, tx.Creditos, tx.OcorrenciaID)
			}
		case blockchain.TipoBloco_Laudo:
			var laudo protocol.Laudo
			if err := json.Unmarshal([]byte(b.Dados), &laudo); err == nil {
				fmt.Printf("║         laudo: missão=%s  drone=%s  resultado=%s\n",
					laudo.MissaoID, laudo.DroneID, laudo.Resultado)
			}
		case blockchain.TipoBloco_Genesis:
			fmt.Printf("║         (bloco gênese)\n")
		default:
			dados := b.Dados
			if len(dados) > 60 {
				dados = dados[:60] + "…"
			}
			fmt.Printf("║         dados: %s\n", dados)
		}
		fmt.Printf("╠══════════════════════════════════════════════════════════════════╣\n")
	}
	fmt.Printf("╚══════════════════════════════════════════════════════════════════╝\n")
}

func (t *Tester) consultarSaldo(addr, companhia string) {
	cs, ok := t.connPara(addr)
	if !ok {
		fmt.Printf("[TESTER] sem conexão com %s\n", addr)
		return
	}

	for len(t.respSaldo) > 0 {
		<-t.respSaldo
	}

	if err := cs.enviar(protocol.Mensagem{
		Tipo:      protocol.TipoConsultaSaldo,
		IDOrigem:  companhia,
		Timestamp: time.Now(),
	}); err != nil {
		fmt.Printf("[TESTER] erro ao solicitar saldo: %v\n", err)
		return
	}

	select {
	case recv := <-t.respSaldo:
		var payload struct {
			Companhia string `json:"companhia"`
			Saldo     int    `json:"saldo"`
		}
		if err := json.Unmarshal([]byte(recv.msg.Payload), &payload); err != nil {
			fmt.Printf("[SALDO] Erro ao decodificar resposta: %v\n", err)
			return
		}
		fmt.Printf("[SALDO] %-15s  créditos=%d  (broker=%s)\n",
			payload.Companhia, payload.Saldo, recv.msg.IDOrigem)
	case <-time.After(5 * time.Second):
		fmt.Println("[SALDO] timeout aguardando resposta do broker")
	}
}

func (t *Tester) cli() {
	scanner := bufio.NewScanner(os.Stdin)

	fmt.Println("╔══════════════════════════════════════════════════╗")
	fmt.Println("║        TESTER — Estreito de Ormuz P3             ║")
	fmt.Println("╠══════════════════════════════════════════════════╣")
	fmt.Println("║  ping   <addr>                                   ║")
	fmt.Println("║  req    <addr> <prio> <n> [companhia]            ║")
	fmt.Println("║  autoOK <on|off>                                 ║")
	fmt.Println("║  ok     <brokerID>                               ║")
	fmt.Println("║  chain  <addr>                                   ║")
	fmt.Println("║  saldo  <addr> <companhia>                       ║")
	fmt.Println("║  watch  <on|off>                                 ║")
	fmt.Println("║  quit                                            ║")
	fmt.Println("╚══════════════════════════════════════════════════╝")
	fmt.Print("> ")

	for scanner.Scan() {
		partes := strings.Fields(strings.TrimSpace(scanner.Text()))
		if len(partes) == 0 {
			fmt.Print("> ")
			continue
		}

		switch partes[0] {
		case "ping":
			if len(partes) < 2 {
				fmt.Println("uso: ping <addr>")
				break
			}
			go t.ping(partes[1])

		case "req":
			if len(partes) < 4 {
				fmt.Println("uso: req <addr> <prio> <n> [companhia]")
				break
			}
			addr := partes[1]
			prio, _ := strconv.Atoi(partes[2])
			n, _ := strconv.Atoi(partes[3])
			
			// 🌍 NOME PADRÃO AJUSTADO AQUI:
			companhia := "b1-alemanha"
			if len(partes) >= 5 {
				companhia = partes[4]
			}
			if prio < 1 || prio > 3 || n <= 0 {
				fmt.Println("prio deve ser 1–3 e n deve ser positivo")
				break
			}
			go t.enviarRequisicao(addr, prio, n, companhia)

		case "autoOK":
			if len(partes) < 2 {
				fmt.Println("uso: autoOK <on|off>")
				break
			}
			t.mu.Lock()
			switch partes[1] {
			case "on":
				t.autoOK = true
				fmt.Println("[TESTER] modo automático ativado")
			case "off":
				t.autoOK = false
				fmt.Println("[TESTER] modo manual — use: ok <brokerID>")
			}
			t.mu.Unlock()

		case "ok":
			if len(partes) < 2 {
				fmt.Println("uso: ok <brokerID>")
				break
			}
			brokerID := partes[1]
			t.mu.Lock()
			addr, existe := t.pendentes[brokerID]
			if existe {
				delete(t.pendentes, brokerID)
			}
			t.mu.Unlock()
			if !existe {
				fmt.Printf("[TESTER] nenhum REQUEST pendente de %s\n", brokerID)
				break
			}
			go t.enviarRAOK(addr, brokerID)

		case "chain":
			if len(partes) < 2 {
				fmt.Println("uso: chain <addr>")
				break
			}
			go t.consultarChain(partes[1])

		case "saldo":
			if len(partes) < 3 {
				fmt.Println("uso: saldo <addr> <companhia>")
				break
			}
			go t.consultarSaldo(partes[1], partes[2])

		case "watch":
			if len(partes) < 2 {
				fmt.Println("uso: watch <on|off>")
				break
			}
			t.mu.Lock()
			switch partes[1] {
			case "on":
				t.watchBloco = true
				fmt.Println("[CHAIN] monitoramento de consenso ativado")
			case "off":
				t.watchBloco = false
				fmt.Println("[CHAIN] monitoramento de consenso desativado")
			default:
				fmt.Println("uso: watch <on|off>")
			}
			t.mu.Unlock()

		case "quit":
			fmt.Println("encerrando.")
			os.Exit(0)

		default:
			fmt.Printf("comando desconhecido: %q\n", partes[0])
		}

		fmt.Print("> ")
	}
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "uso: tester <addr> [addr2 ...]")
		os.Exit(1)
	}

	tester := novoTester(os.Args[1:])
	go tester.processar()
	tester.cli()
}
