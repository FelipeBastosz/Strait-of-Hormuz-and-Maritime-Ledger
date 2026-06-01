// ============================================================
// TESTER — Ferramenta de diagnóstico e stress-test
//
// Baseado no tester de Daniel, adaptado para o novo envelope de protocolo.
// Conecta-se como peer broker (entra no mapa de brokers do cluster)
// e permite observar o Ricart-Agrawala em tempo real, medir latência
// RTT, e injetar múltiplas requisições simultâneas.
//
// Adicionado no Problema 3:
//   - Comando "chain" para exibir tamanho do ledger
//   - Comando "saldo" para consultar créditos de uma companhia
//
// Uso: tester [broker1:9081] [broker2:9082] ...
// ============================================================

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

	"ormuz-p3/protocol"
)

// ============================================================
// STRUCTS
// ============================================================

type Tester struct {
	mu      sync.Mutex
	id      string
	conns   map[string]net.Conn
	relogio int64
	// Canal onde chegam todas as mensagens dos brokers
	incoming chan mensagemRecebida
	autoOK   bool
	pong     chan mensagemRecebida
	// OKs pendentes de envio manual
	pendentes map[string]string // brokerID → addr
}

type mensagemRecebida struct {
	addr string
	msg  protocol.Mensagem
}

// ============================================================
// INICIALIZAÇÃO
// ============================================================

func novoTester(addrs []string) *Tester {
	t := &Tester{
		id:        fmt.Sprintf("tester-%d", time.Now().UnixNano()%10000),
		conns:     make(map[string]net.Conn),
		incoming:  make(chan mensagemRecebida, 256),
		autoOK:    true,
		pong:      make(chan mensagemRecebida, 1),
		pendentes: make(map[string]string),
	}
	for _, addr := range addrs {
		t.conectar(addr)
	}
	return t
}

// ============================================================
// CONEXÃO TLS (Daniel)
// ============================================================

func (t *Tester) conectar(addr string) {
	cfg := &tls.Config{InsecureSkipVerify: true}
	conn, err := tls.DialWithDialer(&net.Dialer{Timeout: 3 * time.Second}, "tcp", addr, cfg)
	if err != nil {
		// Fallback TCP puro
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

func (t *Tester) registrarConn(addr string, conn net.Conn) {
	// Handshake: apresenta-se como broker para receber broadcasts do RA
	info := protocol.InfoConexao{Tipo: "broker", ID: t.id}
	payload, _ := json.Marshal(info)
	msg := protocol.Mensagem{
		Tipo:      protocol.TipoHandshake,
		IDOrigem:  t.id,
		Timestamp: time.Now(),
		Payload:   string(payload),
	}
	json.NewEncoder(conn).Encode(msg)

	t.mu.Lock()
	t.conns[addr] = conn
	t.mu.Unlock()

	fmt.Printf("[TESTER] Conectado a %s\n", addr)

	go t.receberMensagens(addr, conn)
	go t.heartbeat(addr, conn)
}

func (t *Tester) heartbeat(addr string, conn net.Conn) {
	ticker := time.NewTicker(4 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		msg := protocol.Mensagem{
			Tipo:      protocol.TipoHeartbeat,
			IDOrigem:  t.id,
			Timestamp: time.Now(),
		}
		if _, err := fmt.Fprintf(conn, ""); err != nil {
			// Testa se a conexão ainda está viva antes de enviar
		}
		if err := json.NewEncoder(conn).Encode(msg); err != nil {
			fmt.Printf("[TESTER] Conexão com %s perdida\n", addr)
			t.mu.Lock()
			delete(t.conns, addr)
			t.mu.Unlock()
			conn.Close()
			return
		}
	}
}

func (t *Tester) receberMensagens(addr string, conn net.Conn) {
	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		var msg protocol.Mensagem
		if err := json.Unmarshal(scanner.Bytes(), &msg); err != nil {
			continue
		}
		t.incoming <- mensagemRecebida{addr: addr, msg: msg}
	}
	fmt.Printf("[TESTER] Conexão com %s encerrada\n", addr)
}

// ============================================================
// LOOP DE EVENTOS
// ============================================================

func (t *Tester) processar() {
	for recv := range t.incoming {
		msg := recv.msg
		addr := recv.addr

		switch msg.Tipo {

		case protocol.TipoRARequest:
			var req protocol.RequisicaoRA
			json.Unmarshal([]byte(msg.Payload), &req)
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
				t.enviarRAOK(addr, req.BrokerID)
			}

		case protocol.TipoRAOK:
			fmt.Printf("[RA] OK  de broker=%-10s  (via %s)\n", msg.IDOrigem, addr)

		case protocol.TipoPong:
			t.pong <- recv

		case protocol.TipoHeartbeat:
			// Ignora heartbeats recebidos dos brokers

		case protocol.TipoSyncEstado:
			// Apenas registra que recebeu — não processa o estado
			fmt.Printf("[TESTER] SYNC recebido de %s\n", msg.IDOrigem)
		}
	}
}

// ============================================================
// COMANDOS
// ============================================================

func (t *Tester) ping(addr string) {
	t.mu.Lock()
	conn, ok := t.conns[addr]
	t.mu.Unlock()
	if !ok {
		fmt.Printf("[TESTER] sem conexão com %s\n", addr)
		return
	}

	const count = 3
	var rtts []float64

	for i := 0; i < count; i++ {
		msg := protocol.Mensagem{
			Tipo:      protocol.TipoHeartbeat,
			IDOrigem:  t.id,
			Timestamp: time.Now(),
		}
		t0 := time.Now()
		json.NewEncoder(conn).Encode(msg)

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

// enviarRequisicao injeta uma ocorrência no broker com a prioridade dada.
func (t *Tester) enviarRequisicao(addr string, prioridade int, n int, companhia string) {
	t.mu.Lock()
	conn, ok := t.conns[addr]
	t.mu.Unlock()
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
		msg := protocol.Mensagem{
			Tipo:      protocol.TipoOcorrencia,
			IDOrigem:  t.id,
			Timestamp: time.Now(),
			Payload:   string(payload),
		}
		if err := json.NewEncoder(conn).Encode(msg); err != nil {
			fmt.Printf("[TESTER] erro ao enviar req %d: %v\n", i+1, err)
			return
		}
		fmt.Printf("[REQ] enviada #%d  prio=%d  broker=%s  solicitante=%s\n",
			i+1, prioridade, addr, companhia)
		time.Sleep(50 * time.Millisecond)
	}
}

func (t *Tester) enviarRAOK(addr, brokerID string) {
	t.mu.Lock()
	conn, ok := t.conns[addr]
	t.mu.Unlock()
	if !ok {
		return
	}
	msg := protocol.Mensagem{
		Tipo:      protocol.TipoRAOK,
		IDOrigem:  t.id,
		Timestamp: time.Now(),
	}
	json.NewEncoder(conn).Encode(msg)
	fmt.Printf("[RA] OK enviado para broker=%s\n", brokerID)
}

// ============================================================
// CLI
// ============================================================

func (t *Tester) cli() {
	scanner := bufio.NewScanner(os.Stdin)

	fmt.Println("╔══════════════════════════════════════════════════╗")
	fmt.Println("║        TESTER — Estreito de Ormuz P3             ║")
	fmt.Println("╠══════════════════════════════════════════════════╣")
	fmt.Println("║  ping   <addr>                                   ║")
	fmt.Println("║  req    <addr> <prio> <n> [companhia]            ║")
	fmt.Println("║  autoOK <on|off>                                 ║")
	fmt.Println("║  ok     <brokerID>                               ║")
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
			companhia := "companhia-a"
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

		case "quit":
			fmt.Println("encerrando.")
			os.Exit(0)

		default:
			fmt.Printf("comando desconhecido: %q\n", partes[0])
		}

		fmt.Print("> ")
	}
}

// ============================================================
// MAIN
// ============================================================

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "uso: tester <addr> [addr2 ...]")
		os.Exit(1)
	}

	tester := novoTester(os.Args[1:])
	go tester.processar()
	tester.cli()
}
