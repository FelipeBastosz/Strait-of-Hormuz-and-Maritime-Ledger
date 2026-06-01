// ============================================================
// DRONE — Drone autônomo de monitoramento
//
// Baseado no drone de Felipe, adaptado com:
//   - TLS em todas as conexões TCP (Daniel)
//   - Handshake de identificação ao conectar (Daniel)
//   - Envio de Laudo estruturado ao concluir missão (Problema 3)
//   - Fallback para outros brokers se o principal cair (Felipe)
//   - Recarrega e re-registra automaticamente após bateria baixa (Felipe)
//
// Argumentos: [ID_DRONE] [ENDERECO_PROPRIO] [ENDERECO_BROKER]
// Exemplo:    drone1 drone1:9091 broker1:9081
// ============================================================

package main

import (
	"bufio"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"

	"Strait-of-Hormuz-and-Maritime-Ledger/protocol"
)

// Drone representa um drone físico de monitoramento.
type Drone struct {
	ID             string
	Endereco       string // Endereço TCP público (ex: "drone1:9091")
	EnderecoBroker string // Broker principal atual
	Status         string // "disponivel" | "em_missao" | "recarregando"
	Bateria        int
	mu             sync.Mutex
	Brokers        []string // Lista de fallback (todos os brokers do cluster)
}

func main() {
	if len(os.Args) < 4 {
		fmt.Println("Uso: drone [ID] [ENDERECO_PROPRIO] [ENDERECO_BROKER]")
		fmt.Println("Exemplo: drone drone1 drone1:9091 broker1:9081")
		return
	}

	drone := &Drone{
		ID:             os.Args[1],
		Endereco:       os.Args[2],
		EnderecoBroker: os.Args[3],
		Status:         "disponivel",
		Bateria:        100,
	}

	configPath := os.Getenv("CONFIG_PATH")
	if configPath == "" {
		configPath = "/app/config.json"
	}
	if brokers, err := carregarBrokers(configPath); err == nil {
		drone.Brokers = brokers
	}

	rand.Seed(time.Now().UnixNano())

	go drone.escutar()

	time.Sleep(3 * time.Second)
	drone.registrarNoBroker()

	go drone.reregistroPeriodico()

	select {}
}

// ============================================================
// SERVIDOR TLS DO DRONE
// ============================================================

func (d *Drone) escutar() {
	cert, err := tls.LoadX509KeyPair("cert.pem", "key.pem")
	if err != nil {
		// Fallback sem TLS para desenvolvimento local fora do Docker
		fmt.Printf("[Drone %s] Certificados TLS não encontrados, usando TCP puro\n", d.ID)
		d.escutarTCP()
		return
	}

	cfg := &tls.Config{Certificates: []tls.Certificate{cert}}
	_, porta, _ := net.SplitHostPort(d.Endereco)

	ln, err := tls.Listen("tcp", "0.0.0.0:"+porta, cfg)
	if err != nil {
		fmt.Printf("[Drone %s] Erro ao abrir porta %s: %v\n", d.ID, porta, err)
		os.Exit(1)
	}
	fmt.Printf("[Drone %s] Pronto em 0.0.0.0:%s (TLS)\n", d.ID, porta)

	for {
		conn, err := ln.Accept()
		if err != nil {
			continue
		}
		go d.processarComando(conn)
	}
}

func (d *Drone) escutarTCP() {
	_, porta, _ := net.SplitHostPort(d.Endereco)
	ln, err := net.Listen("tcp", "0.0.0.0:"+porta)
	if err != nil {
		fmt.Printf("[Drone %s] Erro ao abrir porta %s: %v\n", d.ID, porta, err)
		os.Exit(1)
	}
	fmt.Printf("[Drone %s] Pronto em 0.0.0.0:%s (TCP)\n", d.ID, porta)

	for {
		conn, err := ln.Accept()
		if err != nil {
			continue
		}
		go d.processarComando(conn)
	}
}

// ============================================================
// PROCESSAMENTO DE COMANDOS
// ============================================================

func (d *Drone) processarComando(conn net.Conn) {
	defer conn.Close()
	scanner := bufio.NewScanner(conn)

	for scanner.Scan() {
		var msg protocol.Mensagem
		if err := json.Unmarshal(scanner.Bytes(), &msg); err != nil {
			continue
		}

		switch msg.Tipo {

		case protocol.TipoComandoDrone:
			d.mu.Lock()
			if d.Status != "disponivel" {
				fmt.Printf("[Drone %s] Recusei missão — status: %s\n", d.ID, d.Status)
				d.mu.Unlock()
				return
			}
			d.Status = "em_missao"
			d.mu.Unlock()

			// ACK imediato ao broker
			ack := protocol.Mensagem{
				Tipo:      protocol.TipoACK,
				IDOrigem:  d.ID,
				Timestamp: time.Now(),
			}
			json.NewEncoder(conn).Encode(ack)

			var comando protocol.ComandoMissao
			json.Unmarshal([]byte(msg.Payload), &comando)

			fmt.Printf("[Drone %s] ✈  Missão aceita: %s (P%d)\n",
				d.ID, comando.OcorrenciaID, comando.Prioridade)

			go d.executarMissao(comando)

		case protocol.TipoRegistroDrone:
			d.registrarNoBroker()
		}
	}
}

// ============================================================
// SIMULAÇÃO DE MISSÃO
// ============================================================

func (d *Drone) executarMissao(comando protocol.ComandoMissao) {
	// Duração proporcional à prioridade (missões críticas são mais longas)
	base := map[int]int{1: 5, 2: 8, 3: 12}[comando.Prioridade]
	if base == 0 {
		base = 7
	}
	duracao := time.Duration(base+rand.Intn(8)) * time.Second

	fmt.Printf("[Drone %s] Voando para %s. Duração estimada: %v\n",
		d.ID, comando.OcorrenciaID, duracao)
	time.Sleep(duracao)

	// Consome bateria
	d.mu.Lock()
	consumo := 8 + rand.Intn(12) // 8–20% por missão
	d.Bateria -= consumo
	if d.Bateria < 0 {
		d.Bateria = 0
	}
	batAtual := d.Bateria
	d.mu.Unlock()

	// Resultados possíveis da missão
	resultados := []string{
		"rota segura",
		"obstáculo detectado",
		"embarcação suspeita identificada",
		"sinalização marítima com falha",
		"rota liberada após inspeção",
		"objeto submerso não identificado",
	}
	resultado := resultados[rand.Intn(len(resultados))]

	fmt.Printf("[Drone %s] ✅ Missão %s concluída. Resultado: %s | Bateria: %d%%\n",
		d.ID, comando.OcorrenciaID, resultado, batAtual)

	// Reporta laudo (gera bloco na blockchain via broker)
	laudo := protocol.Laudo{
		MissaoID:  comando.OcorrenciaID,
		DroneID:   d.ID,
		Resultado: resultado,
		Descricao: comando.Descricao,
		Timestamp: time.Now(),
	}
	d.reportarLaudo(laudo)

	if batAtual < 20 {
		d.recarregar()
	} else {
		d.mu.Lock()
		d.Status = "disponivel"
		d.mu.Unlock()
		d.registrarNoBroker()
	}
}

func (d *Drone) recarregar() {
	fmt.Printf("[Drone %s] ⚡ Bateria baixa. Recarregando (60s)...\n", d.ID)
	d.mu.Lock()
	d.Status = "recarregando"
	d.mu.Unlock()

	time.Sleep(60 * time.Second)

	d.mu.Lock()
	d.Bateria = 100
	d.Status = "disponivel"
	d.mu.Unlock()

	fmt.Printf("[Drone %s] ✅ Recarga completa.\n", d.ID)
	d.registrarNoBroker()
}

// ============================================================
// COMUNICAÇÃO COM O BROKER
// ============================================================

// registrarNoBroker envia handshake de identificação ao broker (Daniel).
func (d *Drone) registrarNoBroker() {
	info := protocol.InfoConexao{Tipo: "drone", ID: d.ID}
	payload, _ := json.Marshal(info)
	msg := protocol.Mensagem{
		Tipo:      protocol.TipoHandshake,
		IDOrigem:  d.ID,
		Timestamp: time.Now(),
		Payload:   string(payload),
	}
	if d.enviarParaBroker(msg) {
		fmt.Printf("[Drone %s] Registrado no broker %s\n", d.ID, d.EnderecoBroker)
	}
}

// reportarLaudo envia o laudo estruturado ao broker para registro na blockchain.
func (d *Drone) reportarLaudo(laudo protocol.Laudo) {
	payload, _ := json.Marshal(laudo)
	msg := protocol.Mensagem{
		Tipo:      protocol.TipoStatusDrone,
		IDOrigem:  d.ID,
		Timestamp: time.Now(),
		Payload:   string(payload),
	}
	d.enviarParaBroker(msg)
}

func (d *Drone) reregistroPeriodico() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		d.mu.Lock()
		status := d.Status
		d.mu.Unlock()
		if status == "disponivel" {
			d.registrarNoBroker()
		}
	}
}

// enviarParaBroker tenta o broker principal; em caso de falha, percorre a lista.
func (d *Drone) enviarParaBroker(msg protocol.Mensagem) bool {
	if d.tentarEnvio(d.EnderecoBroker, msg) {
		return true
	}
	for _, addr := range d.Brokers {
		if addr == d.EnderecoBroker {
			continue
		}
		if d.tentarEnvio(addr, msg) {
			d.EnderecoBroker = addr
			return true
		}
	}
	return false
}

func (d *Drone) tentarEnvio(addr string, msg protocol.Mensagem) bool {
	cfg := &tls.Config{InsecureSkipVerify: true}
	conn, err := tls.DialWithDialer(&net.Dialer{Timeout: 2 * time.Second}, "tcp", addr, cfg)
	if err != nil {
		// Fallback TCP puro se TLS falhar
		conn2, err2 := net.DialTimeout("tcp", addr, 2*time.Second)
		if err2 != nil {
			return false
		}
		defer conn2.Close()
		return json.NewEncoder(conn2).Encode(msg) == nil
	}
	defer conn.Close()
	return json.NewEncoder(conn).Encode(msg) == nil
}

// ============================================================
// CONFIGURAÇÃO
// ============================================================

func carregarBrokers(caminho string) ([]string, error) {
	arquivo, err := os.ReadFile(caminho)
	if err != nil {
		return nil, err
	}
	mapa := make(map[string]string)
	json.Unmarshal(arquivo, &mapa)

	ids := make([]int, 0)
	for k := range mapa {
		// Suporta tanto "1" quanto "broker1" como chaves
		n := 0
		fmt.Sscanf(k, "broker%d", &n)
		if n == 0 {
			n, _ = strconv.Atoi(k)
		}
		if n > 0 {
			ids = append(ids, n)
		}
	}
	sort.Ints(ids)

	brokers := make([]string, 0)
	for _, id := range ids {
		if v, ok := mapa[fmt.Sprintf("broker%d", id)]; ok {
			brokers = append(brokers, v)
		} else if v, ok := mapa[strconv.Itoa(id)]; ok {
			brokers = append(brokers, v)
		}
	}
	return brokers, nil
}
