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

// Drone representa um drone físico de monitoramento focado em comunicação descentralizada.
type Drone struct {
	ID       string
	Endereco string   // Endereço TCP público do próprio drone (ex: "drone1:9091")
	Status   string   // "disponivel" | "em_missao" | "recarregando"
	Bateria  int
	mu       sync.Mutex
	Brokers  []string // Lista completa de brokers do cluster (sem broker principal único)
}

func main() {
	if len(os.Args) < 4 {
		fmt.Println("Uso: drone [ID] [ENDERECO_PROPRIO] [ENDERECO_BROKER_INICIAL]")
		fmt.Println("Exemplo: drone drone1 drone1:9091 broker1:9081")
		return
	}

	// Inicializa a lista de brokers com o broker fornecido por argumento
	brokerInicial := os.Args[3]
	drone := &Drone{
		ID:       os.Args[1],
		Endereco: os.Args[2],
		Status:   "disponivel",
		Bateria:  100,
		Brokers:  []string{brokerInicial},
	}

	configPath := os.Getenv("CONFIG_PATH")
	if configPath == "" {
		configPath = "/app/config.json"
	}
	
	// Carrega os demais brokers do cluster e mescla evitando duplicatas
	if brokersDoCluster, err := carregarBrokers(configPath); err == nil {
		mapaAux := make(map[string]bool)
		mapaAux[brokerInicial] = true
		for _, b := range brokersDoCluster {
			if !mapaAux[b] {
				drone.Brokers = append(drone.Brokers, b)
				mapaAux[b] = true
			}
		}
	}

	rand.Seed(time.Now().UnixNano())

	// Inicializa o servidor interno do drone para receber comandos
	go drone.escutar()

	// Tempo para o servidor estabilizar antes do primeiro registro
	time.Sleep(3 * time.Second)
	
	// Registra a presença em todo o cluster simultaneamente
	drone.registrarNosBrokers()

	// O loop de re-registro periódico foi removido. 
	// O ciclo de vida agora é guiado por eventos e transmissões ativas.
	select {}
}

// ============================================================
// SERVIDOR TLS DO DRONE
// ============================================================

func (d *Drone) escutar() {
	cert, err := tls.LoadX509KeyPair("cert.pem", "key.pem")
	if err != nil {
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

			// ACK imediato ao broker remetente do comando
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
			d.registrarNosBrokers()
		}
	}
}

// ============================================================
// SIMULAÇÃO DE MISSÃO
// ============================================================

func (d *Drone) executarMissao(comando protocol.ComandoMissao) {
	base := map[int]int{1: 5, 2: 8, 3: 12}[comando.Prioridade]
	if base == 0 {
		base = 7
	}
	duracao := time.Duration(base+rand.Intn(8)) * time.Second

	fmt.Printf("[Drone %s] Voando para %s. Duração estimada: %v\n", d.ID, comando.OcorrenciaID, duracao)
	time.Sleep(duracao)

	d.mu.Lock()
	consumo := 8 + rand.Intn(12)
	d.Bateria -= consumo
	if d.Bateria < 0 {
		d.Bateria = 0
	}
	batAtual := d.Bateria
	d.mu.Unlock()

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

	laudo := protocol.Laudo{
		MissaoID:  comando.OcorrenciaID,
		DroneID:   d.ID,
		Resultado: resultado,
		Descricao: comando.Descricao,
		Timestamp: time.Now(),
	}
	
	// Envia o laudo estruturado para todos os brokers
	d.reportarLaudo(laudo)

	d.reportarLaudo(laudo)

	if batAtual < 20 {
		d.recarregar()
	} else {
		d.mu.Lock()
		d.Status = "disponivel"
		d.mu.Unlock()
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
	d.registrarNosBrokers()
}

// ============================================================
// COMUNICAÇÃO TOTALMENTE DESCENTRALIZADA (BROADCAST)
// ============================================================

// registrarNosBrokers envia o handshake de identificação a todos os brokers conhecidos.
func (d *Drone) registrarNosBrokers() {
	info := protocol.InfoConexao{Tipo: "drone", ID: d.ID}
	payload, _ := json.Marshal(info)
	msg := protocol.Mensagem{
		Tipo:      protocol.TipoHandshake,
		IDOrigem:  d.ID,
		Timestamp: time.Now(),
		Payload:   string(payload),
	}
	
	d.enviarParaTodosBrokers(msg, "Registro")
}

// reportarLaudo distribui o laudo estruturado por todo o cluster de brokers.
func (d *Drone) reportarLaudo(laudo protocol.Laudo) {
	payload, _ := json.Marshal(laudo)
	msg := protocol.Mensagem{
		Tipo:      protocol.TipoStatusDrone,
		IDOrigem:  d.ID,
		Timestamp: time.Now(),
		Payload:   string(payload),
	}
	
	d.enviarParaTodosBrokers(msg, "Laudo de Missão")
}

// enviarParaTodosBrokers realiza o disparo em broadcast. 
// Substitui a antiga lógica de fallback e expõe quedas locais de rede/nós.
func (d *Drone) enviarParaTodosBrokers(msg protocol.Mensagem, contexto string) {
	d.mu.Lock()
	listaBrokers := make([]string, len(d.Brokers))
	copy(listaBrokers, d.Brokers)
	d.mu.Unlock()

	var wg sync.WaitGroup
	for _, addr := range listaBrokers {
		wg.Add(1)
		go func(target string) {
			defer wg.Done()
			if d.tentarEnvio(target, msg) {
				fmt.Printf("[Drone %s] [%s] Enviado com sucesso para o broker: %s\n", d.ID, contexto, target)
			} else {
				// Captura da queda de rede ou indisponibilidade do dispositivo broker em tempo de execução
				fmt.Printf("[Drone %s] [%s] FALHA de conexão com o broker: %s (Alerta de desconexão/rede)\n", d.ID, contexto, target)
			}
		}(addr)
	}
	wg.Wait()
}

func (d *Drone) tentarEnvio(addr string, msg protocol.Mensagem) bool {
	cfg := &tls.Config{InsecureSkipVerify: true}
	conn, err := tls.DialWithDialer(&net.Dialer{Timeout: 2 * time.Second}, "tcp", addr, cfg)
	if err != nil {
		// Fallback TCP puro caso o TLS falhe
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
