package main

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"

	"Strait-of-Hormuz-and-Maritime-Ledger/protocol"
)

type Drone struct {
	ID       string
	Endereco string
	Status   string
	Bateria  int
	mu       sync.Mutex
	Brokers  []string
}

func main() {
	if len(os.Args) < 4 {
		fmt.Println("Uso: drone [ID] [ENDERECO_PROPRIO] [ENDERECO_BROKER_INICIAL]")
		fmt.Println("Exemplo: drone drone1 drone1:9091 broker1:9081")
		return
	}

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

	go drone.escutar()
	time.Sleep(3 * time.Second)
	drone.registrarNosBrokers()

	select {}
}

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
			// CRÍTICO: Se o SO esgotar recursos (too many open files), evita travar a CPU em 100%
			time.Sleep(100 * time.Millisecond)
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
			time.Sleep(100 * time.Millisecond)
			continue
		}
		go d.processarComando(conn)
	}
}

func (d *Drone) processarComando(conn net.Conn) {
	defer conn.Close()
	dec := json.NewDecoder(conn)
	enc := json.NewEncoder(conn)

	for {
		var msg protocol.Mensagem
		if err := dec.Decode(&msg); err != nil {
			if err != io.EOF {
				// Ignora logs de EOF para não sujar o terminal
			}
			break
		}

		switch msg.Tipo {
		case protocol.TipoComandoDrone:
			d.mu.Lock()
			if d.Status != "disponivel" {
				statusLocal := d.Status
				d.mu.Unlock()

				fmt.Printf("[Drone %s] Recusei missão — status atual: %s\n", d.ID, statusLocal)

				// DEFESA: Rate Limit contra o Broker. Segura a conexão para impedir loops infinitos
				if statusLocal == "recarregando" || statusLocal == "em_missao" {
					time.Sleep(2 * time.Second)
				}

				_ = enc.Encode(map[string]interface{}{"acao": "rejeitado"})
				return
			}
			d.Status = "em_missao"
			d.mu.Unlock()

			_ = enc.Encode(protocol.Mensagem{
				Tipo:      protocol.TipoACK,
				IDOrigem:  d.ID,
				Timestamp: time.Now(),
			})

			var comando protocol.ComandoMissao
			_ = json.Unmarshal([]byte(msg.Payload), &comando)

			fmt.Printf("[Drone %s] ✈  Missão aceita: %s (P%d)\n", d.ID, comando.OcorrenciaID, comando.Prioridade)
			go d.executarMissao(comando)
			return

		case protocol.TipoRegistroDrone:
			go d.registrarNosBrokers()
		}
	}
}

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

	d.mu.Lock()
	if batAtual < 20 {
		d.Status = "recarregando"
		d.mu.Unlock()
		d.reportarLaudo(laudo)
		go d.recarregar()
	} else {
		d.Status = "disponivel"
		d.mu.Unlock()
		d.reportarLaudo(laudo)
	}
}

func (d *Drone) recarregar() {
	fmt.Printf("[Drone %s] ⚡ Bateria baixa. Recarregando (60s)...\n", d.ID)
	time.Sleep(60 * time.Second)

	d.mu.Lock()
	d.Bateria = 100
	d.Status = "disponivel"
	d.mu.Unlock()

	fmt.Printf("[Drone %s] ✅ Recarga completa.\n", d.ID)
	d.registrarNosBrokers()

	// HACK: Envia um laudo fantasma para forçar os Brokers a enxergarem o drone como livre novamente
	d.reportarLaudo(protocol.Laudo{
		MissaoID:  "RECARGA",
		DroneID:   d.ID,
		Resultado: "Bateria recarregada com sucesso",
		Timestamp: time.Now(),
	})
}

func (d *Drone) registrarNosBrokers() {
	info := protocol.InfoConexao{Tipo: "drone", ID: d.ID, Endereco: d.Endereco}
	payload, _ := json.Marshal(info)
	msg := protocol.Mensagem{
		Tipo:      protocol.TipoHandshake,
		IDOrigem:  d.ID,
		Timestamp: time.Now(),
		Payload:   string(payload),
	}

	d.enviarParaTodosBrokers(msg, "Registro")
}

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
				//fmt.Printf("[Drone %s] [%s] Enviado com sucesso para o broker: %s\n", d.ID, contexto, target)
			}
		}(addr)
	}
	wg.Wait()
}

func (d *Drone) tentarEnvio(addr string, msg protocol.Mensagem) bool {
	cfg := &tls.Config{InsecureSkipVerify: true}
	conn, err := tls.DialWithDialer(&net.Dialer{Timeout: 2 * time.Second}, "tcp", addr, cfg)
	if err != nil {
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

func carregarBrokers(caminho string) ([]string, error) {
	arquivo, err := os.ReadFile(caminho)
	if err != nil {
		return nil, err
	}
	mapa := make(map[string]string)
	_ = json.Unmarshal(arquivo, &mapa)

	ids := make([]int, 0)
	for k := range mapa {
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
