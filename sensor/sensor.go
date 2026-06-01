// ============================================================
// SENSOR — Detector automático de incidentes
//
// Baseado no sensor de Felipe.
// Gera ocorrências com distribuição realista de prioridades e
// as envia ao broker via TLS. Sensores do Problema 3 preenchem
// também Solicitante e Creditos para acionar a validação da blockchain.
//
// Argumentos: [ID_SENSOR] [ID_SETOR] [ENDERECO_BROKER]
// ============================================================

package main

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"os"
	"time"

	"Strait-of-Hormuz-and-Maritime-Ledger/protocol"
)

// Tipos de ocorrências reais do domínio do Estreito de Ormuz
var tiposOcorrencia = []string{
	"Suspeita de bloqueio parcial de rota",
	"Falha de sinalização marítima",
	"Embarcação civil à deriva",
	"Congestionamento em corredor marítimo",
	"Detecção de objeto não identificado submerso",
	"Inspeção visual urgente de embarcação suspeita",
	"Replanejamento de tráfego — risco ambiental detectado",
	"Embarcação sem transponder AIS ativo",
	"Possível mina à deriva na rota principal",
}

// Companhias cadastradas no ledger — sensors rotacionam entre elas para simular tráfego real
var companhias = []string{
	"companhia-a",
	"companhia-b",
	"companhia-c",
	"companhia-d",
}

func main() {
	if len(os.Args) < 4 {
		fmt.Println("Uso: sensor [ID_SENSOR] [ID_SETOR] [ENDERECO_BROKER]")
		fmt.Println("Exemplo: sensor S1A setor-1 broker1:9081")
		return
	}

	sensorID := os.Args[1]
	setorID := os.Args[2]
	enderecoBroker := os.Args[3]

	intervaloMin := 15
	if v := os.Getenv("SENSOR_INTERVALO_MIN"); v != "" {
		fmt.Sscan(v, &intervaloMin)
	}
	intervaloMax := 20
	if v := os.Getenv("SENSOR_INTERVALO_MAX"); v != "" {
		fmt.Sscan(v, &intervaloMax)
	}

	fmt.Printf("[Sensor %s | Setor %s] Iniciado → broker %s | intervalo %d–%ds\n",
		sensorID, setorID, enderecoBroker, intervaloMin, intervaloMax)

	rand.Seed(time.Now().UnixNano())
	contador := 0

	for {
		espera := time.Duration(intervaloMin+rand.Intn(intervaloMax-intervaloMin+1)) * time.Second
		time.Sleep(espera)

		contador++
		prioridade := gerarPrioridade()
		descricao := tiposOcorrencia[rand.Intn(len(tiposOcorrencia))]
		solicitante := companhias[rand.Intn(len(companhias))]

		ocorrencia := protocol.Ocorrencia{
			ID:          fmt.Sprintf("%s-OC%04d", sensorID, contador),
			Prioridade:  prioridade,
			Timestamp:   time.Now(),
			Descricao:   descricao,
			Setor:       setorID,
			Solicitante: solicitante,
			Creditos:    10, // Custo padrão de escolta
		}

		enviarOcorrencia(enderecoBroker, ocorrencia, sensorID)
	}
}

// gerarPrioridade retorna uma prioridade com distribuição realista:
//
//	60% Aviso (1), 30% Alerta (2), 10% Crítico (3)
func gerarPrioridade() int {
	n := rand.Intn(100)
	if n < 10 {
		return 3
	} else if n < 40 {
		return 2
	}
	return 1
}

func enviarOcorrencia(enderecoBroker string, oc protocol.Ocorrencia, sensorID string) {
	payload, err := json.Marshal(oc)
	if err != nil {
		fmt.Printf("[Sensor %s] Erro ao serializar: %v\n", sensorID, err)
		return
	}

	msg := protocol.Mensagem{
		Tipo:      protocol.TipoOcorrencia,
		IDOrigem:  sensorID,
		Timestamp: time.Now(),
		Payload:   string(payload),
	}

	// Tenta conexão TLS primeiro; fallback para TCP puro em dev local
	var conn net.Conn
	tlsCfg := &tls.Config{InsecureSkipVerify: true}
	conn, err = tls.DialWithDialer(&net.Dialer{Timeout: 2 * time.Second}, "tcp", enderecoBroker, tlsCfg)
	if err != nil {
		conn, err = net.DialTimeout("tcp", enderecoBroker, 2*time.Second)
		if err != nil {
			fmt.Printf("[Sensor %s] Broker %s indisponível: %v\n", sensorID, enderecoBroker, err)
			return
		}
	}
	defer conn.Close()

	if err := json.NewEncoder(conn).Encode(msg); err != nil {
		fmt.Printf("[Sensor %s] Erro ao enviar: %v\n", sensorID, err)
		return
	}

	prioLabel := map[int]string{1: "Aviso", 2: "Alerta", 3: "CRÍTICO"}
	fmt.Printf("[Sensor %s | %s] ▶ %s enviada — %s [P%d] | solicitante: %s\n",
		sensorID, oc.Setor, oc.ID, prioLabel[oc.Prioridade], oc.Prioridade, oc.Solicitante)
}
