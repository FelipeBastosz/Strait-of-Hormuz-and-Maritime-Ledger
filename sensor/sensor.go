package main

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"os"
	"strings"
	"time"

	"Strait-of-Hormuz-and-Maritime-Ledger/protocol"
)

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

func main() {
	if len(os.Args) < 4 {
		fmt.Println("Uso: sensor [ID_SENSOR] [ID_SETOR] [ENDERECO_BROKER]")
		return
	}

	sensorID := os.Args[1]
	setorID := os.Args[2]
	enderecoBroker := os.Args[3]

	host := strings.Split(enderecoBroker, ":")[0]
	numBroker := strings.Replace(host, "broker", "", 1)
	paisesDoSetor := obterPaisesPorBroker(numBroker)

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
	fmt.Printf("[Sensor %s] Países operantes no setor: %v\n", sensorID, paisesDoSetor)

	rand.Seed(time.Now().UnixNano())
	contador := 0

	for {
		espera := time.Duration(intervaloMin+rand.Intn(intervaloMax-intervaloMin+1)) * time.Second
		time.Sleep(espera)

		contador++
		prioridade := gerarPrioridade()
		descricao := tiposOcorrencia[rand.Intn(len(tiposOcorrencia))]
		solicitante := paisesDoSetor[rand.Intn(len(paisesDoSetor))]

		ocorrencia := protocol.Ocorrencia{
			ID:          fmt.Sprintf("%s-OC%04d", sensorID, contador),
			Prioridade:  prioridade,
			Timestamp:   time.Now(),
			Descricao:   descricao,
			Setor:       setorID,
			Solicitante: solicitante,
			Creditos:    10, // Custo fixo da escolta marítima
		}

		enviarOcorrencia(enderecoBroker, ocorrencia, sensorID)
	}
}

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
		return
	}

	msg := protocol.Mensagem{
		Tipo:      protocol.TipoOcorrencia,
		IDOrigem:  sensorID,
		Timestamp: time.Now(),
		Payload:   string(payload),
	}

	var conn net.Conn
	tlsCfg := &tls.Config{InsecureSkipVerify: true}
	conn, err = tls.DialWithDialer(&net.Dialer{Timeout: 2 * time.Second}, "tcp", enderecoBroker, tlsCfg)
	if err != nil {
		conn, err = net.DialTimeout("tcp", enderecoBroker, 2*time.Second)
		if err != nil {
			return
		}
	}
	defer conn.Close()

	if err := json.NewEncoder(conn).Encode(msg); err != nil {
		return
	}

	prioLabel := map[int]string{1: "Aviso", 2: "Alerta", 3: "CRÍTICO"}
	fmt.Printf("[Sensor %s | %s] ▶ %s enviada — %s [P%d] | país: %s\n",
		sensorID, oc.Setor, oc.ID, prioLabel[oc.Prioridade], oc.Prioridade, oc.Solicitante)
}