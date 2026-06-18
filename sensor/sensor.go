// ============================================================
// SENSOR - Dispositivo de borda (Edge) simulador de eventos
// ============================================================

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

// tiposOcorrencia armazena uma lista de descricoes pre-definidas para simular
// anomalias e chamados de emergencia reais que ocorreriam no Estreito de Ormuz.
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

// obterPaisesPorBroker mapeia dinamicamente os paises (companhias pagantes)
// para um setor especifico, baseado no ID numerico do broker responsavel pela regiao.
// Isso assegura que a solicitacao de drone acionara o debito na carteira correta na Blockchain.
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

// main e o ponto de entrada do sensor IoT.
// Inicializa os parametros de rede, determina as entidades do seu setor
// e entra em um loop infinito gerando carga de processamento para a rede distribuida.
func main() {
    if len(os.Args) < 4 {
        fmt.Println("Uso: sensor [ID_SENSOR] [ID_SETOR] [ENDERECO_BROKER]")
        return
    }

    sensorID := os.Args[1]
    setorID := os.Args[2]
    enderecoBroker := os.Args[3]

    // Extrai o identificador do broker a partir do endereco DNS (ex: "broker3:9083" -> "3")
    host := strings.Split(enderecoBroker, ":")[0]
    numBroker := strings.Replace(host, "broker", "", 1)
    paisesDoSetor := obterPaisesPorBroker(numBroker)

    // Configura os intervalos de disparo. Podem ser sobrescritos pelo docker-compose
    // para modular a frequencia de ataques ou sobrecarga no cluster (Stress Test).
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

    // Loop de Deteccao Continua: Simula o hardware monitorando o oceano 24/7
    for {
        espera := time.Duration(intervaloMin+rand.Intn(intervaloMax-intervaloMin+1)) * time.Second
        time.Sleep(espera)

        contador++
        prioridade := gerarPrioridade()
        descricao := tiposOcorrencia[rand.Intn(len(tiposOcorrencia))]
        
        // Sorteia aleatoriamente qual pais daquele setor esta acionando o pedido (e quem vai pagar)
        solicitante := paisesDoSetor[rand.Intn(len(paisesDoSetor))]

        ocorrencia := protocol.Ocorrencia{
            ID:          fmt.Sprintf("%s-OC%04d", sensorID, contador),
            Prioridade:  prioridade,
            Timestamp:   time.Now(),
            Descricao:   descricao,
            Setor:       setorID,
            Solicitante: solicitante,
            Creditos:    10, // Define o custo transacional que o Broker devera debitar na Chain
        }

        enviarOcorrencia(enderecoBroker, ocorrencia, sensorID)
    }
}

// gerarPrioridade utiliza uma distribuicao probabilistica para definir a urgencia do evento.
// Simula a realidade onde ocorrencias menores sao constantes, mas emergencias sao raras.
func gerarPrioridade() int {
    n := rand.Intn(100)
    if n < 10 {
        return 3 // 10% de chance para Prioridade Maxima (CRITICO)
    } else if n < 40 {
        return 2 // 30% de chance para Prioridade Media (Alerta)
    }
    return 1 // 60% de chance para Prioridade Baixa (Aviso padrao)
}

// enviarOcorrencia encapsula o evento maritimo gerado no protocolo de mensagens do cluster
// e transmite para o Gateway local (o Broker do seu respectivo setor).
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
    
    // Tentativa de conexao hibrida: Tenta fechar o handshake TLS primeiro.
    // Caso falhe (indicando que a rede de teste nao mapeou certificados validos),
    // realiza o fallback elegante para um TCP inseguro padrao.
    tlsCfg := &tls.Config{InsecureSkipVerify: true}
    conn, err = tls.DialWithDialer(&net.Dialer{Timeout: 2 * time.Second}, "tcp", enderecoBroker, tlsCfg)
    if err != nil {
        conn, err = net.DialTimeout("tcp", enderecoBroker, 2*time.Second)
        if err != nil {
            return
        }
    }
    
    // Dispositivos IoT comumente operam no modelo "Fire-and-Forget".
    // Envia a carga util e imediatamente encerra o socket para poupar bateria e recursos.
    defer conn.Close()

    if err := json.NewEncoder(conn).Encode(msg); err != nil {
        return
    }

    prioLabel := map[int]string{1: "Aviso", 2: "Alerta", 3: "CRÍTICO"}
    fmt.Printf("[Sensor %s | %s] ▶ %s enviada — %s [P%d] | país: %s\n",
        sensorID, oc.Setor, oc.ID, prioLabel[oc.Prioridade], oc.Prioridade, oc.Solicitante)
}