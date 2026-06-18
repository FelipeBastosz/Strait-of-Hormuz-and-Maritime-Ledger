// ============================================================
// DRONE — Atuador IoT do cluster descentralizado
// ============================================================

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

// Drone representa a entidade fisica simulada na rede.
// Gerencia seu proprio estado de bateria, disponibilidade e as
// conexoes de rede necessarias para reportar status aos Brokers.
type Drone struct {
    ID       string
    Endereco string
    Status   string
    Bateria  int
    mu       sync.Mutex // Protege acessos concorrentes ao Status e Bateria
    Brokers  []string   // Lista de enderecos dos brokers conhecidos
}

// main e o ponto de entrada do processo do Drone.
// Configura as portas locais, descobre a rede de brokers via arquivo de configuracao,
// e inicia as rotinas de escuta e registro assincrono.
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

    // Tenta ler o arquivo de configuracao para popular a frota com todos os brokers.
    // O mapa auxiliar evita que o broker inicial seja duplicado na lista.
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

    // Inicia o servidor TCP em background para aceitar comandos dos brokers
    go drone.escutar()
    time.Sleep(3 * time.Second)
    
    // Anuncia sua existencia para todos os nos da rede
    drone.registrarNosBrokers()

    // Trava a main thread infinitamente para manter o container vivo
    select {}
}

// escutar inicializa um servidor seguro via TLS. 
// Caso os certificados nao existam no ambiente, ele realiza o fallback para TCP puro.
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
            // DEFESA: Se o sistema operacional esgotar os file descriptors (too many open files),
            // essa pausa evita que o laco infinito consuma 100% de CPU.
            time.Sleep(100 * time.Millisecond)
            continue
        }
        go d.processarComando(conn)
    }
}

// escutarTCP e a funcao de contingencia acionada pela falta de certificados.
// Inicia um listener socket TCP padrao na porta designada.
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

// processarComando e engatilhado a cada nova conexao de um Broker local ou remoto.
// Avalia mensagens JSON de forma continua ate que a conexao caia.
func (d *Drone) processarComando(conn net.Conn) {
    defer conn.Close()
    dec := json.NewDecoder(conn)
    enc := json.NewEncoder(conn)

    for {
        var msg protocol.Mensagem
        if err := dec.Decode(&msg); err != nil {
            if err != io.EOF {
                // Ignora logs de fechamento de conexao esperados (EOF)
            }
            break
        }

        switch msg.Tipo {
        case protocol.TipoComandoDrone:
            d.mu.Lock()
            
            // Verifica a disponibilidade do drone e trata concorrencia com missoes em paralelo
            if d.Status != "disponivel" {
                statusLocal := d.Status
                d.mu.Unlock()

                fmt.Printf("[Drone %s] Recusei missão — status atual: %s\n", d.ID, statusLocal)

                // DEFESA: Rate limit contra Brokers. Se o drone estiver ocupado, retarda a rejeicao
                // para evitar um loop hiperativo de retentativas na rede, acalmando o broker solicitante.
                if statusLocal == "recarregando" || statusLocal == "em_missao" {
                    time.Sleep(2 * time.Second)
                }

                _ = enc.Encode(map[string]interface{}{"acao": "rejeitado"})
                return
            }
            
            // Marca o drone como ocupado garantindo que novos pedidos sofram rejeicao
            d.Status = "em_missao"
            d.mu.Unlock()

            // Retorna ACK informando que decolou com sucesso
            _ = enc.Encode(protocol.Mensagem{
                Tipo:      protocol.TipoACK,
                IDOrigem:  d.ID,
                Timestamp: time.Now(),
            })

            var comando protocol.ComandoMissao
            _ = json.Unmarshal([]byte(msg.Payload), &comando)

            fmt.Printf("[Drone %s] ✈  Missão aceita: %s (P%d)\n", d.ID, comando.OcorrenciaID, comando.Prioridade)
            
            // Desvia a execucao fisica da missao para uma rotina assincrona,
            // permitindo que este handler volte a ouvir novos pacotes rapidamente.
            go d.executarMissao(comando)
            return

        case protocol.TipoRegistroDrone:
            // Comando explicito para forcar um re-anuncio a frota
            go d.registrarNosBrokers()
        }
    }
}

// executarMissao simula o tempo de voo, execucao da tarefa fisica e a degradacao da bateria.
// O tempo de missao escala de acordo com o nivel de prioridade (quanto menor o nivel, mais longa a missao).
func (d *Drone) executarMissao(comando protocol.ComandoMissao) {
    // Calculo do tempo base de voo (P1: 5s, P2: 8s, P3: 12s)
    base := map[int]int{1: 5, 2: 8, 3: 12}[comando.Prioridade]
    if base == 0 {
        base = 7
    }
    
    // Adiciona entropia (jitter) simulando ventos, transito aereo, etc
    duracao := time.Duration(base+rand.Intn(8)) * time.Second

    fmt.Printf("[Drone %s] Voando para %s. Duração estimada: %v\n", d.ID, comando.OcorrenciaID, duracao)
    time.Sleep(duracao)

    d.mu.Lock()
    // Sorteia um consumo de 8% a 19% por missao para desgastar o equipamento progressivamente
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
    // Regra de negocios: se a bateria baixar de 20%, entra em estado de pane controlada e forca docagem.
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

// recarregar prende o drone em solo por 60 segundos antes de restaurar as condicoes mecanicas de voo.
func (d *Drone) recarregar() {
    fmt.Printf("[Drone %s] ⚡ Bateria baixa. Recarregando (60s)...\n", d.ID)
    time.Sleep(60 * time.Second)

    d.mu.Lock()
    d.Bateria = 100
    d.Status = "disponivel"
    d.mu.Unlock()

    fmt.Printf("[Drone %s] ✅ Recarga completa.\n", d.ID)
    
    // Avisa todos que o hardware esta de volta ao funcionamento
    d.registrarNosBrokers()

    // HACK LOGICO: Emite um laudo de fechamento falso chamado "RECARGA".
    // Isso instrui qualquer Broker que tivesse bloqueado este ID na Mempool
    // a varrer os metadados e marcar a string como liberada.
    d.reportarLaudo(protocol.Laudo{
        MissaoID:  "RECARGA",
        DroneID:   d.ID,
        Resultado: "Bateria recarregada com sucesso",
        Timestamp: time.Now(),
    })
}

// registrarNosBrokers prepara a payload de apresentacao inicial e faz broadcast na rede.
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

// reportarLaudo encapsula a resposta da finalizacao da missao e publica para todo o cluster.
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

// enviarParaTodosBrokers faz o descarregamento da mensagem iterando simultaneamente 
// por todos os parceiros mapeados. Utiliza WaitGroups para nao bloquear a caller.
func (d *Drone) enviarParaTodosBrokers(msg protocol.Mensagem, contexto string) {
    d.mu.Lock()
    listaBrokers := make([]string, len(d.Brokers))
    copy(listaBrokers, d.Brokers)
    d.mu.Unlock()

    // Cria padrao scatter/gather para disparar comunicacoes sem enfileirar I/O de rede
    var wg sync.WaitGroup
    for _, addr := range listaBrokers {
        wg.Add(1)
        go func(target string) {
            defer wg.Done()
            if d.tentarEnvio(target, msg) {
                // Logging omitido propositalmente para evitar verbosidade excessiva
                // fmt.Printf("[Drone %s] [%s] Enviado com sucesso para o broker: %s\n", d.ID, contexto, target)
            }
        }(addr)
    }
    wg.Wait()
}

// tentarEnvio contem a logica base de comunicacao socket efemera do cliente para o Broker.
// Tenta fechar o acordo via TLS; se rejeitado ou sem timeout, tenta plain TCP como fallback.
func (d *Drone) tentarEnvio(addr string, msg protocol.Mensagem) bool {
    cfg := &tls.Config{InsecureSkipVerify: true}
    conn, err := tls.DialWithDialer(&net.Dialer{Timeout: 2 * time.Second}, "tcp", addr, cfg)
    
    // Tratativa em cascata para redes hibridas
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

// carregarBrokers le o arquivo JSON e isola apenas as chaves relativas aos Brokers da topologia.
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
        
        // Verifica dois padroes de chave: "brokerX" ou apenas o id numerico "X"
        fmt.Sscanf(k, "broker%d", &n)
        if n == 0 {
            n, _ = strconv.Atoi(k)
        }
        if n > 0 {
            ids = append(ids, n)
        }
    }
    
    // Ordena as chaves numericamente para consistencia de mapeamento
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