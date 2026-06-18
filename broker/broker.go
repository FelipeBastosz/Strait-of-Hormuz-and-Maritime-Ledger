// ============================================================
// BROKER — Nó do cluster totalmente descentralizado
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

type InfoDespacho struct {
    DroneID    string
    DroneAddr  string
    Ocorrencia *protocol.Ocorrencia
    Comando    protocol.Mensagem
    CriadoEm   time.Time // ⏱️ TEMPO PARA TIMEOUT DA MEMPOOL
}

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

    missoesPendentes map[string]InfoDespacho

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

    // Blockchain e Segurança
    chain           *blockchain.Chain
    votosBloco      map[string]int
    blocosPendentes map[string]blockchain.Bloco
    esperandoSync   bool // 🛡️ TRAVA ANTI-INJEÇÃO DE HISTÓRICO FALSO
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
        id:               id,
        endereco:         mapaRede[id],
        mapaRede:         mapaRede,
        drones:           make(map[string]*protocol.Drone),
        missoesAtivas:    make(map[string]bool),
        missoesPendentes: make(map[string]InfoDespacho),
        respostasOK:      make(map[string]bool),
        deferred:         make(map[string]bool),
        connBrokers:      make(map[string]*connSegura),
        connDrones:       make(map[net.Conn]string),
        connClientes:     make(map[net.Conn]string),
        ultimoHB:         make(map[string]time.Time),
        votosBloco:       make(map[string]int),
        blocosPendentes:  make(map[string]blockchain.Bloco),
        chain:            blockchain.NovaChain(id, saldosIniciais),
        esperandoSync:    false,
    }
    b.fila.Inicializar()

    fmt.Printf("[Broker %s] Iniciado em %s | %d nós no cluster\n", id, mapaRede[id], len(mapaRede))
    go b.recargaAutomaticaLoop()
    go b.monitorarMempool() // 🚀 INICIA O GARBAGE COLLECTOR
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
    
    b.mu.Lock()
    b.esperandoSync = true
    b.mu.Unlock()
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
                if d.Status != "disponivel" {
                    d.Status = "disponivel"
                    fmt.Printf("[Broker %s] Drone %s está pronto e disponível novamente\n", b.id, info.ID)
                }
            }
            b.mu.Unlock()

            fmt.Printf("[Broker %s] Drone %s registrado (addr=%s)\n", b.id, info.ID, info.Endereco)
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
        _ = cs.enviar(protocol.Mensagem{Tipo: protocol.TipoPong, IDOrigem: b.id, Timestamp: time.Now()})

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

    b.mu.Lock()
    totalAtivos := 0
    for id := range b.connBrokers {
        if !strings.HasPrefix(id, "tester") {
            totalAtivos++
        }
    }
    b.mu.Unlock()

    // 🛡️ Validação Pré-Fila
    if oc.Solicitante != "" && totalAtivos > 0 {
        if err := b.chain.ValidarTransacao(oc.Solicitante, oc.Creditos); err != nil {
            fmt.Printf("[Broker %s] Créditos insuficientes para %s: %v\n", b.id, oc.Solicitante, err)
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
    fmt.Printf("[Broker %s] Ocorrência %s enfileirada (P%d) | fila local: %d\n", b.id, oc.ID, oc.Prioridade, b.fila.Len())

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

    fmt.Printf("[Broker %s] ✅ Missão %s concluída | drone=%s | resultado: %s\n", b.id, laudo.MissaoID, laudo.DroneID, laudo.Resultado)

    paises := obterPaisesPorBroker(strings.Replace(b.id, "broker", "", 1))
    if len(paises) >= 4 {
        fmt.Printf("[Broker %s] Saldo atual local: %s=%d %s=%d %s=%d %s=%d\n", b.id,
            paises[0], b.chain.ConsultarSaldo(paises[0]),
            paises[1], b.chain.ConsultarSaldo(paises[1]),
            paises[2], b.chain.ConsultarSaldo(paises[2]),
            paises[3], b.chain.ConsultarSaldo(paises[3]))
    }

    for _, peerID := range adiados {
        b.enviarRAOK(peerID)
    }

    if eraMinhaMissao {
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

    livres := 0
    for _, d := range b.drones {
        if d.Status == "disponivel" {
            livres++
        }
    }
    
    if livres == 0 || b.requesting || b.inCS {
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

    if len(peers) == 0 {
        b.entrarCS(oc)
        return
    }

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

        if minhaPrio > reqPrio {
            deveAdiar = true
        } else if reqPrio > minhaPrio {
            deveAdiar = false
        } else {
            if meuRelogio < req.Relogio {
                deveAdiar = true
            } else if meuRelogio > req.Relogio {
                deveAdiar = false
            } else {
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

    if b.requesting {
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
        b.fila.AplicarAging()
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
    if recebidos >= necessario {
        b.requesting = false
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

    b.despacharDrone(oc)

    b.mu.Lock()
    b.inCS = false
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

    go b.tentarDespachar()
}

func (b *Broker) enviarRAOK(peerID string) {
    b.mu.Lock()
    cs, ok := b.connBrokers[peerID]
    b.mu.Unlock()
    if ok {
        _ = cs.enviar(protocol.Mensagem{Tipo: protocol.TipoRAOK, IDOrigem: b.id, Timestamp: time.Now()})
    }
}

// ============================================================
// LÓGICA DE DESPACHO E MEMPOOL DE MISSÕES
// ============================================================

func (b *Broker) enviarComandoFisicoDrone(droneID string, droneAddr string, oc *protocol.Ocorrencia, msg protocol.Mensagem) {
    var conn net.Conn
    var err error

    cfg := &tls.Config{InsecureSkipVerify: true}
    conn, err = tls.DialWithDialer(&net.Dialer{Timeout: 2 * time.Second}, "tcp", droneAddr, cfg)
    if err != nil {
        conn, err = net.DialTimeout("tcp", droneAddr, 2*time.Second)
    }

    if err == nil {
        defer conn.Close()
        if err := json.NewEncoder(conn).Encode(msg); err != nil {
            b.recolocarOcorrenciaNaFila(oc, droneID, false)
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
            fmt.Printf("[Broker %s] Falha ao despachar para drone %s. Devolvendo à fila.\n", b.id, droneID)
            b.recolocarOcorrenciaNaFila(oc, droneID, rejeitado)
            return
        }

        fmt.Printf("[Broker %s] ✈  Drone %s DECOLOU → ocorrência %s (P%d)\n", b.id, droneID, oc.ID, oc.Prioridade)
    } else {
        fmt.Printf("[Broker %s] Falha de rede com drone %s (%s). Devolvendo à fila.\n", b.id, droneID, droneAddr)
        b.recolocarOcorrenciaNaFila(oc, droneID, false)
    }
}

func (b *Broker) despacharDrone(oc *protocol.Ocorrencia) {
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
        return
    }

    droneID = disponiveis[rand.Intn(len(disponiveis))]
    d := b.drones[droneID]

    d.Status = "em_missao"
    d.MissaoID = oc.ID
    b.missoesAtivas[oc.ID] = true
    droneAddr := d.Posicao

    reservaMsg := protocol.Mensagem{Tipo: protocol.TipoReservaDrone, IDOrigem: b.id, Timestamp: time.Now(), Payload: droneID}
    peers := make([]*connSegura, 0, len(b.connBrokers))
    for _, c := range b.connBrokers {
        peers = append(peers, c)
    }
    b.mu.Unlock()

    // Envia a reserva para a rede (Síncrono para garantir o RA)
    for _, c := range peers {
        _ = c.enviar(reservaMsg)
    }

    if droneAddr == "" {
        droneAddr = droneID + ":" + "909" + string(droneID[len(droneID)-1])
    }

    cmd := protocol.ComandoMissao{OcorrenciaID: oc.ID, Descricao: oc.Descricao, Prioridade: oc.Prioridade}
    payloadCmd, _ := json.Marshal(cmd)
    msgCmd := protocol.Mensagem{Tipo: protocol.TipoComandoDrone, IDOrigem: b.id, Timestamp: time.Now(), Payload: string(payloadCmd)}

    if oc.Solicitante != "" {
        txID := fmt.Sprintf("TX-%s-%d", oc.ID, time.Now().UnixNano())
        tx := protocol.Transacao{
            ID:           txID,
            De:           oc.Solicitante,
            Para:         "sistema",
            Creditos:     oc.Creditos,
            OcorrenciaID: oc.ID,
            Timestamp:    time.Now(),
        }
        
        // 🧊 Salva o comando físico como "pendente de confirmação de pagamento" com Timestamp
        b.mu.Lock()
        b.missoesPendentes[tx.ID] = InfoDespacho{
            DroneID:    droneID,
            DroneAddr:  droneAddr,
            Ocorrencia: oc,
            Comando:    msgCmd,
            CriadoEm:   time.Now(),
        }
        b.mu.Unlock()

        fmt.Printf("[Broker %s] Missão de %s em Mempool. Aguardando confirmação do pagamento de %d créditos...\n", b.id, tx.De, tx.Creditos)

        // Propõe o bloco na rede. A decolagem acontecerá no CommitarBloco!
        go func() {
            time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
            b.proporBloco(blockchain.TipoBloco_Transacao, tx)
        }()
    } else {
        // Missões do sistema (sem cobrança) decolam imediatamente
        go b.enviarComandoFisicoDrone(droneID, droneAddr, oc, msgCmd)
    }
}

func (b *Broker) recolocarOcorrenciaNaFila(oc *protocol.Ocorrencia, droneID string, rejeitado bool) {
    b.mu.Lock()
    if dr, ok := b.drones[droneID]; ok {
        if rejeitado {
            dr.Status = "em_missao"
        } else {
            dr.Status = "indisponivel"
        }
        dr.MissaoID = ""
    }

    oc.Prioridade = 3
    b.fila.Push(oc)
    delete(b.missoesAtivas, oc.ID)
    b.mu.Unlock()

    go b.tentarDespachar()
}

// ============================================================
// 🧹 MONITOR DE MEMPOOL (GARBAGE COLLECTOR DE MISSÕES FANTASMAS)
// ============================================================
func (b *Broker) monitorarMempool() {
    ticker := time.NewTicker(5 * time.Second)
    defer ticker.Stop()
    for range ticker.C {
        b.mu.Lock()
        agora := time.Now()
        houveLimpeza := false

        for txID, info := range b.missoesPendentes {
            if agora.Sub(info.CriadoEm) > 10*time.Second {
                fmt.Printf("[Broker %s] 🧹 TIMEOUT CONSENSO: Abortando missão fantasma %s. Liberando Drone %s\n", b.id, info.Ocorrencia.ID, info.DroneID)
                
                if dr, ok := b.drones[info.DroneID]; ok {
                    dr.Status = "disponivel"
                    dr.MissaoID = ""
                }
                
                info.Ocorrencia.Prioridade = 3
                b.fila.Push(info.Ocorrencia)
                
                delete(b.missoesAtivas, info.Ocorrencia.ID)
                delete(b.missoesPendentes, txID)
                houveLimpeza = true
            }
        }
        b.mu.Unlock()

        if houveLimpeza {
            go b.tentarDespachar()
        }
    }
}

// ============================================================
// RECARGA PERIÓDICA AUTOMÁTICA EM BACKGROUND
// ============================================================
func (b *Broker) recargaAutomaticaLoop() {
    ticker := time.NewTicker(30 * time.Second)
    defer ticker.Stop()
    for range ticker.C {
        if os.Getenv("MALICIOUS") == "true" {
            continue
        }
        
        paisesLocais := obterPaisesPorBroker(strings.Replace(b.id, "broker", "", 1))
        for _, pais := range paisesLocais {
            saldo := b.chain.ConsultarSaldo(pais)
            if saldo < 20 {
                fmt.Printf("[Broker %s] ⚡ Saldo de %s crítico (%d). Injetando recarga protocolar...\n", b.id, pais, saldo)
                txRecharge := protocol.Transacao{
                    ID:           fmt.Sprintf("RECHARGE-%s-%d", pais, time.Now().UnixNano()),
                    De:           "sistema",
                    Para:         pais,
                    Creditos:     100,
                    OcorrenciaID: "SYSTEM_RECHARGE",
                    Timestamp:    time.Now(),
                }
                b.proporBloco(blockchain.TipoBloco_Transacao, txRecharge)
            }
        }
    }
}

// ============================================================
// BLOCKCHAIN PROPOSER E DEFENSORES
// ============================================================

func (b *Broker) proporBloco(tipoDados blockchain.TipoDados, dados interface{}) {
    inicioEspera := time.Now()
    for {
        b.mu.Lock()
        emConsenso := len(b.blocosPendentes) > 0
        b.mu.Unlock()
        
        if !emConsenso {
            break
        }
        
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

    if os.Getenv("MALICIOUS") == "true" {
        ataque := rand.Intn(3)
        switch ataque {
        case 0:
            tipoDados = blockchain.TipoBloco_Transacao
            dados = protocol.Transacao{ ID: fmt.Sprintf("RENOVA-FRAUDE-%d", time.Now().UnixNano()), De: "sistema", Para: "b5-australia", Creditos: 5, OcorrenciaID: "ATAQUE-SALAMI", Timestamp: time.Now() }
            fmt.Printf("\n[Broker %s] 😈 VILÃO: Tentando ataque Salami para b5-australia\n", b.id)
        case 1:
            bloco, _ := b.chain.ProporBloco(tipoDados, dados, b.id)
            bloco.HashAnterior = "deadbeef00000000000000000000000000000000000000000000000000000000"
            fmt.Printf("\n[Broker %s] 😈 VILÃO: Tentando criar fork estrutural\n", b.id)
            b.propagarBloco(bloco)
            return
        case 2:
            bloco, _ := b.chain.ProporBloco(tipoDados, dados, b.id)
            bloco.Dados = `{"mensagem":"Laudo forjado pós-assinatura!"}`
            fmt.Printf("\n[Broker %s] 😈 VILÃO: Tentando quebrar integridade do payload\n", b.id)
            b.propagarBloco(bloco)
            return
        }
    }

    bloco, err := b.chain.ProporBloco(tipoDados, dados, b.id)
    if err != nil {
        return
    }
    b.propagarBloco(bloco)
}

func (b *Broker) propagarBloco(bloco blockchain.Bloco) {
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
    msg := protocol.Mensagem{Tipo: protocol.TipoNovoBloco, IDOrigem: b.id, Timestamp: time.Now(), Payload: string(blocoJSON)}
    for _, c := range peers {
        _ = c.enviar(msg)
    }
}

func (b *Broker) handleNovoBloco(msg protocol.Mensagem) {
    var bloco blockchain.Bloco
    if err := json.Unmarshal([]byte(msg.Payload), &bloco); err != nil {
        return
    }

    // 🛡️ DEFESA 1: Evita Forking (Abre a porta de Sincronização)
    ultimoBloco := b.chain.UltimoBloco()
    if bloco.HashAnterior != ultimoBloco.Hash {
        fmt.Printf("\n[Broker %s] 🛡️ REJEITADO: Falha de Forking vinda do nó %s\n", b.id, msg.IDOrigem)
        b.mu.Lock()
        b.esperandoSync = true
        b.mu.Unlock()
        b.solicitarChain()
        return
    }

    // 🛡️ DEFESA 2: Integridade Criptográfica
    if blockchain.CalcularHash(bloco) != bloco.Hash {
        fmt.Printf("\n[Broker %s] 🛡️ REJEITADO: Quebra de integridade criptográfica do nó %s\n", b.id, msg.IDOrigem)
        return
    }

    // 🛡️ DEFESAS FINANCEIRAS E ANTI-SALAMI
    if bloco.TipoDados == blockchain.TipoBloco_Transacao {
        var tx protocol.Transacao
        if err := json.Unmarshal([]byte(bloco.Dados), &tx); err == nil {
            if tx.De == "sistema" {
                if strings.HasPrefix(tx.ID, "RENOVA-") {
                    if tx.Creditos > 5 {
                        fmt.Printf("\n[Broker %s] 🛡️ REJEITADO: Recompensa inflada (Origem: %s)\n", b.id, msg.IDOrigem)
                        return
                    }
                    blocosHistoricos := b.chain.ObterBlocos()
                    for _, blk := range blocosHistoricos {
                        if blk.TipoDados == blockchain.TipoBloco_Transacao {
                            var txHistorica protocol.Transacao
                            if json.Unmarshal([]byte(blk.Dados), &txHistorica) == nil {
                                if txHistorica.OcorrenciaID == tx.OcorrenciaID && txHistorica.De == "sistema" && strings.HasPrefix(txHistorica.ID, "RENOVA-") {
                                    fmt.Printf("\n[Broker %s] 🛡️ BLOQUEIO: Ataque Salami interceptado! Ocorrência '%s' já foi paga. Voto Negado.\n", b.id, tx.OcorrenciaID)
                                    return
                                }
                            }
                        }
                    }
                } else if tx.OcorrenciaID == "SYSTEM_RECHARGE" {
                    if tx.Creditos != 100 {
                        fmt.Printf("\n[Broker %s] 🛡️ REJEITADO: Recarga do sistema com valor ilegal\n", b.id)
                        return
                    }
                    if b.chain.ConsultarSaldo(tx.Para) >= 20 {
                        fmt.Printf("\n[Broker %s] 🛡️ REJEITADO: Recarga inválida! %s possui saldo suficiente\n", b.id, tx.Para)
                        return
                    }
                } else {
                    return
                }
            } else if tx.De != "" {
                if b.chain.ConsultarSaldo(tx.De) < tx.Creditos {
                    fmt.Printf("\n[Broker %s] 🛡️ REJEITADO: Tentativa de Duplo-Gasto de %s\n", b.id, tx.De)
                    return 
                }
            }
        }
    }

    b.mu.Lock()

    // ⚔️ TIE-BREAKER (DESEMPATE AO VIVO)
    for hashPendente, blocoPendente := range b.blocosPendentes {
        if bloco.HashAnterior == blocoPendente.HashAnterior { 
            if bloco.Hash > blocoPendente.Hash { 
                b.mu.Unlock()
                return // O bloco que chegou perdeu no desempate
            } else if bloco.Hash < blocoPendente.Hash {
                delete(b.blocosPendentes, hashPendente)
                delete(b.votosBloco, hashPendente)
            }
        }
    }

    if _, ok := b.blocosPendentes[bloco.Hash]; !ok {
        b.blocosPendentes[bloco.Hash] = bloco
        b.votosBloco[bloco.Hash] = 1 
    }
    b.votosBloco[bloco.Hash]++
    votos := b.votosBloco[bloco.Hash]
    
    // 🛡️ IGNORA O TESTER DO QUORUM
    totalAtivos := 1 
    for id := range b.connBrokers {
        if !strings.HasPrefix(id, "tester") {
            totalAtivos++
        }
    }
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

    quorum := totalAtivos/2 + 1
    if votos >= quorum {
        b.commitarBloco(bloco)
    }
}

func (b *Broker) handleAceiteBloco(msg protocol.Mensagem) {
    var payload struct{ Hash string `json:"hash"` }
    if err := json.Unmarshal([]byte(msg.Payload), &payload); err != nil { return }

    b.mu.Lock()
    b.votosBloco[payload.Hash]++
    votos := b.votosBloco[payload.Hash]
    
    // 🛡️ IGNORA O TESTER DO QUORUM
    totalAtivos := 1
    for id := range b.connBrokers {
        if !strings.HasPrefix(id, "tester") {
            totalAtivos++
        }
    }
    
    bloco, existe := b.blocosPendentes[payload.Hash]
    b.mu.Unlock()

    if !existe { return }
    
    quorum := totalAtivos/2 + 1
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
            
            sucessoDebito := true

            if tx.De != "sistema" && tx.De != "" {
                if err := b.chain.DebitarCreditos(tx.De, tx.Creditos); err != nil {
                    b.mu.Lock()
                    totalAtivos := 0
                    for id := range b.connBrokers {
                        if !strings.HasPrefix(id, "tester") { totalAtivos++ }
                    }
                    b.mu.Unlock()

                    if totalAtivos == 0 {
                        fmt.Printf("[Broker %s] 🌐 FALLBACK MODO SOLO: Saldo insuficiente de %s, mas liberando despacho por emergência!\n", b.id, tx.De)
                    } else {
                        fmt.Printf("[Broker %s] ⛔ RECUSADO. %s falhou no pagamento: %v\n", b.id, tx.De, err)
                        sucessoDebito = false
                    }
                } else {
                    fmt.Printf("[Broker %s] Créditos debitados da companhia: %s\n", b.id, tx.De)
                }
            }
            
            if tx.Para != "sistema" && tx.Para != "" {
                b.chain.CreditarSaldo(tx.Para, tx.Creditos)
            }

            b.mu.Lock()
            info, eraDesteBroker := b.missoesPendentes[tx.ID]
            if eraDesteBroker {
                delete(b.missoesPendentes, tx.ID)
            }
            b.mu.Unlock()

            if eraDesteBroker {
                if sucessoDebito {
                    fmt.Printf("[Broker %s] Pagamento confirmado na rede! Autorizando decolagem...\n", b.id)
                    go b.enviarComandoFisicoDrone(info.DroneID, info.DroneAddr, info.Ocorrencia, info.Comando)
                } else {
                    fmt.Printf("[Broker %s] Cancelando despacho do %s por falta de fundos. Devolvendo ocorrência.\n", b.id, info.DroneID)
                    b.recolocarOcorrenciaNaFila(info.Ocorrencia, info.DroneID, false)
                }
            }
        }
    }
}

func (b *Broker) handleReqChain(cs *connSegura) {
    chainJSON, err := b.chain.SerializarChain()
    if err != nil { return }
    _ = cs.enviar(protocol.Mensagem{Tipo: protocol.TipoRespChain, IDOrigem: b.id, Timestamp: time.Now(), Payload: chainJSON})
}

func (b *Broker) handleRespChain(msg protocol.Mensagem) {
    b.mu.Lock()
    if !b.esperandoSync {
        b.mu.Unlock()
        fmt.Printf("\n[Broker %s] ⛔ HACK REJEITADO: Nós não solicitamos sincronização de chain do nó %s.\n", b.id, msg.IDOrigem)
        return
    }
    b.esperandoSync = false
    b.mu.Unlock()

    var blocos []blockchain.Bloco
    if err := json.Unmarshal([]byte(msg.Payload), &blocos); err != nil { return }
    
    saldosBase := gerarSaldosIniciais(b.mapaRede)
    if b.chain.SubstituirChain(blocos, saldosBase) {
        fmt.Printf("[Broker %s] [Chain] Sincronizada: %d blocos\n", b.id, b.chain.Tamanho())
    }
}

func (b *Broker) handleConsultaSaldo(cs *connSegura, msg protocol.Mensagem) {
    saldo := b.chain.ConsultarSaldo(msg.IDOrigem)
    _ = cs.enviar(protocol.Mensagem{Tipo: protocol.TipoRespSaldo, IDOrigem: b.id, Timestamp: time.Now(), Payload: fmt.Sprintf(`{"companhia":"%s","saldo":%d}`, msg.IDOrigem, saldo)})
}

func (b *Broker) solicitarChain() {
    peers := b.peersAtivos()
    for _, c := range peers {
        _ = c.enviar(protocol.Mensagem{Tipo: protocol.TipoReqChain, IDOrigem: b.id, Timestamp: time.Now()})
    }
}

func (b *Broker) heartbeatLoop() {
    ticker := time.NewTicker(intervaloHeartbeat)
    defer ticker.Stop()
    for range ticker.C {
        peers := b.peersAtivos()
        msg := protocol.Mensagem{Tipo: protocol.TipoHeartbeat, IDOrigem: b.id, Timestamp: time.Now()}
        for _, c := range peers { _ = c.enviar(msg) }
    }
}

func (b *Broker) monitorarPeers() {
    ticker := time.NewTicker(intervaloHeartbeat)
    defer ticker.Stop()
    for range ticker.C {
        b.mu.Lock()
        var mortos []string
        for id, t := range b.ultimoHB {
            if time.Since(t) > timeoutHeartbeat { mortos = append(mortos, id) }
        }
        b.mu.Unlock()

        for _, id := range mortos {
            fmt.Printf("[Broker %s] Peer %s não responde. Removendo.\n", b.id, id)
            b.mu.Lock()
            if c, ok := b.connBrokers[id]; ok { c.fechar() }
            delete(b.connBrokers, id)
            delete(b.ultimoHB, id)

            if b.requesting {
                delete(b.respostasOK, id)
                delete(b.deferred, id)
                totalAtivos := 0
                for key := range b.connBrokers {
                    if !strings.HasPrefix(key, "tester") { totalAtivos++ }
                }
                if len(b.respostasOK) >= totalAtivos && totalAtivos > 0 {
                    b.requesting = false
                    oc := &protocol.Ocorrencia{ID: b.currentReqID, Prioridade: b.currentReqRA.Prioridade, Timestamp: b.currentReqRA.Timestamp}
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

    fmt.Printf("\n[Broker %s] Encerrando persistentemente...\n", b.id)
    if err := b.chain.SalvarChain(); err != nil {
        fmt.Printf("[Broker %s] Aviso: falha ao persistir chain no encerramento: %v\n", b.id, err)
    }

    b.mu.Lock()
    for _, c := range b.connBrokers { c.fechar() }
    b.mu.Unlock()
    os.Exit(0)
}

func carregarConfiguracao(caminho string) (map[string]string, error) {
    dados, err := os.ReadFile(caminho)
    if err != nil { return nil, fmt.Errorf("não foi possível ler %s: %w", caminho, err) }
    var mapa map[string]string
    if err := json.Unmarshal(dados, &mapa); err != nil { return nil, fmt.Errorf("config.json malformado: %w", err) }
    return mapa, nil
}

func main() {
    if len(os.Args) < 2 {
        fmt.Println("Uso: broker [ID_BROKER]")
        os.Exit(1)
    }

    brokerID := os.Args[1]
    configPath := os.Getenv("CONFIG_PATH")
    if configPath == "" { configPath = "/app/config.json" }

    mapaRede, err := carregarConfiguracao(configPath)
    if err != nil { os.Exit(1) }
    if _, ok := mapaRede[brokerID]; !ok { os.Exit(1) }

    b := novoBroker(brokerID, mapaRede)
    go b.encerrarSistema()

    b.iniciarServidor()
}