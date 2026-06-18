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

// InfoDespacho guarda o estado temporário de uma missão que aguarda a 
// confirmação de pagamento na Blockchain antes de autorizar a decolagem física.
type InfoDespacho struct {
    DroneID    string
    DroneAddr  string
    Ocorrencia *protocol.Ocorrencia
    Comando    protocol.Mensagem
    CriadoEm   time.Time // ⏱️ TEMPO PARA TIMEOUT DA MEMPOOL
}

// connSegura envolve uma conexão de rede (net.Conn) com um Mutex,
// garantindo que múltiplas goroutines possam escrever JSON nela sem corromper os dados.
type connSegura struct {
    mu      sync.Mutex
    conn    net.Conn
    encoder *json.Encoder
}

// novaConnSegura inicializa uma conexão protegida contra concorrência.
func novaConnSegura(conn net.Conn) *connSegura {
    return &connSegura{
        conn:    conn,
        encoder: json.NewEncoder(conn),
    }
}

// enviar serializa e transmite uma mensagem JSON na rede de forma thread-safe.
func (c *connSegura) enviar(msg protocol.Mensagem) error {
    c.mu.Lock()
    defer c.mu.Unlock()
    return c.encoder.Encode(msg)
}

// fechar encerra a conexão de rede embutida.
func (c *connSegura) fechar() {
    _ = c.conn.Close()
}

// Constantes de configuração do Broker para timeouts e economia do sistema.
const (
    intervaloHeartbeat = 5 * time.Second
    timeoutHeartbeat   = 12 * time.Second
    creditosIniciais   = 100
    custoEscolta       = 10
)

// Broker representa um nó do sistema distribuído.
// Ele gerencia o estado da fila, as conexões de rede, o consenso (Ricart-Agrawala)
// para uso dos drones e a validação do Ledger (Blockchain).
type Broker struct {
    mu sync.Mutex

    id       string
    endereco string
    mapaRede map[string]string

    drones        map[string]*protocol.Drone
    fila          state.FilaComAging // Fila de ocorrências que previne starvation (prioridade dinâmica)
    missoesAtivas map[string]bool
    encerrando    bool

    missoesPendentes map[string]InfoDespacho // Mempool: Missões aguardando o validador do bloco

    // ==========================================
    // Variáveis de Consenso (Ricart-Agrawala)
    // ==========================================
    relogioLocal int64
    requesting   bool
    inCS         bool // Indica se o nó está na Sessão Crítica (Critical Section)
    currentReqOc *protocol.Ocorrencia
    currentReqID string
    currentReqRA protocol.RequisicaoRA
    respostasOK  map[string]bool // Registra quem já deu 'OK' para entrar na CS
    deferred     map[string]bool // Registra para quem este broker deve enviar 'OK' após sair da CS

    // ==========================================
    // Variáveis de Rede e Comunicação
    // ==========================================
    connBrokers  map[string]*connSegura
    connDrones   map[net.Conn]string
    connClientes map[net.Conn]string
    ultimoHB     map[string]time.Time

    // ==========================================
    // Blockchain e Segurança
    // ==========================================
    chain           *blockchain.Chain
    votosBloco      map[string]int              // Conta votos de consenso para blocos pendentes
    blocosPendentes map[string]blockchain.Bloco // Guarda blocos em processo de votação
    esperandoSync   bool                        // 🛡️ TRAVA ANTI-INJEÇÃO DE HISTÓRICO FALSO
}

// obterPaisesPorBroker associa companhias fictícias aos brokers para divisão inicial do estado.
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

// gerarSaldosIniciais injeta o saldo base para as carteiras (wallets) no Bloco Gênesis.
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

// novoBroker constrói e inicializa todas as rotinas em background de um Broker.
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

// maxInt64 é um helper utilitário para sincronização do relógio lógico de Lamport.
func maxInt64(a, b int64) int64 {
    if a > b {
        return a
    }
    return b
}

// peersAtivos retorna um snapshot isolado e seguro das conexões ativas com outros brokers.
func (b *Broker) peersAtivos() map[string]*connSegura {
    b.mu.Lock()
    defer b.mu.Unlock()
    copia := make(map[string]*connSegura, len(b.connBrokers))
    for id, c := range b.connBrokers {
        copia[id] = c
    }
    return copia
}

// iniciarServidor configura e sobe o socket TCP utilizando TLS (criptografia SSL).
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

    // Loop infinito para aceitar novas conexões de Testers, Drones ou Brokers
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

// conectarPeers agenda a tentativa de conexão com toda a vizinhança declarada no config.json.
func (b *Broker) conectarPeers() {
    time.Sleep(3 * time.Second)
    for peerID, peerAddr := range b.mapaRede {
        if peerID == b.id {
            continue
        }
        go b.manterConexaoPeer(peerID, peerAddr)
    }
    time.Sleep(2 * time.Second)
    
    // Libera a porta de sincronização apenas durante a inicialização/reconexão
    b.mu.Lock()
    b.esperandoSync = true
    b.mu.Unlock()
    b.solicitarChain()
}

// manterConexaoPeer roda em background tentando reconectar infinitamente caso um broker vizinho caia.
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
            time.Sleep(3 * time.Second) // Backoff básico de falha
            continue
        }

        // Executa o Handshake inicial para se identificar para o outro nó
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
        b.lerMensagens(cs, peerID, dec) // Prende a goroutine escutando os pacotes deste peer
    }
}

// handleConexao é acionado quando um novo nó bate na porta deste broker.
// Define a classificação (Broker, Drone ou Cliente) baseada na mensagem de Handshake.
func (b *Broker) handleConexao(conn net.Conn) {
    dec := json.NewDecoder(conn)
    var msg protocol.Mensagem
    if err := dec.Decode(&msg); err != nil {
        conn.Close()
        return
    }

    cs := novaConnSegura(conn)

    // Avalia o aperto de mãos (Handshake)
    if msg.Tipo == protocol.TipoHandshake {
        var info protocol.InfoConexao
        _ = json.Unmarshal([]byte(msg.Payload), &info)

        switch info.Tipo {
        case "broker":
            b.mu.Lock()
            // Evita criar conexões cruzadas (A->B e B->A simultaneamente) usando ordenação de ID
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
            // Registra um Drone físico na rede local
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
                // Resgata o drone de falhas ou desconexões, devolvendo-o à frota
                if d.Status != "disponivel" {
                    d.Status = "disponivel"
                    fmt.Printf("[Broker %s] Drone %s está pronto e disponível novamente\n", b.id, info.ID)
                }
            }
            b.mu.Unlock()

            fmt.Printf("[Broker %s] Drone %s registrado (addr=%s)\n", b.id, info.ID, info.Endereco)
            go b.tentarDespachar() // Assim que um drone surge, checamos se há missões aguardando
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

// lerMensagens é o listener perpétuo de cada socket conectado. 
func (b *Broker) lerMensagens(cs *connSegura, remetenteID string, dec *json.Decoder) {
    for {
        var msg protocol.Mensagem
        // Fica bloqueado aqui até que bytes cheguem ou o socket quebre
        if err := dec.Decode(&msg); err != nil {
            break 
        }
        b.despachar(cs, msg)
    }
    // Se o loop quebrar, significa que a conexão foi cortada
    b.removerConexao(cs.conn, remetenteID)
}

// despachar age como o grande "Switchboard" ou Roteador do Broker.
// Incrementa o Relógio Lógico de Lamport e repassa os eventos aos handlers corretos.
func (b *Broker) despachar(cs *connSegura, msg protocol.Mensagem) {
    b.mu.Lock()
    // Algoritmo de Relógio de Lamport: Meu relógio se sincroniza com o maior valor visto na rede
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

// handleOcorrencia processa novas missões pedidas pelo Tester/Sensores.
// Checa fundos, adiciona à fila ou se for o nó Malicioso, tenta fraudar a rede bypassando a fila.
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

    // GATILHO VILÃO: Se o nó for malicioso, ele não usa drones nem filas. 
    // Ele usa o ping do Tester apenas como pretexto para disparar a fraude na rede!
    if os.Getenv("MALICIOUS") == "true" {
        fmt.Printf("[Broker %s] 😈 Interceptando requisição do Tester para injetar ATAQUE na rede!\n", b.id)
        
        // Responde ao Tester com ACK para o script não dar Timeout
        _ = cs.enviar(protocol.Mensagem{
            Tipo:      protocol.TipoACK,
            IDOrigem:  b.id,
            Timestamp: time.Now(),
        })
        
        // Chama o proporBloco. A função interna vai ignorar a ocorrência 
        // e rodar o switch de ataque (Salami, Fork ou Payload Corrompido)
        go b.proporBloco(blockchain.TipoBloco_Transacao, oc)
        return 
    }

    // =========================================================
    // FLUXO NORMAL PARA NÓS HONESTOS (PC1, PC2, etc)
    // =========================================================
    b.mu.Lock()
    totalAtivos := 0
    for id := range b.connBrokers {
        if !strings.HasPrefix(id, "tester") {
            totalAtivos++
        }
    }
    b.mu.Unlock()

    // Validação Pré-Fila: Bloqueia a inserção se a carteira já estiver zerada
    // (A não ser que o cluster inteiro tenha caído e estejamos em Modo Solo)
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

// handleStatusDrone recebe o aviso que um drone físico terminou o voo (Laudo gerado).
func (b *Broker) handleStatusDrone(msg protocol.Mensagem) {
    var laudo protocol.Laudo
    if err := json.Unmarshal([]byte(msg.Payload), &laudo); err != nil {
        return
    }

    b.mu.Lock()
    droneID := msg.IDOrigem
    // Devolve o drone para a disponibilidade
    if d, ok := b.drones[droneID]; ok {
        d.Status = "disponivel"
        d.MissaoID = ""
    }

    // Processa os 'OKs' de Ricart-Agrawala que ficaram represados enquanto este nó voava
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

    // Dispara as mensagens de liberação de exclusão mútua
    for _, peerID := range adiados {
        b.enviarRAOK(peerID)
    }

    // Propõe o Laudo como um novo bloco imutável na Blockchain
    if eraMinhaMissao {
        go func() {
            time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
            b.proporBloco(blockchain.TipoBloco_Laudo, laudo)
        }()
    }

    // Como liberou drone, verifica a fila novamente
    go b.tentarDespachar()
}

// tentarDespachar engatilha o Algoritmo de Exclusão Mútua Distribuída (Ricart-Agrawala).
// Ele solicita ao cluster autorização para tomar posse temporária da rede e despachar o drone.
func (b *Broker) tentarDespachar() {
    b.mu.Lock()

    // 1. Checa fila
    if b.fila.Len() == 0 {
        b.mu.Unlock()
        return
    }

    // 2. Checa drones locais na Frota
    livres := 0
    for _, d := range b.drones {
        if d.Status == "disponivel" {
            livres++
        }
    }
    
    // Aborta silenciosamente se não puder proceder
    if livres == 0 || b.requesting || b.inCS {
        b.mu.Unlock()
        return
    }

    oc := b.fila.Pop()
    if oc == nil {
        b.mu.Unlock()
        return
    }

    // 3. Modela o Request de RA e altera estado
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

    // Extrai lista pura de Brokers reais (ignora Testers visuais)
    peers := make(map[string]*connSegura, len(b.connBrokers))
    for id, c := range b.connBrokers {
        if !strings.HasPrefix(id, "tester") {
            peers[id] = c
        }
    }
    b.mu.Unlock()

    // Fallback: Se estiver sozinho (Modo Solo), entra na Sessão Crítica direto
    if len(peers) == 0 {
        b.entrarCS(oc)
        return
    }

    // 4. Multicast do Pedido RA para todo o cluster
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

// handleRARequest avalia um pedido de Sessão Crítica (CS) vindo de outro Broker.
// Compara Relógios de Lamport e Prioridades para decidir quem "ganha" o acesso ao Drone.
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
    
    // Regras de Decisão RA com Tie-Breaker em Cascata:
    if b.inCS {
        // Se eu já estou com o drone voando, ele tem que esperar.
        deveAdiar = true
    } else if b.requesting {
        minhaPrio := b.currentReqRA.Prioridade
        reqPrio := req.Prioridade

        // Critério 1: Urgência (P1 ganha de P2)
        if minhaPrio > reqPrio {
            deveAdiar = true
        } else if reqPrio > minhaPrio {
            deveAdiar = false
        } else {
            // Critério 2: Relógio de Lamport (Menor TS temporal ganha)
            if meuRelogio < req.Relogio {
                deveAdiar = true
            } else if meuRelogio > req.Relogio {
                deveAdiar = false
            } else {
                // Critério 3: Desempate absoluto via ID lexicográfico (evita deadlock infinito)
                if err1 == nil && err2 == nil {
                    deveAdiar = idLocal < idReq
                } else {
                    deveAdiar = b.id < req.BrokerID
                }
            }
        }
    } else {
        // Não estou pedindo drone para nada, pode liberar na hora
        deveAdiar = false
    }

    // O request do parceiro perdeu, colocamos ele na geladeira (deferred)
    if deveAdiar {
        b.deferred[req.BrokerID] = true
        b.mu.Unlock()
        return
    }

    // Se eu perdi, significa que deixei ele passar. Submeto transação de RENOVA para ganhar recompensa de boa-vontade.
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
        b.fila.AplicarAging() // Aumento a prioridade local para não morrer de "Starvation"
    }

    delete(b.deferred, req.BrokerID)
    b.mu.Unlock()
    b.enviarRAOK(req.BrokerID) // Deixo ele acessar a Sessão Crítica
}

// handleRAOK contabiliza os "sinal-verde" recebidos do cluster.
func (b *Broker) handleRAOK(msg protocol.Mensagem) {
    if strings.HasPrefix(msg.IDOrigem, "tester") {
        return // Ignora testers
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

    // Se alcancei a Unanimidade (todos vivos disseram OK), entro na CS
    oc := b.currentReqOc
    if recebidos >= necessario {
        b.requesting = false
        b.mu.Unlock()
        b.entrarCS(oc)
        return
    }
    b.mu.Unlock()
}

// entrarCS é o coração da Sessão Crítica do Ricart-Agrawala. 
// Dispara o drone físico de fato e libera o lock lógico da rede.
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

    // Varre quem ficou na "geladeira" esperando e libera (Manda OKs)
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

// enviarRAOK transmite a autorização (OK) efetiva.
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

// enviarComandoFisicoDrone faz a ponte TCP com o IoT/Embarcado (Container do Drone).
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

        // Aguarda ACK do hardware confirmando que entendeu o pedido
        conn.SetReadDeadline(time.Now().Add(3 * time.Second))
        var resposta map[string]interface{}
        errDecode := json.NewDecoder(conn).Decode(&resposta)

        rejeitado := false
        if errDecode == nil && resposta["acao"] == "rejeitado" {
            rejeitado = true
        }

        // Se hardware rejeitar (bateria fraca, offline), aborta operação
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

// despacharDrone prepara a missão e propõe o Pagamento na Blockchain. 
// A decolagem em si fica engatilhada na "Mempool" esperando a aprovação financeira da rede.
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

    // Manda um broadcast garantindo o bloqueio lógico desse drone em todos os nós
    reservaMsg := protocol.Mensagem{Tipo: protocol.TipoReservaDrone, IDOrigem: b.id, Timestamp: time.Now(), Payload: droneID}
    peers := make([]*connSegura, 0, len(b.connBrokers))
    for _, c := range b.connBrokers {
        peers = append(peers, c)
    }
    b.mu.Unlock()

    for _, c := range peers {
        _ = c.enviar(reservaMsg)
    }

    if droneAddr == "" {
        droneAddr = droneID + ":" + "909" + string(droneID[len(droneID)-1])
    }

    cmd := protocol.ComandoMissao{OcorrenciaID: oc.ID, Descricao: oc.Descricao, Prioridade: oc.Prioridade}
    payloadCmd, _ := json.Marshal(cmd)
    msgCmd := protocol.Mensagem{Tipo: protocol.TipoComandoDrone, IDOrigem: b.id, Timestamp: time.Now(), Payload: string(payloadCmd)}

    // Fluxo Cripto-Financeiro
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
        
        // Mempool: Salva o comando físico como "pendente de confirmação de pagamento"
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

        // Propõe o bloco na rede. A decolagem acontece apenas se a rede Commitar o Bloco!
        go func() {
            time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
            b.proporBloco(blockchain.TipoBloco_Transacao, tx)
        }()
    } else {
        // Missões do sistema (sem carteira atrelada) decolam imediatamente ignorando blockchain
        go b.enviarComandoFisicoDrone(droneID, droneAddr, oc, msgCmd)
    }
}

// recolocarOcorrenciaNaFila é o "Rollback" caso haja falha física na comunicação IoT.
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
// MONITOR DE MEMPOOL (GARBAGE COLLECTOR DE MISSÕES FANTASMAS)
// ============================================================

// monitorarMempool funciona como um Watchdog para a consistência da Rede.
// Se um bloco de pagamento ficar preso por um Fork na rede (Timeouts do Ricart/Blockchain),
// a Mempool detecta a missão órfã e a destrói para liberar o Drone travado (Drone Leak).
func (b *Broker) monitorarMempool() {
    ticker := time.NewTicker(5 * time.Second)
    defer ticker.Stop()
    for range ticker.C {
        b.mu.Lock()
        agora := time.Now()
        houveLimpeza := false

        // Varre todas as missões aguardando o Consenso da Blockchain
        for txID, info := range b.missoesPendentes {
            if agora.Sub(info.CriadoEm) > 10*time.Second {
                fmt.Printf("[Broker %s] 🧹 TIMEOUT CONSENSO: Abortando missão fantasma %s. Liberando Drone %s\n", b.id, info.Ocorrencia.ID, info.DroneID)
                
                // Força a liberação física do drone
                if dr, ok := b.drones[info.DroneID]; ok {
                    dr.Status = "disponivel"
                    dr.MissaoID = ""
                }
                
                // Põe a ocorrência no fim da fila
                info.Ocorrencia.Prioridade = 3
                b.fila.Push(info.Ocorrencia)
                
                delete(b.missoesAtivas, info.Ocorrencia.ID)
                delete(b.missoesPendentes, txID)
                houveLimpeza = true
            }
        }
        b.mu.Unlock()

        // Acorda a fila caso tenhamos devolvido algum drone ao pool
        if houveLimpeza {
            go b.tentarDespachar()
        }
    }
}

// ============================================================
// RECARGA PERIÓDICA AUTOMÁTICA EM BACKGROUND
// ============================================================

// recargaAutomaticaLoop atua como "Banco Central" imprimindo moeda protocolar
// para salvar países que ficarem sem fundos, mantendo o cluster vivo.
func (b *Broker) recargaAutomaticaLoop() {
    ticker := time.NewTicker(30 * time.Second)
    defer ticker.Stop()
    for range ticker.C {
        // Se o Broker for "Malicioso", ele não quer ajudar a economia
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

// proporBloco serializa um pacote de dados, engatilha simulações de fraude (se malicioso),
// ou o propaga validamente para receber votos dos outros nós.
func (b *Broker) proporBloco(tipoDados blockchain.TipoDados, dados interface{}) {
    inicioEspera := time.Now()
    
    // Trava de Single-Thread simples para ordenação de blocos
    for {
        b.mu.Lock()
        emConsenso := len(b.blocosPendentes) > 0
        b.mu.Unlock()
        
        if !emConsenso {
            break
        }
        
        // Evita ficar preso em um bloco que ninguém votou
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

    // INJEÇÃO DE ATAQUES (Apenas Brokers com MALICIOUS=true)
    if os.Getenv("MALICIOUS") == "true" {
        ataque := rand.Intn(3)
        switch ataque {
        case 0: // Ataque 1: Salami Attack (Roubo sistêmico gradual)
            tipoDados = blockchain.TipoBloco_Transacao
            dados = protocol.Transacao{ ID: fmt.Sprintf("RENOVA-FRAUDE-%d", time.Now().UnixNano()), De: "sistema", Para: "b5-australia", Creditos: 5, OcorrenciaID: "ATAQUE-SALAMI", Timestamp: time.Now() }
            fmt.Printf("\n[Broker %s] 😈 VILÃO: Tentando ataque Salami para b5-australia\n", b.id)
        case 1: // Ataque 2: Forking (Tentar forçar a quebra do hash_anterior)
            bloco, _ := b.chain.ProporBloco(tipoDados, dados, b.id)
            bloco.HashAnterior = "deadbeef00000000000000000000000000000000000000000000000000000000"
            fmt.Printf("\n[Broker %s] 😈 VILÃO: Tentando criar fork estrutural\n", b.id)
            b.propagarBloco(bloco)
            return
        case 2: // Ataque 3: Alteração de Payload pós-assinatura
            bloco, _ := b.chain.ProporBloco(tipoDados, dados, b.id)
            bloco.Dados = `{"mensagem":"Laudo forjado pós-assinatura!"}`
            fmt.Printf("\n[Broker %s] 😈 VILÃO: Tentando quebrar integridade do payload\n", b.id)
            b.propagarBloco(bloco)
            return
        }
    }

    // Fluxo Honesto: Propõe normalmente
    bloco, err := b.chain.ProporBloco(tipoDados, dados, b.id)
    if err != nil {
        return
    }
    b.propagarBloco(bloco)
}

// propagarBloco distribui a proposta para a vizinhança avaliadora (Gossip Protocol)
func (b *Broker) propagarBloco(bloco blockchain.Bloco) {
    b.mu.Lock()
    b.blocosPendentes[bloco.Hash] = bloco
    b.votosBloco[bloco.Hash] = 1 // Computa auto-voto
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

// handleNovoBloco é o auditor de segurança do Blockchain. Ele é invocado quando alguém quer minerar algo.
// Realiza escaneamentos robustos contra ataques Forking, Salami e Double-Spending.
func (b *Broker) handleNovoBloco(msg protocol.Mensagem) {
    var bloco blockchain.Bloco
    if err := json.Unmarshal([]byte(msg.Payload), &bloco); err != nil {
        return
    }

    // DEFESA 1: Evita Forking (Abre a porta de Sincronização)
    ultimoBloco := b.chain.UltimoBloco()
    if bloco.HashAnterior != ultimoBloco.Hash {
        fmt.Printf("\n[Broker %s] 🛡️ REJEITADO: Falha de Forking vinda do nó %s\n", b.id, msg.IDOrigem)
        b.mu.Lock()
        b.esperandoSync = true
        b.mu.Unlock()
        b.solicitarChain()
        return
    }

    // DEFESA 2: Integridade Criptográfica do Hash
    if blockchain.CalcularHash(bloco) != bloco.Hash {
        fmt.Printf("\n[Broker %s] 🛡️ REJEITADO: Quebra de integridade criptográfica do nó %s\n", b.id, msg.IDOrigem)
        return
    }

    // DEFESAS FINANCEIRAS E ANTI-SALAMI (Varre o Ledger e o JSON dos dados)
    if bloco.TipoDados == blockchain.TipoBloco_Transacao {
        var tx protocol.Transacao
        if err := json.Unmarshal([]byte(bloco.Dados), &tx); err == nil {
            if tx.De == "sistema" {
                if strings.HasPrefix(tx.ID, "RENOVA-") {
                    if tx.Creditos > 5 {
                        fmt.Printf("\n[Broker %s] 🛡️ REJEITADO: Recompensa inflada (Origem: %s)\n", b.id, msg.IDOrigem)
                        return
                    }
                    // Varre todo o histórico buscando se esse pagamento de ID já foi gerado
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

    // TIE-BREAKER CRIPTOGRÁFICO (DESEMPATE AO VIVO)
    // Se dois brokers emitirem propostas paralelas apontando pro mesmo bloco anterior, ocorre Split-Brain.
    // O nó verifica se o Hash dele é "lexicograficamente menor". Quem tem menor hash domina a rede.
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

    // Regista o bloco para ser votado
    if _, ok := b.blocosPendentes[bloco.Hash]; !ok {
        b.blocosPendentes[bloco.Hash] = bloco
        b.votosBloco[bloco.Hash] = 1 
    }
    b.votosBloco[bloco.Hash]++
    votos := b.votosBloco[bloco.Hash]
    
    // 🛡️ CORREÇÃO DE QUÓRUM: Garante que os Testers ("espectadores") não quebrem a matemática de consenso
    totalAtivos := 1 
    for id := range b.connBrokers {
        if !strings.HasPrefix(id, "tester") {
            totalAtivos++
        }
    }
    b.mu.Unlock()

    // Se o bloco foi aprovado por mim, aviso os outros "Eu Aceito"
    aceite := protocol.Mensagem{
        Tipo:      protocol.TipoAceiteBloco,
        IDOrigem:  b.id,
        Timestamp: time.Now(),
        Payload:   fmt.Sprintf(`{"hash":"%s"}`, bloco.Hash),
    }
    for _, peerConn := range b.peersAtivos() {
        _ = peerConn.enviar(aceite)
    }

    // Maioria Simples
    quorum := totalAtivos/2 + 1
    if votos >= quorum {
        b.commitarBloco(bloco)
    }
}

// handleAceiteBloco assimila os "Aceites" transmitidos pela rede. Se atingir Quórum, solidifica.
func (b *Broker) handleAceiteBloco(msg protocol.Mensagem) {
    var payload struct{ Hash string `json:"hash"` }
    if err := json.Unmarshal([]byte(msg.Payload), &payload); err != nil { return }

    b.mu.Lock()
    b.votosBloco[payload.Hash]++
    votos := b.votosBloco[payload.Hash]
    
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

// commitarBloco atua no pós-consenso. Pede à Blockchain para arquivar e faz as transferências financeiras.
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

    // Realiza as transações contábeis e reage ao evento no mundo real
    if bloco.TipoDados == blockchain.TipoBloco_Transacao {
        var tx protocol.Transacao
        if err := json.Unmarshal([]byte(bloco.Dados), &tx); err == nil {
            
            sucessoDebito := true

            // Lógica de Débito (Cobrança)
            if tx.De != "sistema" && tx.De != "" {
                if err := b.chain.DebitarCreditos(tx.De, tx.Creditos); err != nil {
                    // Fallback Salvavidas: Se a rede caiu e a carteira faliu, permite voar assim mesmo
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
            
            // Lógica de Depósito (Recompensas)
            if tx.Para != "sistema" && tx.Para != "" {
                b.chain.CreditarSaldo(tx.Para, tx.Creditos)
            }

            // ========================================================
            // GATILHO ARQUITETURAL ASÍNCRONO
            // O pagamento bateu! Se esse despache for nosso (Mempool Local), solte o drone agora.
            // ========================================================
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

// handleReqChain responde a um pedido de recuperação de rede mandando seu arquivo Chain
func (b *Broker) handleReqChain(cs *connSegura) {
    chainJSON, err := b.chain.SerializarChain()
    if err != nil { return }
    _ = cs.enviar(protocol.Mensagem{Tipo: protocol.TipoRespChain, IDOrigem: b.id, Timestamp: time.Now(), Payload: chainJSON})
}

// handleRespChain lida com a importação de uma Blockchain Externa caso nós tenhamos Forkado
func (b *Broker) handleRespChain(msg protocol.Mensagem) {
    b.mu.Lock()
    // Trava do Engineer Attack: Ignora uploads forçados se não pedimos nada
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

// handleConsultaSaldo responde diretamente ao Tester com a contabilidade local
func (b *Broker) handleConsultaSaldo(cs *connSegura, msg protocol.Mensagem) {
    saldo := b.chain.ConsultarSaldo(msg.IDOrigem)
    _ = cs.enviar(protocol.Mensagem{Tipo: protocol.TipoRespSaldo, IDOrigem: b.id, Timestamp: time.Now(), Payload: fmt.Sprintf(`{"companhia":"%s","saldo":%d}`, msg.IDOrigem, saldo)})
}

// solicitarChain manda requisições de download do estado mundial para todos os pares
func (b *Broker) solicitarChain() {
    peers := b.peersAtivos()
    for _, c := range peers {
        _ = c.enviar(protocol.Mensagem{Tipo: protocol.TipoReqChain, IDOrigem: b.id, Timestamp: time.Now()})
    }
}

// heartbeatLoop jorra Pings (Alive checks)
func (b *Broker) heartbeatLoop() {
    ticker := time.NewTicker(intervaloHeartbeat)
    defer ticker.Stop()
    for range ticker.C {
        peers := b.peersAtivos()
        msg := protocol.Mensagem{Tipo: protocol.TipoHeartbeat, IDOrigem: b.id, Timestamp: time.Now()}
        for _, c := range peers { _ = c.enviar(msg) }
    }
}

// monitorarPeers gerencia a topologia removendo os nós que falharem no Heartbeat Timeout
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

            // Se o parceiro morreu e a gente tava dependendo do OK dele para o Ricart-Agrawala, aborta o tracking dele.
            if b.requesting {
                delete(b.respostasOK, id)
                delete(b.deferred, id)
                totalAtivos := 0
                for key := range b.connBrokers {
                    if !strings.HasPrefix(key, "tester") { totalAtivos++ }
                }
                // Analisa a eleição de novo (nó falhou, menos votos necessários agora)
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

// removerConexao abstrai limpezas de memória (Maps) ao cair uma conexão
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

// encerrarSistema captura CTRL+C e gerencia salvamento pacífico
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

// carregarConfiguracao puxa as portas de deploy do arquivo raiz
func carregarConfiguracao(caminho string) (map[string]string, error) {
    dados, err := os.ReadFile(caminho)
    if err != nil { return nil, fmt.Errorf("não foi possível ler %s: %w", caminho, err) }
    var mapa map[string]string
    if err := json.Unmarshal(dados, &mapa); err != nil { return nil, fmt.Errorf("config.json malformado: %w", err) }
    return mapa, nil
}

// main é a entrada da CLI do container
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
    go b.encerrarSistema() // Fica em stand-by pro Docker Shutdown

    b.iniciarServidor()
}