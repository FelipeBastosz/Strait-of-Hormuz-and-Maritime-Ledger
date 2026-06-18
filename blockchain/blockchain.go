// ============================================================
// BLOCKCHAIN/PERSISTENCE — Persistência local da chain em disco
// ============================================================

package blockchain

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// ============================================================
// PERSISTÊNCIA EM DISCO
// ============================================================

type snapshotDisco struct {
	Blocos []Bloco        `json:"blocos"`
	Saldos map[string]int `json:"saldos"`
}

func caminhoArquivo(brokerID string) string {
	dir := os.Getenv("STATE_DIR")
	if dir == "" {
		dir = "/app/state"
	}
	return filepath.Join(dir, fmt.Sprintf("chain_%s.json", brokerID))
}

func (c *Chain) SalvarChain() error {
	c.mu.RLock()
	snapshot := snapshotDisco{
		Blocos: make([]Bloco, len(c.Blocos)),
		Saldos: make(map[string]int, len(c.Saldos)),
	}
	copy(snapshot.Blocos, c.Blocos)
	for k, v := range c.Saldos {
		snapshot.Saldos[k] = v
	}
	c.mu.RUnlock()

	dados, err := json.MarshalIndent(snapshot, "", "  ")
	if err != nil {
		return fmt.Errorf("erro ao serializar chain para disco: %w", err)
	}

	caminho := caminhoArquivo(c.DroneID)

	// 🛡️ Garante que o diretório base existe antes de gravar
	if err := os.MkdirAll(filepath.Dir(caminho), 0755); err != nil {
		return fmt.Errorf("erro ao criar diretório de estado: %w", err)
	}

	tmp := caminho + ".tmp"
	if err := os.WriteFile(tmp, dados, 0644); err != nil {
		return fmt.Errorf("erro ao escrever arquivo temporário: %w", err)
	}
	if err := os.Rename(tmp, caminho); err != nil {
		return fmt.Errorf("erro ao mover arquivo para destino final: %w", err)
	}

	fmt.Printf("[blockchain %s] Chain persistida: %d bloco(s) → %s\n",
		c.DroneID, len(snapshot.Blocos), caminho)
	return nil
}

func CarregarChain(brokerID string) (*snapshotDisco, error) {
	caminho := caminhoArquivo(brokerID)

	dados, err := os.ReadFile(caminho)
	if err != nil {
		if os.IsNotExist(err) {
			fmt.Printf("[blockchain %s] Nenhum estado anterior encontrado. Iniciando do zero.\n", brokerID)
			return nil, nil
		}
		return nil, fmt.Errorf("erro ao ler arquivo de estado: %w", err)
	}

	var snapshot snapshotDisco
	if err := json.Unmarshal(dados, &snapshot); err != nil {
		return nil, fmt.Errorf("arquivo de estado corrompido (%s): %w", caminho, err)
	}

	fmt.Printf("[blockchain %s] Estado anterior encontrado: %d bloco(s) carregados de %s\n",
		brokerID, len(snapshot.Blocos), caminho)
	return &snapshot, nil
}

func RestaurarChain(brokerID string, snapshot *snapshotDisco) *Chain {
	if snapshot == nil || len(snapshot.Blocos) == 0 {
		return nil
	}

	c := &Chain{
		Blocos:  snapshot.Blocos,
		Saldos:  snapshot.Saldos,
		DroneID: brokerID,
	}

	if !c.ValidarChain() {
		fmt.Printf("[blockchain %s] AVISO: Chain carregada do disco é inválida. Descartando e reiniciando.\n", brokerID)
		return nil
	}

	fmt.Printf("[blockchain %s] Chain restaurada com sucesso: %d bloco(s), saldos: %v\n",
		brokerID, len(c.Blocos), c.Saldos)
	return c
}

// ============================================================
// ESTRUTURAS
// ============================================================

type TipoDados string

const (
	TipoBloco_Transacao TipoDados = "TRANSACAO"
	TipoBloco_Laudo     TipoDados = "LAUDO"
	TipoBloco_Genesis   TipoDados = "GENESIS"
)

type Bloco struct {
	Indice       int       `json:"indice"`
	Timestamp    time.Time `json:"timestamp"`
	TipoDados    TipoDados `json:"tipo_dados"`
	Dados        string    `json:"dados"`
	HashAnterior string    `json:"hash_anterior"`
	Hash         string    `json:"hash"`
	Validador    string    `json:"validador"`
}

type Chain struct {
	mu      sync.RWMutex
	Blocos  []Bloco
	Saldos  map[string]int
	DroneID string
}

// ============================================================
// INICIALIZAÇÃO
// ============================================================

func NovaChain(brokerID string, saldosIniciais map[string]int) *Chain {
	snapshot, err := CarregarChain(brokerID)
	if err != nil {
		fmt.Printf("[blockchain %s] Erro ao carregar estado do disco: %v. Iniciando do zero.\n", brokerID, err)
	} else if snapshot != nil {
		if restaurada := RestaurarChain(brokerID, snapshot); restaurada != nil {
			return restaurada
		}
	}

	c := &Chain{
		Saldos:  make(map[string]int),
		DroneID: brokerID,
	}

	for k, v := range saldosIniciais {
		c.Saldos[k] = v
	}

	timestampGenesis, _ := time.Parse(time.RFC3339, "2026-01-01T00:00:00Z")

	genesis := Bloco{
		Indice:       0,
		Timestamp:    timestampGenesis,
		TipoDados:    TipoBloco_Genesis,
		Dados:        `{"mensagem":"Genesis block - Estreito de Ormuz P3","broker":"SISTEMA"}`,
		HashAnterior: "0000000000000000",
		Validador:    "SISTEMA",
	}
	genesis.Hash = CalcularHash(genesis)
	c.Blocos = append(c.Blocos, genesis)

	fmt.Printf("[blockchain %s] Chain iniciada do zero. Bloco gênesis: %s\n", brokerID, genesis.Hash[:16])

	if err := c.SalvarChain(); err != nil {
		fmt.Printf("[blockchain %s] Aviso: falha ao persistir estado inicial: %v\n", brokerID, err)
	}

	return c
}

// ============================================================
// HASH E BLOCOS
// ============================================================

func CalcularHash(b Bloco) string {
	entrada := fmt.Sprintf("%d%s%s%s%s%s",
		b.Indice,
		b.Timestamp.Format(time.RFC3339Nano),
		b.TipoDados,
		b.Dados,
		b.HashAnterior,
		b.Validador,
	)
	h := sha256.Sum256([]byte(entrada))
	return fmt.Sprintf("%x", h)
}

func (c *Chain) ProporBloco(tipoDados TipoDados, dados interface{}, validador string) (Bloco, error) {
	dadosJSON, err := json.Marshal(dados)
	if err != nil {
		return Bloco{}, fmt.Errorf("erro ao serializar dados do bloco: %w", err)
	}

	c.mu.RLock()
	ultimoBloco := c.Blocos[len(c.Blocos)-1]
	novoIndice := ultimoBloco.Indice + 1
	c.mu.RUnlock()

	novo := Bloco{
		Indice:       novoIndice,
		Timestamp:    time.Now(),
		TipoDados:    tipoDados,
		Dados:        string(dadosJSON),
		HashAnterior: ultimoBloco.Hash,
		Validador:    validador,
	}
	novo.Hash = CalcularHash(novo)

	return novo, nil
}

func (c *Chain) CommitarBloco(b Bloco) error {
	c.mu.Lock()

	ultimo := c.Blocos[len(c.Blocos)-1]

	if b.Indice != ultimo.Indice+1 {
		c.mu.Unlock() // Libera antes de retornar erro
		return fmt.Errorf("índice inválido: esperado %d, recebido %d", ultimo.Indice+1, b.Indice)
	}
	if b.HashAnterior != ultimo.Hash {
		c.mu.Unlock() // Libera antes de retornar erro
		return fmt.Errorf("hash anterior inválido: esperado %s, recebido %s", ultimo.Hash[:16], b.HashAnterior[:16])
	}

	hashCalculado := CalcularHash(b)
	if hashCalculado != b.Hash {
		c.mu.Unlock() // Libera antes de retornar erro
		return fmt.Errorf("hash do bloco adulterado: calculado %s, recebido %s", hashCalculado[:16], b.Hash[:16])
	}

	c.Blocos = append(c.Blocos, b)
	fmt.Printf("[blockchain %s] Bloco #%d commitado. Hash: %s | Tipo: %s\n",
		c.DroneID, b.Indice, b.Hash[:16], b.TipoDados)

	c.mu.Unlock() // Libera o lock ANTES de chamar SalvarChain

	if err := c.SalvarChain(); err != nil {
		fmt.Printf("[blockchain %s] AVISO: falha ao persistir após commit do bloco #%d: %v\n",
			c.DroneID, b.Indice, err)
	}

	return nil
}

// ============================================================
// GESTÃO DE CRÉDITOS E TRANSAÇÕES
// ============================================================

func (c *Chain) ConsultarSaldo(companhiaID string) int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.Saldos[companhiaID]
}

func (c *Chain) ValidarTransacao(companhiaID string, creditos int) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if creditos <= 0 {
		return fmt.Errorf("valor de créditos inválido: %d", creditos)
	}
	saldo, existe := c.Saldos[companhiaID]
	if !existe {
		return fmt.Errorf("companhia '%s' não cadastrada no ledger", companhiaID)
	}
	if saldo < creditos {
		return fmt.Errorf("saldo insuficiente: companhia '%s' tem %d crédito(s), precisa de %d",
			companhiaID, saldo, creditos)
	}
	return nil
}

func (c *Chain) DebitarCreditos(companhiaID string, creditos int) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	saldo, existe := c.Saldos[companhiaID]
	if !existe {
		return fmt.Errorf("companhia '%s' não encontrada", companhiaID)
	}
	if saldo < creditos {
		return fmt.Errorf("saldo insuficiente ao debitar: tem %d, tentou debitar %d", saldo, creditos)
	}
	c.Saldos[companhiaID] -= creditos
	fmt.Printf("[blockchain %s] Débito: companhia=%s créditos=-%d saldo_restante=%d\n",
		c.DroneID, companhiaID, creditos, c.Saldos[companhiaID])
	return nil
}

func (c *Chain) CreditarSaldo(companhiaID string, creditos int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.Saldos[companhiaID] += creditos
	fmt.Printf("[blockchain %s] Crédito: companhia=%s créditos=+%d saldo_total=%d\n",
		c.DroneID, companhiaID, creditos, c.Saldos[companhiaID])
}

// ============================================================
// VALIDAÇÃO E SINCRONIZAÇÃO
// ============================================================

func (c *Chain) ValidarChain() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	for i := 1; i < len(c.Blocos); i++ {
		atual := c.Blocos[i]
		anterior := c.Blocos[i-1]

		if atual.HashAnterior != anterior.Hash {
			fmt.Printf("[blockchain %s] ADULTERAÇÃO detectada no bloco #%d: hash anterior não bate\n",
				c.DroneID, atual.Indice)
			return false
		}

		if CalcularHash(atual) != atual.Hash {
			fmt.Printf("[blockchain %s] ADULTERAÇÃO detectada no bloco #%d: hash adulterado\n",
				c.DroneID, atual.Indice)
			return false
		}
	}
	return true
}

func (c *Chain) SubstituirChain(nova []Bloco, saldosIniciais map[string]int) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	// 🛡️ Impede regressões, mas abre espaço para desempate
	if len(nova) < len(c.Blocos) {
		return false
	}

	// ⚔️ TIE-BREAKER (Desempate em Fork)
	if len(nova) == len(c.Blocos) {
		ultimoLocal := c.Blocos[len(c.Blocos)-1]
		ultimoRemoto := nova[len(nova)-1]

		// O hash menor vence a disputa lexicograficamente
		if ultimoLocal.Hash <= ultimoRemoto.Hash {
			return false
		}
	}

	// Validação de integridade da nova Chain
	for i := 1; i < len(nova); i++ {
		if nova[i].HashAnterior != nova[i-1].Hash {
			fmt.Printf("[blockchain %s] Chain recebida inválida no bloco #%d\n", c.DroneID, i)
			return false
		}
		if CalcularHash(nova[i]) != nova[i].Hash {
			fmt.Printf("[blockchain %s] Hash adulterado na chain recebida, bloco #%d\n", c.DroneID, i)
			return false
		}
	}

	c.Blocos = nova

	// 🧹 Limpeza completa do mapa de saldos antes de refazer o estado
	c.Saldos = make(map[string]int)
	for k, v := range saldosIniciais {
		c.Saldos[k] = v
	}

	// Replay financeiro
	for _, bloco := range nova {
		if bloco.TipoDados == TipoBloco_Transacao {
			var tx struct {
				De       string `json:"de"`
				Para     string `json:"para"`
				Creditos int    `json:"creditos"`
			}
			if err := json.Unmarshal([]byte(bloco.Dados), &tx); err == nil {
				if tx.De != "sistema" && tx.De != "" {
					c.Saldos[tx.De] -= tx.Creditos
				}
				if tx.Para != "sistema" && tx.Para != "" {
					c.Saldos[tx.Para] += tx.Creditos
				}
			}
		}
	}

	fmt.Printf("[blockchain %s] 🔄 Fork Resolvido! Chain sincronizada e desempate concluído (%d blocos)\n", c.DroneID, len(nova))

	go func() {
		if err := c.SalvarChain(); err != nil {
			fmt.Printf("[blockchain %s] Aviso: falha ao persistir após substituição de chain: %v\n", c.DroneID, err)
		}
	}()

	return true
}

// ============================================================
// SERIALIZAÇÃO E UTILITÁRIOS
// ============================================================

func (c *Chain) SerializarChain() (string, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	dados, err := json.Marshal(c.Blocos)
	if err != nil {
		return "", fmt.Errorf("erro ao serializar chain: %w", err)
	}
	return string(dados), nil
}

func (c *Chain) SerializarSaldos() (string, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	dados, err := json.Marshal(c.Saldos)
	if err != nil {
		return "", fmt.Errorf("erro ao serializar saldos: %w", err)
	}
	return string(dados), nil
}

func (c *Chain) Tamanho() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.Blocos)
}

func (c *Chain) UltimoBloco() Bloco {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.Blocos[len(c.Blocos)-1]
}

// ============================================================
// SEGURANÇA CONTRA FRAUDES E CONCORRÊNCIA
// ============================================================

func (c *Chain) ObterBlocos() []Bloco {
	c.mu.RLock()
	defer c.mu.RUnlock()

	copia := make([]Bloco, len(c.Blocos))
	copy(copia, c.Blocos)

	return copia
}