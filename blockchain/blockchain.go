// ============================================================
// BLOCKCHAIN — Ledger distribuído imutável
//
// Responsabilidades:
//   - Manter a chain local de blocos encadeados por hash
//   - Gerar e verificar hashes SHA-256
//   - Gerenciar saldos de créditos das companhias (UTXO simplificado)
//   - Validar transações (anti-duplo gasto)
//   - Participar do consenso PoA (Proof of Authority):
//       um broker propõe um bloco → broadcast para peers →
//       maioria simples (>50%) aceita → bloco é commitado
//
// Origem: totalmente novo no Problema 3.
// ============================================================

package blockchain

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

// ============================================================
// ESTRUTURAS
// ============================================================

// TipoDados identifica o conteúdo do bloco.
type TipoDados string

const (
	TipoBloco_Transacao TipoDados = "TRANSACAO" // Pagamento de créditos
	TipoBloco_Laudo     TipoDados = "LAUDO"     // Relatório de missão concluída
	TipoBloco_Genesis   TipoDados = "GENESIS"   // Bloco inicial da cadeia
)

// Bloco é a unidade atômica do ledger.
// O encadeamento de hashes torna qualquer adulteração detectável:
// alterar dados de um bloco invalida seu hash e, consequentemente,
// o campo HashAnterior de todos os blocos seguintes.
type Bloco struct {
	Indice       int       `json:"indice"`
	Timestamp    time.Time `json:"timestamp"`
	TipoDados    TipoDados `json:"tipo_dados"`
	Dados        string    `json:"dados"` // JSON do payload (Transacao ou Laudo)
	HashAnterior string    `json:"hash_anterior"`
	Hash         string    `json:"hash"`
	Validador    string    `json:"validador"` // ID do broker que propôs o bloco
}

// Chain é a estrutura local do ledger de um nó.
type Chain struct {
	mu      sync.RWMutex
	Blocos  []Bloco        // Cadeia de blocos em ordem
	Saldos  map[string]int // saldos[companhiaID] = créditos disponíveis
	SaldosBloqueados map[string]int // Créditos "em trânsito" aguardando bloco
	DroneID string         // ID do broker dono desta chain (para logs)
}

// ============================================================
// INICIALIZAÇÃO
// ============================================================

// NovaChain cria uma chain com bloco gênesis e saldos iniciais das companhias.
// saldosIniciais permite configurar cada companhia com créditos ao iniciar.
// NovaChain cria uma chain com bloco gênesis estático e saldos iniciais.
func NovaChain(brokerID string, saldosIniciais map[string]int) *Chain {
	c := &Chain{
		Saldos:  make(map[string]int),
		SaldosBloqueados: make(map[string]int),
		DroneID: brokerID,
	}

	for k, v := range saldosIniciais {
		c.Saldos[k] = v
	}

	// GÊNESIS DETERMINÍSTICO: Todos os brokers geram exatamente o mesmo bloco
	timestampGenesis, _ := time.Parse(time.RFC3339, "2026-01-01T00:00:00Z")

	genesis := Bloco{
		Indice:       0,
		Timestamp:    timestampGenesis,
		TipoDados:    TipoBloco_Genesis,
		Dados:        `{"mensagem":"Genesis block - Estreito de Ormuz P3","broker":"SISTEMA"}`,
		HashAnterior: "0000000000000000",
		Validador:    "SISTEMA",
	}
	genesis.Hash = calcularHash(genesis)
	c.Blocos = append(c.Blocos, genesis)

	fmt.Printf("[blockchain %s] Chain iniciada. Bloco gênesis: %s\n", brokerID, genesis.Hash[:16])
	return c
}

// ============================================================
// HASH
// ============================================================

// calcularHash gera o SHA-256 de um bloco.
// O hash depende de índice, timestamp, tipo, dados, hash anterior e validador —
// qualquer alteração em qualquer campo produz um hash completamente diferente.
func calcularHash(b Bloco) string {
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

// ============================================================
// ADIÇÃO DE BLOCOS
// ============================================================

// ProporBloco cria um novo bloco candidato a ser adicionado à chain.
// O bloco ainda não é commitado — ele precisa passar pelo consenso PoA.
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
	novo.Hash = calcularHash(novo)

	return novo, nil
}

// CommitarBloco adiciona um bloco já aprovado pelo consenso à chain local.
// Valida a integridade antes de aceitar.
// Retorna erro se o bloco for inválido (adulterado ou fora de sequência).
func (c *Chain) CommitarBloco(b Bloco) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	ultimo := c.Blocos[len(c.Blocos)-1]

	// Verifica encadeamento
	if b.Indice != ultimo.Indice+1 {
		return fmt.Errorf("índice inválido: esperado %d, recebido %d", ultimo.Indice+1, b.Indice)
	}
	if b.HashAnterior != ultimo.Hash {
		return fmt.Errorf("hash anterior inválido: esperado %s, recebido %s", ultimo.Hash[:16], b.HashAnterior[:16])
	}

	// Verifica integridade do bloco em si
	hashCalculado := calcularHash(b)
	if hashCalculado != b.Hash {
		return fmt.Errorf("hash do bloco adulterado: calculado %s, recebido %s", hashCalculado[:16], b.Hash[:16])
	}

	c.Blocos = append(c.Blocos, b)
	fmt.Printf("[blockchain %s] Bloco #%d commitado. Hash: %s | Tipo: %s\n",
		c.DroneID, b.Indice, b.Hash[:16], b.TipoDados)
	return nil
}

// ============================================================
// GESTÃO DE CRÉDITOS (anti-duplo gasto)
// ============================================================

// ConsultarSaldo retorna o saldo atual de uma companhia.
func (c *Chain) ConsultarSaldo(companhiaID string) int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.Saldos[companhiaID]
}

// ValidarTransacao verifica se a companhia tem saldo suficiente.
// Se houver, "cativa" o valor temporariamente em SaldosBloqueados.
func (c *Chain) ValidarTransacao(companhiaID string, creditos int) error {
	c.mu.Lock() // IMPORTANTE: Mudou de RLock para Lock pois agora alteramos estado
	defer c.mu.Unlock()

	if creditos <= 0 {
		return fmt.Errorf("valor de créditos inválido: %d", creditos)
	}
	
	saldo, existe := c.Saldos[companhiaID]
	if !existe {
		return fmt.Errorf("companhia '%s' não cadastrada no ledger", companhiaID)
	}

	bloqueado := c.SaldosBloqueados[companhiaID]
	saldoDisponivel := saldo - bloqueado

	if saldoDisponivel < creditos {
		return fmt.Errorf("saldo insuficiente: companhia '%s' tem %d livre (%d bloqueados), precisa de %d",
			companhiaID, saldoDisponivel, bloqueado, creditos)
	}

	// Sucesso! O saldo está livre. Agora bloqueamos esse valor para uso.
	c.SaldosBloqueados[companhiaID] += creditos
	return nil
}

// DebitarCreditos subtrai créditos do saldo de uma companhia.
// Deve ser chamado SOMENTE após CommitarBloco de uma transação válida,
// garantindo que o débito está registrado imutavelmente antes de ser efetivado.
func (c *Chain) DebitarCreditos(companhiaID string, creditos int) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// ... [validações de saldo já existentes no seu código] ...

	c.Saldos[companhiaID] -= creditos
	
	// Remove o valor dos bloqueados, pois o débito foi consolidado
	if c.SaldosBloqueados[companhiaID] >= creditos {
		c.SaldosBloqueados[companhiaID] -= creditos
	} else {
		c.SaldosBloqueados[companhiaID] = 0 // Fallback de segurança
	}

	fmt.Printf("[blockchain %s] Débito efetivado: companhia=%s | Saldo Restante=%d\n",
		c.DroneID, companhiaID, c.Saldos[companhiaID])
	return nil
}

// LiberarSaldoBloqueado devolve os créditos "em trânsito" para o saldo livre
// da companhia. Usado quando uma ocorrência expira ou é cancelada na fila.
func (c *Chain) LiberarSaldoBloqueado(companhiaID string, creditos int) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.SaldosBloqueados[companhiaID] >= creditos {
		c.SaldosBloqueados[companhiaID] -= creditos
	} else {
		c.SaldosBloqueados[companhiaID] = 0 // Proteção contra valores negativos
	}

	fmt.Printf("[blockchain %s] 🔄 Saldo desbloqueado: companhia=%s | Créditos Devolvidos=%d\n",
		c.DroneID, companhiaID, creditos)
}

// CreditarSaldo adiciona créditos à conta de uma companhia (recarga ou devolução).
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

// ValidarChain verifica a integridade completa da cadeia local.
// Percorre todos os blocos e garante que hashes estão corretos e encadeados.
// Usado ao receber uma chain de outro nó para detectar adulterações.
func (c *Chain) ValidarChain() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	for i := 1; i < len(c.Blocos); i++ {
		atual := c.Blocos[i]
		anterior := c.Blocos[i-1]

		// Verifica encadeamento de hashes
		if atual.HashAnterior != anterior.Hash {
			fmt.Printf("[blockchain %s] ADULTERAÇÃO detectada no bloco #%d: hash anterior não bate\n",
				c.DroneID, atual.Indice)
			return false
		}

		// Verifica integridade do hash do bloco atual
		if calcularHash(atual) != atual.Hash {
			fmt.Printf("[blockchain %s] ADULTERAÇÃO detectada no bloco #%d: hash adulterado\n",
				c.DroneID, atual.Indice)
			return false
		}
	}
	return true
}

// SubstituirChain substitui a chain local por uma chain recebida de outro nó,
// mas somente se a chain recebida for mais longa E válida.
// É o mecanismo de sincronização ao reconectar ao cluster.
// Também reconstrói os saldos replaying todas as transações da nova chain.
func (c *Chain) SubstituirChain(nova []Bloco, saldosIniciais map[string]int) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(nova) <= len(c.Blocos) {
		return false // Chain recebida não é mais longa, ignorar
	}

	// Valida a chain recebida antes de aceitar
	for i := 1; i < len(nova); i++ {
		if nova[i].HashAnterior != nova[i-1].Hash {
			fmt.Printf("[blockchain %s] Chain recebida inválida no bloco #%d\n", c.DroneID, i)
			return false
		}
		if calcularHash(nova[i]) != nova[i].Hash {
			fmt.Printf("[blockchain %s] Hash adulterado na chain recebida, bloco #%d\n", c.DroneID, i)
			return false
		}
	}

	c.Blocos = nova

	// Reconstrói saldos a partir do zero
	c.SaldosBloqueados = make(map[string]int) // Zera os em trânsito
	for k, v := range saldosIniciais {
		c.Saldos[k] = v
	}

	for _, bloco := range nova {
		if bloco.TipoDados == TipoBloco_Transacao {
			var tx struct {
				De       string `json:"de"`
				Creditos int    `json:"creditos"`
			}
			if err := json.Unmarshal([]byte(bloco.Dados), &tx); err == nil {
				c.Saldos[tx.De] -= tx.Creditos
			}
		}
	}

	fmt.Printf("[blockchain %s] Chain substituída por chain com %d blocos\n", c.DroneID, len(nova))
	return true
}

// ============================================================
// SERIALIZAÇÃO
// ============================================================

// SerializarChain converte a chain local em JSON para transmissão.
func (c *Chain) SerializarChain() (string, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	dados, err := json.Marshal(c.Blocos)
	if err != nil {
		return "", fmt.Errorf("erro ao serializar chain: %w", err)
	}
	return string(dados), nil
}

// SerializarSaldos converte o mapa de saldos em JSON para transmissão.
func (c *Chain) SerializarSaldos() (string, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	dados, err := json.Marshal(c.Saldos)
	if err != nil {
		return "", fmt.Errorf("erro ao serializar saldos: %w", err)
	}
	return string(dados), nil
}

// Tamanho retorna o número de blocos na chain (incluindo gênesis).
func (c *Chain) Tamanho() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.Blocos)
}

// UltimoBloco retorna uma cópia do bloco mais recente da chain.
func (c *Chain) UltimoBloco() Bloco {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.Blocos[len(c.Blocos)-1]
}
