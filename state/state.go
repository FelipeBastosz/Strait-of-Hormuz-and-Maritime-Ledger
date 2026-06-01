// ============================================================
// STATE — Fila de prioridade com Aging
//
// Combina:
//   - Heap de Felipe: interface heap.Interface do Go (O(log n))
//   - Aging de Daniel: incrementa prioridade de quem espera,
//     prevenindo starvation de ocorrências de baixa prioridade
//
// Regras de ordenação:
//   1. Maior Prioridade ganha (3 > 2 > 1)
//   2. Desempate: timestamp mais antigo ganha (FIFO por nível)
//   3. A cada despacho bem-sucedido, todos os demais itens
//      recebem +1 de prioridade (aging)
// ============================================================

package state

import (
	"Strait-of-Hormuz-and-Maritime-Ledger/protocol"
	"container/heap"
)

// ============================================================
// HEAP (Felipe)
// ============================================================

// FilaPrioridade implementa heap.Interface para Ocorrencias.
type FilaPrioridade []*protocol.Ocorrencia

func (pq FilaPrioridade) Len() int { return len(pq) }

// Less define a ordem de prioridade.
// Retorna true se o item i deve ser servido antes do item j.
func (pq FilaPrioridade) Less(i, j int) bool {
	// Regra 1: quem tem maior prioridade numérica sai primeiro
	if pq[i].Prioridade != pq[j].Prioridade {
		return pq[i].Prioridade > pq[j].Prioridade
	}
	// Regra 2: desempate por tempo de chegada (mais antigo = antes)
	return pq[i].Timestamp.Before(pq[j].Timestamp)
}

// Swap troca dois itens de posição (exigido pelo heap.Interface).
func (pq FilaPrioridade) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
}

// Push adiciona um item ao final do array antes do heap reordenar.
func (pq *FilaPrioridade) Push(x interface{}) {
	item := x.(*protocol.Ocorrencia)
	*pq = append(*pq, item)
}

// Pop remove e retorna o item de maior prioridade (topo do heap).
func (pq *FilaPrioridade) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	*pq = old[0 : n-1]
	return item
}

// ============================================================
// AGING (Daniel) — envolvendo a heap
// ============================================================

// FilaComAging é a fila de prioridade principal do sistema.
// Encapsula a heap e aplica aging automaticamente após cada despacho.
type FilaComAging struct {
	fila FilaPrioridade
}

// Inicializar prepara a fila (chamada obrigatória antes do uso).
func (f *FilaComAging) Inicializar() {
	f.fila = FilaPrioridade{}
	heap.Init(&f.fila)
}

// Push insere uma ocorrência na fila, respeitando a ordem do heap.
func (f *FilaComAging) Push(oc *protocol.Ocorrencia) {
	heap.Push(&f.fila, oc)
}

// Pop remove e retorna a ocorrência mais urgente.
// Aplica aging em todos os itens restantes para prevenir starvation.
func (f *FilaComAging) Pop() *protocol.Ocorrencia {
	if f.fila.Len() == 0 {
		return nil
	}
	item := heap.Pop(&f.fila).(*protocol.Ocorrencia)

	// Aging: cada item restante ganha +1 de prioridade.
	// Após no máximo (MaxPrioridade - MinPrioridade) chamadas de Pop,
	// um item de prioridade 1 terá a mesma prioridade que um de nível 3 originalmente.
	for _, oc := range f.fila {
		oc.Prioridade++
	}
	// Reordena o heap após alterar as prioridades
	heap.Init(&f.fila)

	return item
}

// Len retorna o número de itens na fila.
func (f *FilaComAging) Len() int {
	return f.fila.Len()
}

// Items retorna uma cópia da fila atual (para serialização/sync).
// Não altera a heap interna.
func (f *FilaComAging) Items() []*protocol.Ocorrencia {
	copia := make([]*protocol.Ocorrencia, len(f.fila))
	copy(copia, f.fila)
	return copia
}

// Restaurar reconstrói a fila a partir de uma slice de ocorrências
// (usado ao receber um snapshot de estado via TipoSyncEstado).
func (f *FilaComAging) Restaurar(itens []*protocol.Ocorrencia) {
	f.fila = make(FilaPrioridade, len(itens))
	copy(f.fila, itens)
	heap.Init(&f.fila)
}
