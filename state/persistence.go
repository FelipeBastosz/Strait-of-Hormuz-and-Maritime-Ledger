// ============================================================
// STATE — Estado global do cluster
//
// GlobalState é o snapshot completo do sistema em um dado momento.
// É serializado em JSON e enviado pelo coordenador via TipoSyncEstado
// a cada 2 segundos para manter todos os brokers sincronizados.
//
//
// ============================================================

package state

import (
	"Strait-of-Hormuz-and-Maritime-Ledger/protocol"
)

// GlobalState é a foto do sistema: frota de drones + fila de espera.
// O campo UltimoUpdate (Unix nano) permite comparar versões ao receber syncs:
// aceitar somente se UltimoUpdate > local, descartando syncs atrasados.
type GlobalState struct {
	Drones       map[string]*protocol.Drone `json:"drones"`
	FilaEspera   []*protocol.Ocorrencia     `json:"fila_espera"`
	UltimoUpdate int64                      `json:"ultimo_update"`
}

// NovoEstado inicializa um estado vazio com maps alocados.
func NovoEstado() *GlobalState {
	return &GlobalState{
		Drones:     make(map[string]*protocol.Drone),
		FilaEspera: []*protocol.Ocorrencia{},
	}
}
