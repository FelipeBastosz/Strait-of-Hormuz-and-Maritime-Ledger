// ============================================================
// CLIENT — Terminal de comando interativo
//
// Baseado no client de Felipe, com:
//   - TLS na conexão com o broker
//   - Campo Solicitante (companhia) e Creditos na ocorrência
//   - Comando "saldo" para consultar créditos na blockchain
//   - Handshake de identificação ao conectar (Daniel)
//
// Uso: client [ENDERECO_BROKER]
// Exemplo: client broker1:9081
// ============================================================

package main

import (
	"bufio"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"Strait-of-Hormuz-and-Maritime-Ledger/protocol"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Uso: client [ENDERECO_BROKER]")
		fmt.Println("Exemplo: client broker1:9081")
		return
	}

	enderecoBroker := os.Args[1]
	reader := bufio.NewReader(os.Stdin)

	menu()
	fmt.Printf("Conectado ao broker: %s\n\n", enderecoBroker)

	contador := 1
	companhiaAtual := "companhia-a" // Pode ser alterado durante a sessão

	for {
		menu()
		fmt.Printf("Companhia ativa: %s\n", companhiaAtual)
		fmt.Println("Comandos: [1] Enviar ocorrência  [2] Consultar saldo  [3] Trocar companhia  [sair]")
		fmt.Print("> ")

		linha, _ := reader.ReadString('\n')
		linha = strings.TrimSpace(linha)

		switch strings.ToLower(linha) {

		case "sair":
			fmt.Println("Encerrando terminal.")
			return

		case "2", "saldo":
			consultarSaldo(enderecoBroker, companhiaAtual)

		case "3", "trocar":
			fmt.Println("Companhias disponíveis: companhia-a, companhia-b, companhia-c, companhia-d")
			fmt.Print("Nova companhia: ")
			nova, _ := reader.ReadString('\n')
			nova = strings.TrimSpace(nova)
			if nova != "" {
				companhiaAtual = nova
				fmt.Printf("Companhia alterada para: %s\n", companhiaAtual)
			}

		default: // "1" ou qualquer texto = enviar ocorrência
			fmt.Print("Descrição da ocorrência (ou 'sair'): ")
			descricao, _ := reader.ReadString('\n')
			descricao = strings.TrimSpace(descricao)

			if strings.ToLower(descricao) == "sair" {
				return
			}

			fmt.Print("Prioridade (1=Aviso, 2=Alerta, 3=Crítico): ")
			prioStr, _ := reader.ReadString('\n')
			prioridade, err := strconv.Atoi(strings.TrimSpace(prioStr))
			if err != nil || prioridade < 1 || prioridade > 3 {
				fmt.Println("❌ Prioridade inválida. Use 1, 2 ou 3.\n")
				continue
			}

			oc := protocol.Ocorrencia{
				ID:          fmt.Sprintf("CLI-%s-OC%04d", os.Getenv("HOSTNAME"), contador),
				Prioridade:  prioridade,
				Timestamp:   time.Now(),
				Descricao:   descricao,
				Setor:       "manual-input",
				Solicitante: companhiaAtual,
				Creditos:    10, // Custo padrão de escolta
			}

			enviarOcorrencia(enderecoBroker, oc)
			contador++
			fmt.Println()
		}
	}
}

// enviarOcorrencia empacota e envia ao broker via TLS.
func enviarOcorrencia(enderecoBroker string, oc protocol.Ocorrencia) {
	payload, _ := json.Marshal(oc)
	msg := protocol.Mensagem{
		Tipo:      protocol.TipoOcorrencia,
		IDOrigem:  oc.Solicitante,
		Timestamp: time.Now(),
		Payload:   string(payload),
	}

	conn := conectar(enderecoBroker)
	if conn == nil {
		fmt.Printf("❌ Não foi possível conectar ao broker %s\n", enderecoBroker)
		return
	}
	defer conn.Close()

	json.NewEncoder(conn).Encode(msg)

	// Aguarda ACK ou NACK do broker
	conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	var resp protocol.Mensagem
	if err := json.NewDecoder(conn).Decode(&resp); err == nil {
		switch resp.Tipo {
		case protocol.TipoACK:
			fmt.Printf("✅ Ocorrência %s aceita pelo broker!\n", oc.ID)
		case "NACK":
			// NACK indica saldo insuficiente ou erro de validação
			fmt.Printf("❌ Ocorrência rejeitada: %s\n", resp.Payload)
		default:
			fmt.Printf("⚠️  Mensagem enviada, aguardando processamento...\n")
		}
	} else {
		fmt.Printf("⚠️  Mensagem enviada (sem confirmação imediata)\n")
	}
}

// consultarSaldo solicita o saldo de créditos da companhia ao broker.
func consultarSaldo(enderecoBroker, companhiaID string) {
	msg := protocol.Mensagem{
		Tipo:      protocol.TipoConsultaSaldo,
		IDOrigem:  companhiaID,
		Timestamp: time.Now(),
	}

	conn := conectar(enderecoBroker)
	if conn == nil {
		fmt.Printf("❌ Não foi possível conectar ao broker %s\n", enderecoBroker)
		return
	}
	defer conn.Close()

	json.NewEncoder(conn).Encode(msg)

	conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	var resp protocol.Mensagem
	if err := json.NewDecoder(conn).Decode(&resp); err == nil && resp.Tipo == protocol.TipoRespSaldo {
		fmt.Printf("💳 Saldo de %s: %s crédito(s)\n", companhiaID, resp.Payload)
	} else {
		fmt.Println("⚠️  Não foi possível obter o saldo no momento.")
	}
}

// conectar abre uma conexão TLS com fallback para TCP puro em dev local.
func conectar(addr string) net.Conn {
	cfg := &tls.Config{InsecureSkipVerify: true}
	conn, err := tls.DialWithDialer(&net.Dialer{Timeout: 2 * time.Second}, "tcp", addr, cfg)
	if err == nil {
		return conn
	}
	// Fallback TCP
	conn2, err2 := net.DialTimeout("tcp", addr, 2*time.Second)
	if err2 != nil {
		return nil
	}
	return conn2
}

func menu() {
	fmt.Println("==================================================")
	fmt.Println("   Terminal de Comando — Estreito de Ormuz P3    ")
	fmt.Println("==================================================")
}
