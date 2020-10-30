// AUTOR:             	Rafael Tolosana
// ADAPTACIÓN:			Víctor Hernández Fernández y Sergio García-Campero Hernández
// NIAs:                717044 y 721520
// FICHERO:             messagesystem.go
// FECHA:               29/10/2020
// TIEMPO:              3 horas aproximadamente.
// DESCRIPCIÓN:         Crea un sistema de comunicación.

package messagesystem

import (
	"bufio"
	"encoding/gob"
	"fmt"
	"net"
	"os"
)

type Message struct {
	Operation string
	Node      int
	Clock     int
	Role      int
	GoVector  []byte
}

type MessageSystem struct {
	replyCh     chan Message
	requestCh   chan Message
	operationCh chan Message
	peers       []string
	done        chan bool
	me          int
}

const (
	REQUEST     = "REQUEST"
	REPLY       = "REPLY"
	WRITE       = "WRITE"
	READ        = "READ"
	MAXMESSAGES = 10000
)

func checkError(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Fatal error: %s", err.Error())
		os.Exit(1)
	}
}

func parsePeers(path string) (lines []string) {
	file, err := os.Open(path)
	checkError(err)

	defer file.Close()

	scanner := bufio.NewScanner(file)

	scanner.Split(bufio.ScanLines)

	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}

	return lines
}

func (ms MessageSystem) Send(pid int, msg Message) {
	tcpAddr, err := net.ResolveTCPAddr("tcp", ms.peers[pid-1])
	checkError(err)
	conn, err := net.DialTCP("tcp", nil, tcpAddr)
	checkError(err)
	encoder := gob.NewEncoder(conn)
	err = encoder.Encode(msg)
	conn.Close()
}

func (ms *MessageSystem) ReceiveRequest() (msg Message) {
	msg = <-ms.requestCh
	return msg
}

func (ms *MessageSystem) ReceiveReply() (msg Message) {
	msg = <-ms.replyCh
	return msg
}

func (ms *MessageSystem) ReceiveOperation() (msg Message) {
	msg = <-ms.operationCh
	return msg
}

func New(whoIam int, usersFile string) (ms MessageSystem) {
	ms.me = whoIam
	ms.peers = parsePeers(usersFile)
	ms.replyCh = make(chan Message, MAXMESSAGES)
	ms.requestCh = make(chan Message, MAXMESSAGES)
	ms.operationCh = make(chan Message, MAXMESSAGES)
	ms.done = make(chan bool)

	go func() {
		listener, err := net.Listen("tcp", ms.peers[ms.me-1])
		checkError(err)
		fmt.Println("Process listening at " + ms.peers[ms.me-1])
		defer close(ms.requestCh)
		defer close(ms.replyCh)
		for {
			select {
			case <-ms.done:
				return
			default:
				conn, err := listener.Accept()
				checkError(err)
				decoder := gob.NewDecoder(conn)
				var msg Message
				err = decoder.Decode(&msg)
				conn.Close()

				if msg.Operation == REQUEST {
					ms.requestCh <- msg
				} else if msg.Operation == REPLY {
					ms.replyCh <- msg
				} else {
					ms.operationCh <- msg
				}

			}
		}
	}()
	return ms
}

func (ms *MessageSystem) Stop() {
	ms.done <- true
}
