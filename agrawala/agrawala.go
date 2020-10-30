// AUTORES:             Víctor Hernández Fernández y Sergio García-Campero Hernández
// NIAs:                717044 y 721520
// FICHERO:             agrawala.go
// FECHA:               29/10/2020
// TIEMPO:              15 horas aproximadamente.
// DESCRIPCIÓN:         Implementa el algoritmo Ricart-Agrawala distribuido.

package agrawala

import (
	"fmt"
	gv "github.com/DistributedClocks/GoVector/govec"
	"math"
	ms "github.com/SergioGCH/agrawala/tree/main/messagesystem"
	"strconv"
)

type Agrawala struct {
	node                  int
	ms                    ms.MessageSystem
	requesting_cs         bool
	perm_delayed          []bool
	clock                 int
	highest_clock         int
	mutex                 semaphore
	outstandingReplyCount semaphore
	myRole                int
}

type semaphore chan int

var Logger *gv.GoLog
var messagepayload []byte

const (
	TOTAL_NODES = 5
)

var aw Agrawala

func New(numNode int, mesy ms.MessageSystem, nodeRole int) {
	aw.node = numNode
	aw.ms = mesy
	aw.requesting_cs = false
	aw.perm_delayed = make([]bool, TOTAL_NODES)
	aw.clock = 0
	aw.highest_clock = 0
	aw.mutex = make(semaphore, 1)
	aw.outstandingReplyCount = make(semaphore, 0)
	aw.myRole = nodeRole
	var identificador string
	messagepayload = []byte("samplepayload")

	if nodeRole == 0 {
		identificador = "Lector" + strconv.Itoa(aw.node)
	} else {
		identificador = "Escritor" + strconv.Itoa(aw.node)
	}

	Logger = gv.InitGoVector(identificador, identificador, gv.GetDefaultConfig())

	go request()
	go reply()
}

func Acquire_mutex() {
	aw.mutex.wait(1)
	aw.requesting_cs = true
	aw.clock = aw.highest_clock + 1
	aw.mutex.signal(1)

	//Envía REQUEST a todos los otros nodos
	for i := 2; i <= TOTAL_NODES; i++ {
		if i != aw.node {
			vectorClockMessage := Logger.PrepareSend("Request. My clock: "+strconv.Itoa(aw.clock), messagepayload, gv.GetDefaultLogOptions())

			aw.ms.Send(i, ms.Message{Operation: ms.REQUEST, Node: aw.node, Clock: aw.clock, Role: aw.myRole, GoVector: vectorClockMessage})
		}
	}

	//Espera el REPLY de los otros nodos
	aw.outstandingReplyCount.wait(TOTAL_NODES - 2)
	
	Logger.LogLocalEvent("Entering CS", gv.GetDefaultLogOptions())
}

func Release_mutex() {
	Logger.LogLocalEvent("Leaving CS", gv.GetDefaultLogOptions())
	aw.requesting_cs = false

	for i := 0; i < TOTAL_NODES; i++ {
		if aw.perm_delayed[i] {
			aw.perm_delayed[i] = false

			vectorClockMessage := Logger.PrepareSend("Reply. My clock: "+strconv.Itoa(aw.clock), messagepayload, gv.GetDefaultLogOptions())

			aw.ms.Send(i+1, ms.Message{Operation: ms.REPLY, Node: aw.node, Clock: aw.clock, Role: aw.myRole, GoVector: vectorClockMessage})
		}
	}
}

func request() {
	var defered bool
	
	for {
		msg := aw.ms.ReceiveRequest()

		Logger.UnpackReceive("Receive Request. My clock: "+strconv.Itoa(aw.clock)+" Its clock: "+strconv.Itoa(msg.Clock), msg.GoVector, &messagepayload, gv.GetDefaultLogOptions())

		maxClock := math.Max(float64(aw.clock), float64(msg.Clock))
		aw.highest_clock = int(maxClock)

		fmt.Println("Mi clock:", aw.clock, " Mi nodo:", aw.node)
		fmt.Println("Su clock:", msg.Clock, " Su nodo:", msg.Node)

		aw.mutex.wait(1)
		excluded := exclude(aw.myRole, msg.Role)
		defered = aw.requesting_cs && (msg.Clock > aw.clock || (msg.Clock == aw.clock && msg.Node > aw.node))
		aw.mutex.signal(1)

		if defered && excluded {
			aw.perm_delayed[msg.Node-1] = true
		} else {
			vectorClockMessage := Logger.PrepareSend("Reply. My clock: "+strconv.Itoa(aw.clock), messagepayload, gv.GetDefaultLogOptions())

			aw.ms.Send(msg.Node, ms.Message{Operation: ms.REPLY, Node: aw.node, Clock: aw.clock, Role: aw.myRole, GoVector: vectorClockMessage})
		}

	}
}

func reply() {
	for {
		msg := aw.ms.ReceiveReply()

		Logger.UnpackReceive("Receive Reply. My clock: "+strconv.Itoa(aw.clock), msg.GoVector, &messagepayload, gv.GetDefaultLogOptions())

		aw.outstandingReplyCount.signal(1)
	}
}

func exclude(firstRole int, secondRole int) (excluded bool) {
	matrix := [3]bool{false, true, true}

	fmt.Println("Matriz de exclusión:", firstRole, secondRole, matrix[firstRole+secondRole])
	return matrix[firstRole+secondRole]
}

func (s semaphore) wait(n int) {
	for i := 0; i < n; i++ {
		s <- 1
	}
}

func (s semaphore) signal(n int) {
	for i := 0; i < n; i++ {
		<-s
	}
}
