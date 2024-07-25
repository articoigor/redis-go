package main

import (
	"encoding/hex"
	"flag"
	"fmt"
	"net"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const alphaNumeric = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

type HashMap struct {
	createdAt, expiry int64
	value             string
}

type Server struct {
	database            map[string]HashMap
	replica             string
	role, replicationId string
	offset              int
}

func main() {
	var port int

	var masterAddress string

	flag.IntVar(&port, "port", 6379, "Port given as argument")

	flag.StringVar(&masterAddress, "replicaof", "", "Role assigned to the current connection replica")

	flag.Parse()

	l, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", port))

	if err != nil {
		fmt.Printf("Failed to bind to port %d", port)
		os.Exit(1)
	} else {
		fmt.Printf("Listening on port %d", port)
	}
	fmt.Println("AAAAAAAAAAAAAAAAAAAAAAAAAA")
	go handleConnections(l, masterAddress, port)
}

func handleConnections(listener net.Listener, masterAddress string, port int) {
	fmt.Println("AAAAAAAAAAAAAAAAAAAAAAAAAA")
	for {

		fmt.Println("********")
		_, err := listener.Accept()

		if err != nil {
			fmt.Println("Error accepting connection:", err.Error())

			continue
		}

		if len(masterAddress) > 0 {
			sendHandshake(masterAddress, port)
			server := Server{role: "master", database: map[string]HashMap{}, replicationId: generateRepId(), replica: "", offset: 0}
			server.role = "subscriber"
		}

		// go handleCommand(conn, &server)
	}
}

func sendHandshake(masterAddress string, port int) {
	master := strings.Split(masterAddress, " ")

	dialAddress := strings.Join(master, ":")

	handshakeConn, err := net.Dial("tcp", dialAddress)

	if err == nil {
		handshakeConn.Write([]byte("*1\r\n$4\r\nPING\r\n"))
	}

	handshakeRes := make([]byte, 256)

	_, err = handshakeConn.Read(handshakeRes)

	if err == nil {
		sendReplconf(handshakeConn, strconv.Itoa(port))
	}

	handshakeConn.Close()
}

func sendReplconf(conn net.Conn, port string) {
	defer conn.Close()

	confirmationStr := fmt.Sprintf("*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$%d\r\n%s\r\n", len(port), port)

	conn.Write([]byte(confirmationStr))

	firstRes := make([]byte, 128)

	_, err := conn.Read(firstRes)

	isOk := err == nil && regexp.MustCompile("OK").MatchString(string(firstRes))

	if isOk {
		conn.Write([]byte("*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n"))
	}

	secondRes := make([]byte, 128)

	_, err = conn.Read(secondRes)

	isOk = err == nil && regexp.MustCompile("OK").MatchString(string(secondRes))

	if isOk {
		conn.Write([]byte("*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n"))
	}
}

func handleCommand(conn net.Conn, serverAdr *Server) {
	defer conn.Close()

	for {
		buf := make([]byte, 1024)

		_, err := conn.Read(buf)

		if err != nil {
			fmt.Println("Error reading from connection:", err.Error())
			break
		}

		data := strings.Split(string(buf), "\r\n")

		processRequest(data, string(buf), serverAdr, conn)
	}
}

func processRequest(data []string, req string, server *Server, conn net.Conn) {
	endpoint := data[2]

	switch endpoint {
	case "ECHO":
		conn.Write([]byte(fmt.Sprintf(("+%s\r\n"), data[4])))
	case "PING":
		conn.Write([]byte("+PONG\r\n"))
	case "GET":
		processGetRequest(data, server.database, conn)
	case "SET":
		processSetRequest(data, req, server.database, conn, *server)
	case "INFO":
		processInfoRequest(*server, conn)
	case "REPLCONF":
		processReplconf(conn, req, server)
	case "PSYNC":
		processPsync(conn, *server)
	default:
		fmt.Println("Invalid command informed")
	}
}

func processGetRequest(data []string, hashMap map[string]HashMap, conn net.Conn) {
	key := data[4]

	mapObj := hashMap[key]

	timeSpan := retrieveTimePassed(mapObj)

	message := "+" + mapObj.value

	if mapObj.expiry > 0 && timeSpan > mapObj.expiry {
		delete(hashMap, key)

		message = "$-1"
	}

	_, err := conn.Write([]byte(fmt.Sprintf("%s\r\n", message)))

	if err != nil {
		fmt.Println("Error writing to connection:", err.Error())
	}
}

func processSetRequest(data []string, req string, hashMap map[string]HashMap, conn net.Conn, server Server) {
	now := time.Now()

	expiryVal := 0

	regex, _ := regexp.Compile("px")

	if regex.MatchString(req) {
		expiryVal, _ = strconv.Atoi(data[10])
	}

	hashValue := HashMap{value: data[6], createdAt: now.UnixMilli(), expiry: int64(expiryVal)}

	key := data[4]

	hashMap[key] = hashValue

	_, err := conn.Write([]byte(fmt.Sprintf("%s\r\n", "+OK")))

	if err != nil {
		fmt.Println("Error writing to connection:", err.Error())
	}

	if server.role == "master" {
		fmt.Printf("\r\nStarted propagating SET command to %s\r\n", server.replica)
		propagateToReplica(server, key, hashValue.value)
	}
}

func processInfoRequest(server Server, conn net.Conn) {
	str := fmt.Sprintf("role:%s\r\nmaster_replid:%s\r\nmaster_repl_offset:%d", server.role, server.replicationId, server.offset)

	message := fmt.Sprintf("$%d\r\n%s", len(str), str)

	_, err := conn.Write([]byte(fmt.Sprintf("%s\r\n", message)))

	if err != nil {
		fmt.Println("Error writing to connection:", err.Error())
	}
}

func processReplconf(conn net.Conn, req string, serverAdr *Server) {
	re := regexp.MustCompile(`listening\-port\r\n\$[1-9]{0,4}\r\n[0-9]{0,4}`)

	if re.MatchString(req) {
		uri := strings.Split(re.FindString(req), "\r\n")

		server := *serverAdr

		server.replica = uri[2]
	}

	conn.Write([]byte("+OK\r\n"))
}

func processPsync(conn net.Conn, server Server) {
	emptyRDB, _ := hex.DecodeString("524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2")

	message := fmt.Sprintf(("+FULLRESYNC %s 0\r\n$%d\r\n%s"), server.replicationId, len(emptyRDB), emptyRDB)

	conn.Write([]byte(message))
}
