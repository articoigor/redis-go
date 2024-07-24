package main

import (
	"crypto/rand"
	"encoding/hex"
	"flag"
	"fmt"
	"math"
	"net"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const alphaNumeric = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

func main() {
	var port int

	var replicaMaster string

	serverRole := "master"

	flag.IntVar(&port, "port", 6379, "Port given as argument")

	flag.StringVar(&replicaMaster, "replicaof", "", "Role assigned to the current connection replica")

	flag.Parse()

	if replicaMaster != "" {
		serverRole = "slave"
	}

	l, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", port))

	if err != nil {
		fmt.Printf("Failed to bind to port %d", port)
		os.Exit(1)
	} else {
		fmt.Printf("Listening on port %d", port)
	}

	defer l.Close()

	handleConnections(l, serverRole, replicaMaster, port)
}

func handleConnections(listener net.Listener, serverRole, masterUri string, port int) {
	connCount := 0

	server := Server{role: serverRole, database: map[string]HashMap{}, replicationId: generateRepId(), replica: "?", offset: 0}

	for {
		sendHandshake(masterUri, port)

		conn, err := listener.Accept()

		fmt.Printf("replica port: %s", server.replica)

		if err != nil {
			fmt.Println("Error accepting connection:", err.Error())

			continue
		}

		connCount++

		fmt.Printf("Connection %d establised !", connCount)

		go handleCommand(conn, connCount, server)
	}
}

func sendHandshake(masterUri string, port int) {
	if len(masterUri) > 0 {
		master := strings.Split(masterUri, " ")

		masterAddress := strings.Join(master, ":")

		conn, err := net.Dial("tcp", masterAddress)

		if err == nil {
			conn.Write([]byte("*1\r\n$4\r\nPING\r\n"))
		}

		handshakeRes := make([]byte, 128)

		_, err = conn.Read(handshakeRes)

		if err == nil {
			sendReplconf(conn, strconv.Itoa(port))
		}
	}
}

func sendReplconf(conn net.Conn, port string) {
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

type HashMap struct {
	createdAt, expiry int64
	value             string
}

type Server struct {
	database                     map[string]HashMap
	role, replicationId, replica string
	offset                       int
}

func generateRepId() string {
	byteArray := make([]byte, 40)

	rand.Read(byteArray)

	for i, b := range byteArray {
		byteArray[i] = alphaNumeric[b%byte(len(alphaNumeric))]
	}
	return string(byteArray)
}

func handleCommand(conn net.Conn, connID int, server Server) {
	defer conn.Close()

	pingCount := 0

	for {
		pingCount++

		fmt.Printf("Connection %d: %d pings received\n", connID, pingCount)

		buf := make([]byte, 1024)

		_, err := conn.Read(buf)

		if err != nil {
			fmt.Println("Error reading from connection:", err.Error())
			break
		}

		data := strings.Split(string(buf), "\r\n")

		processRequest(data, string(buf), server, conn)
	}
}

func processRequest(data []string, req string, server Server, conn net.Conn) {
	endpoint := data[2]

	hashMap := server.database

	switch endpoint {
	case "ECHO":
		conn.Write([]byte(fmt.Sprintf(("+%s\r\n"), data[4])))
	case "PING":
		conn.Write([]byte("+PONG\r\n"))
	case "GET":
		processGetRequest(data, hashMap, conn)
	case "INFO":
		processInfoRequest(server, conn)
	case "SET":
		processSetRequest(data, req, hashMap, conn, server)
	case "REPLCONF":
		processReplconf(conn, req, server)
	case "PSYNC":
		processPsync(conn, server)
	default:
		fmt.Println("Invalid command informed")
	}
}

func processReplconf(conn net.Conn, req string, server Server) {
	re := regexp.MustCompile(`listening\-port\r\n\$[1-9]{0,4}\r\n[0-9]{0,4}`)

	if re.MatchString(req) && server.role == "master" {
		uri := strings.Split(re.FindString(req), "\r\n")

		server.replica = uri[2]

		fmt.Printf("replica after setting: %s", server.replica)
	}

	conn.Write([]byte("+OK\r\n"))
}

func processPsync(conn net.Conn, server Server) {
	emptyRDB, _ := hex.DecodeString("524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2")

	message := fmt.Sprintf(("+FULLRESYNC %s 0\r\n$%d\r\n%s"), server.replicationId, len(emptyRDB), emptyRDB)

	conn.Write([]byte(message))
}

func processInfoRequest(server Server, conn net.Conn) {
	str := fmt.Sprintf("role:%s\r\nmaster_replid:%s\r\nmaster_repl_offset:%d", server.role, server.replicationId, server.offset)

	message := fmt.Sprintf("$%d\r\n%s", len(str), str)

	_, err := conn.Write([]byte(fmt.Sprintf("%s\r\n", message)))

	if err != nil {
		fmt.Println("Error writing to connection:", err.Error())
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

func retrieveTimePassed(mapObj HashMap) int64 {
	milli := float64(time.Now().UnixMilli())

	createdAt := float64(mapObj.createdAt)

	return int64(math.Abs(milli - createdAt))
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
		fmt.Println(server.replica)
		fmt.Println(server.replicationId)

		propagateToReplica(server.replica, key, hashValue.value)
	}
}

func propagateToReplica(subscriber, key, value string) {
	fmt.Printf("Starting propagation of SET method on replica (port %s) !", subscriber)

	conn, err := net.Dial("tcp", fmt.Sprintf("localhost:%s", subscriber))

	if err != nil {
		fmt.Println("Error dialing to subscriber:", err.Error())
	}

	message := fmt.Sprintf("*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(key), key, len(value), value)

	_, err = conn.Write([]byte(message))

	if err != nil {
		fmt.Println("Error writing to connection:", err.Error())
	}
}
