package main

import (
	"encoding/hex"
	"flag"
	"fmt"
	"log"
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

type Address struct {
	host string
	port int
}

type ServerClient struct {
	address             Address
	conn                net.Conn
	database            map[string]HashMap
	replicas            []net.Conn
	role, replicationId string
	offset              int
}

func main() {
	var port int

	var replicaMaster string

	serverRole := "master"

	flag.IntVar(&port, "port", 6379, "Port given as argument")

	flag.StringVar(&replicaMaster, "replicaof", "", "Role assigned to the current connection replica")

	if replicaMaster != "" {
		serverRole = "slave"

		sendHandshake(replicaMaster, port)
	}

	client := ServerClient{
		address:       Address{host: "0.0.0.0", port: port},
		role:          serverRole,
		database:      map[string]HashMap{},
		replicationId: generateRepId(),
		replicas:      make([]net.Conn, 0),
		offset:        0,
		conn:          nil,
	}

	flag.Parse()

	l, err := net.Listen("tcp", fmt.Sprintf("%s:%d", client.address.host, client.address.port))

	if err != nil {
		fmt.Printf("Failed to bind to port %d", port)
		os.Exit(1)
	} else {
		fmt.Printf("Listening on port %d", port)
	}

	fmt.Println("AAAAAAA")
	client.handleConnections(l)
}

func sendHandshake(masterAddress string, port int) {
	master := strings.Split(masterAddress, " ")

	dialAddress := fmt.Sprintf("%s:%s", master[0], master[1])

	handshakeConn, err := net.Dial("tcp", dialAddress)

	if err == nil {
		handshakeConn.Write([]byte("*1\r\n$4\r\nPING\r\n"))
	}

	handshakeRes := make([]byte, 256)

	_, err = handshakeConn.Read(handshakeRes)

	if err == nil {
		sendReplconf(handshakeConn, strconv.Itoa(port))
	}
}

func (sv *ServerClient) handleConnections(listener net.Listener) {
	for {
		conn, err := listener.Accept()

		if err != nil {
			fmt.Println("Error accepting connection:", err.Error())

			continue
		}

		sv.conn = conn

		go sv.handleCommand()
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

func (sv *ServerClient) handleCommand() {
	for {
		rawData := make([]byte, 1024)

		_, err := sv.conn.Read(rawData)

		if nil != err {
			log.Fatal("Error on reading: ", err.Error())
		}

		data := sv.processData(string(rawData))

		message := sv.processRequest(data, string(rawData))

		sv.conn.Write([]byte(message))
	}
}

func (sv *ServerClient) processRequest(data []string, rawRequest string) string {
	command := data[0]

	fmt.Printf("\r\nProcessing %s command\r\n", data[0])

	switch command {
	case "ECHO":
		return fmt.Sprintf(("+%s\r\n"), data[1])
	case "PING":
		return "+PONG\r\n"
	case "GET":
		return sv.processGetRequest(data)
	case "SET":
		for _, replica := range sv.replicas {
			fmt.Println(replica)
			_, err := sv.conn.Write([]byte("+AAAAAAAAAAAAA\r\n"))

			if err != nil {
				fmt.Println("Error propagating command to replica !")
			}
		}

		return sv.processSetRequest(data, rawRequest)
	case "INFO":
		return sv.processInfoRequest()
	case "REPLCONF":
		return "+OK\r\n"
	case "PSYNC":
		sv.replicas = append(sv.replicas, sv.conn)

		return sv.processPsync()
	default:
		return "Invalid command informed !"
	}
}

func (sv *ServerClient) processGetRequest(data []string) string {
	key := data[1]

	hash := sv.database[key]

	timeSpan := retrieveTimePassed(hash)

	message := "+" + hash.value

	if hash.expiry > 0 && timeSpan > hash.expiry {
		delete(sv.database, key)

		message = "$-1"
	}

	return fmt.Sprintf("%s\r\n", message)
}

func (sv *ServerClient) processSetRequest(data []string, rawRequest string) string {
	key, value := data[1], data[2]

	now := time.Now()

	var expiryVal int64 = 0

	if strings.Contains(rawRequest, "px") {
		expiryVal, _ = strconv.ParseInt(data[4], 10, 64)
	}

	hashValue := HashMap{value: value, createdAt: now.UnixMilli(), expiry: expiryVal}

	sv.database[key] = hashValue

	return fmt.Sprintf("%s\r\n", "+OK")
}

func (sv *ServerClient) processInfoRequest() string {
	str := fmt.Sprintf("role:%s\r\nmaster_replid:%s\r\nmaster_repl_offset:%d", sv.role, sv.replicationId, sv.offset)

	message := fmt.Sprintf("$%d\r\n%s", len(str), str)

	return fmt.Sprintf("%s\r\n", message)
}

func (sv *ServerClient) processPsync() string {
	emptyRDB, _ := hex.DecodeString("524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2")

	return fmt.Sprintf(("+FULLRESYNC %s 0\r\n$%d\r\n%s"), sv.replicationId, len(emptyRDB), emptyRDB)
}
