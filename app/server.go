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

	l, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", port))

	if err != nil {
		fmt.Printf("Failed to bind to port %d", port)
		os.Exit(1)
	} else {
		fmt.Printf("Listening on port %d", port)
	}

	handleConnections(l, &client)
}

func handleConnections(listener net.Listener, client *ServerClient) {
	for {
		conn, err := listener.Accept()

		if err != nil {
			fmt.Println("Error accepting connection:", err.Error())

			continue
		}

		client.conn = conn

		go handleCommand(conn, client)
	}
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

func handleCommand(conn net.Conn, server *ServerClient) {
	for {
		buf := make([]byte, 1024)

		_, err := conn.Read(buf)

		if err != nil {
			fmt.Println("Error reading from connection:", err.Error())
			break
		}

		data := strings.Split(string(buf), "\r\n")

		server.processRequest(data, string(buf), conn)
	}
}

func (sv *ServerClient) processRequest(data []string, req string, conn net.Conn) {
	endpoint := data[2]

	fmt.Printf("\r\nProcessing %s command\r\n", endpoint)

	switch endpoint {
	case "ECHO":
		conn.Write([]byte(fmt.Sprintf(("+%s\r\n"), data[4])))
	case "PING":
		conn.Write([]byte("+PONG\r\n"))
	case "GET":
		sv.processGetRequest(data)
	case "SET":
		sv.processSetRequest(data, req)
	case "INFO":
		sv.processInfoRequest()
	case "REPLCONF":
		sv.processReplconf(req)
	case "PSYNC":
		sv.replicas = append(sv.replicas, sv.conn)

		sv.processPsync()
	default:
		fmt.Println("Invalid command informed")
	}
}

func (sv *ServerClient) processGetRequest(data []string) {
	hashMap := sv.database

	key := data[4]

	mapObj := hashMap[key]

	timeSpan := retrieveTimePassed(mapObj)

	message := "+" + mapObj.value

	if mapObj.expiry > 0 && timeSpan > mapObj.expiry {
		delete(hashMap, key)

		message = "$-1"
	}

	_, err := sv.conn.Write([]byte(fmt.Sprintf("%s\r\n", message)))

	if err != nil {
		fmt.Println("Error writing to connection:", err.Error())
	}
}

func (sv *ServerClient) processSetRequest(data []string, req string) {
	hashMap := sv.database

	now := time.Now()

	expiryVal := 0

	regex, _ := regexp.Compile("px")

	if regex.MatchString(req) {
		expiryVal, _ = strconv.Atoi(data[10])
	}

	hashValue := HashMap{value: data[6], createdAt: now.UnixMilli(), expiry: int64(expiryVal)}

	key := data[4]

	hashMap[key] = hashValue

	_, err := sv.conn.Write([]byte(fmt.Sprintf("%s\r\n", "+OK")))

	if err != nil {
		fmt.Println("Error writing to connection:", err.Error())
	}

	if sv.role == "master" {
		conn := 1
		for _, replicaConn := range sv.replicas {
			fmt.Printf("Processing the %d replica command propagation", conn)

			repMessage := fmt.Sprintf("*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(key), key, len(hashValue.value), hashValue.value)

			fmt.Println(repMessage)
			_, err := replicaConn.Write([]byte(fmt.Sprintf("*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(key), key, len(hashValue.value), hashValue.value)))

			if err != nil {
				fmt.Println("Error writing to replica: ", err)
			}

			conn++
		}
	}
}

func (sv *ServerClient) processInfoRequest() {
	str := fmt.Sprintf("role:%s\r\nmaster_replid:%s\r\nmaster_repl_offset:%d", sv.role, sv.replicationId, sv.offset)

	message := fmt.Sprintf("$%d\r\n%s", len(str), str)

	_, err := sv.conn.Write([]byte(fmt.Sprintf("%s\r\n", message)))

	if err != nil {
		fmt.Println("Error writing to connection:", err.Error())
	}
}

func (sv *ServerClient) processReplconf(req string) string {
	re := regexp.MustCompile(`listening\-port\r\n\$[1-9]{0,4}\r\n[0-9]{0,4}`)

	if re.MatchString(req) {
		uri := strings.Split(re.FindString(req), "\r\n")

		sv.conn.Write([]byte("+OK\r\n"))

		return uri[2]
	}

	sv.conn.Write([]byte("+OK\r\n"))

	return ""
}

func (sv *ServerClient) processPsync() {
	emptyRDB, _ := hex.DecodeString("524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2")

	message := fmt.Sprintf(("+FULLRESYNC %s 0\r\n$%d\r\n%s"), sv.replicationId, len(emptyRDB), emptyRDB)

	sv.conn.Write([]byte(message))
}
