package main

import (
	"crypto/rand"
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

	for {
		sendHandshake(masterUri, port)

		conn, err := listener.Accept()

		if err != nil {
			fmt.Println("Error accepting connection:", err.Error())

			continue
		}

		connCount++

		fmt.Printf("Connection %d establised !", connCount)

		go handleCommand(conn, connCount, serverRole)
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
			sendReplconf(conn, port)

			replconfRes := make([]byte, 128)

			_, err = conn.Read(replconfRes)

			if err == nil {
				sendPsync(conn)
			}
		}
	}
}

func sendReplconf(conn net.Conn, port int) {
	strPort := strconv.Itoa(port)

	confirmationStr := fmt.Sprintf("*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$%d\r\n%s\r\n*3\r\n*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n*3\r\n", len(strPort), strPort)

	conn.Write([]byte(confirmationStr))

	// firstResponse := make([]byte, 128)

	// _, err := conn.Read(firstResponse)

	// if err == nil {
	// 	conn.Write([]byte("*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n*3\r\n"))
	// }
}

func sendPsync(conn net.Conn) {
	confirmationStr := "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n"

	conn.Write([]byte(confirmationStr))
}

type HashMap struct {
	createdAt, expiry int64
	value             string
}

type Server struct {
	database            map[string]HashMap
	role, replicationId string
	offset              int
}

func generateRepId() string {
	byteArray := make([]byte, 40)

	rand.Read(byteArray)

	for i, b := range byteArray {
		byteArray[i] = alphaNumeric[b%byte(len(alphaNumeric))]
	}
	return string(byteArray)
}

func handleCommand(conn net.Conn, connID int, serverRole string) {
	defer conn.Close()

	server := Server{role: serverRole, database: map[string]HashMap{}, replicationId: generateRepId(), offset: 0}

	pingCount := 0

	for {
		pingCount++

		fmt.Printf("Connection %d: %d pings received\n", connID, pingCount)

		buf := make([]byte, 1024)

		_, err := conn.Read(buf)

		fmt.Println(string(buf))

		if err != nil {
			fmt.Println("Error reading from connection:", err.Error())
			break
		}

		data := strings.Split(string(buf), "\r\n")

		returnMessage := processRequest(data, string(buf), server)

		_, err = conn.Write([]byte(fmt.Sprintf("%s\r\n", returnMessage)))

		if err != nil {
			fmt.Println("Error writing to connection:", err.Error())
		}
	}
}

func processRequest(data []string, req string, server Server) string {
	endpoint := data[2]

	hashMap := server.database

	fmt.Println(endpoint)

	switch endpoint {
	case "ECHO":
		return "+" + data[4]
	case "PING":
		return "+" + "PONG"
	case "GET":
		return processGetRequest(data, hashMap)
	case "INFO":
		return processInfoRequest(server)
	case "SET":
		return processSetRequest(data, req, hashMap)
	default:
		return ""
	}
}

func processInfoRequest(server Server) string {
	fmt.Println("chegou aqui")
	str := fmt.Sprintf("role:%s\r\nmaster_replid:%s\r\nmaster_repl_offset:%d", server.role, server.replicationId, server.offset)

	return fmt.Sprintf("$%d\r\n%s", len(str), str)
}

func processGetRequest(data []string, hashMap map[string]HashMap) string {
	key := data[4]

	mapObj := hashMap[key]

	timeSpan := retrieveTimePassed(mapObj)

	message := "+" + mapObj.value

	if mapObj.expiry > 0 && timeSpan > mapObj.expiry {
		delete(hashMap, key)

		message = "$-1"
	}

	return message
}

func retrieveTimePassed(mapObj HashMap) int64 {
	milli := float64(time.Now().UnixMilli())

	createdAt := float64(mapObj.createdAt)

	return int64(math.Abs(milli - createdAt))
}

func processSetRequest(data []string, req string, hashMap map[string]HashMap) string {
	now := time.Now()

	expiryVal := 0

	regex, _ := regexp.Compile("px")

	if regex.MatchString(req) {
		expiryVal, _ = strconv.Atoi(data[10])
	}

	hashValue := HashMap{value: data[6], createdAt: now.UnixMilli(), expiry: int64(expiryVal)}

	key := data[4]

	hashMap[key] = hashValue

	return "+OK"
}
