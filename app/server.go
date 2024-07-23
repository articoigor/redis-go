package main

import (
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

func main() {
	var port int

	var serverRole string

	flag.IntVar(&port, "port", 6379, "Port given as argument")

	flag.StringVar(&serverRole, "replicaof", "master", "Role assigned to the current connection replica")

	flag.Parse()

	if serverRole != "master" {
		serverRole = "slave"
	}

	l, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", port))

	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}

	defer l.Close()

	handleConnections(l, serverRole)
}

func handleConnections(listener net.Listener, serverRole string) {
	connCount := 0

	for {
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

type HashMap struct {
	createdAt, expiry int64
	value             string
}

type Server struct {
	database map[string]HashMap
	role     string
}

func handleCommand(conn net.Conn, connID int, serverRole string) {
	defer conn.Close()

	server := Server{role: serverRole, database: map[string]HashMap{}}

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

		fmt.Println(returnMessage)

		_, err = conn.Write([]byte(fmt.Sprintf("%s\r\n", returnMessage)))

		if err != nil {
			fmt.Println("Error writing to connection:", err.Error())
		}
	}
}

func processRequest(data []string, req string, server Server) string {
	endpoint := data[2]

	hashMap := server.database

	switch endpoint {
	case "ECHO":
		return "+" + data[4]
	case "PING":
		return "+" + "PONG"
	case "GET":
		return processGetRequest(data, hashMap)
	case "INFO":
		return fmt.Sprintf("$%d\r\nrole:%s\r\n", len(server.role), server.role)
	case "SET":
		return processSetRequest(data, req, hashMap)
	default:
		return ""
	}
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
