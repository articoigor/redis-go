package main

import (
	"fmt"
	"math"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

func main() {
	l, err := net.Listen("tcp", "0.0.0.0:6379")

	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}

	defer l.Close()

	handleConnections(l)
}

func handleConnections(listener net.Listener) {
	connCount := 0

	for {
		conn, err := listener.Accept()

		if err != nil {
			fmt.Println("Error accepting connection:", err.Error())

			continue
		}

		connCount++

		fmt.Printf("Connection %d establised !", connCount)

		go handleCommand(conn, connCount)
	}
}

type HashMap struct {
	createdAt, expiry int64
	value             string
}

func handleCommand(conn net.Conn, connID int) {
	defer conn.Close()

	hashMap := map[string]HashMap{}

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
		fmt.Println(data[2])

		returnMessage := processRequest(data, hashMap)

		fmt.Println(returnMessage)

		_, err = conn.Write([]byte(fmt.Sprintf("%s\r\n", returnMessage)))

		if err != nil {
			fmt.Println("Error writing to connection:", err.Error())
		}
	}
}

func processRequest(data []string, hashMap map[string]HashMap) string {
	endpoint := data[2]

	switch endpoint {
	case "ECHO":
		return data[4]
	case "PING":
		return "+" + "PONG"
	case "GET":
		return processGetRequest(data, hashMap)
	case "SET":
		return processSetRequest(data, hashMap)
	default:
		return ""
	}
}

func processGetRequest(data []string, hashMap map[string]HashMap) string {
	key := data[4]

	mapObj := hashMap[key]

	timeSpan := retrieveTimePassed(mapObj)

	message := mapObj.value

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

func processSetRequest(data []string, hashMap map[string]HashMap) string {
	now := time.Now()

	expiryVal := 0

	if data[8] == "px" {
		expiryVal, _ = strconv.Atoi(data[10])
	}

	hashValue := HashMap{value: data[6], createdAt: now.UnixMilli(), expiry: int64(expiryVal)}

	key := data[4]

	hashMap[key] = hashValue

	return "+OK"
}
