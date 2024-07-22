package main

import (
	"fmt"
	"net"
	"os"
	"strings"
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

func handleCommand(conn net.Conn, connID int) {
	defer conn.Close()

	hashMap := map[string]string{}

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

		fmt.Println(string(buf))

		data := strings.Split(string(buf), "\r\n")

		returnMessage := processRequest(data, hashMap)

		_, err = conn.Write([]byte(fmt.Sprintf("+%s\r\n", returnMessage)))

		if err != nil {
			fmt.Println("Error writing to connection:", err.Error())
		}
	}
}

func processRequest(data []string, hashMap map[string]string) string {
	endpoint := data[0]
	fmt.Println(endpoint)
	switch endpoint {
	case "ECHO":
		return data[4]
	case "PING":
		return "PONG"
	case "GET":
		return processGetRequest(data, hashMap)
	case "SET":
		return processSetRequest(data, hashMap)
	default:
		return ""
	}
}

func processGetRequest(data []string, hashMap map[string]string) string {
	value := hashMap[data[4]]

	return fmt.Sprintf("%d\r\n%s\r\n", len(value), value)
}

func processSetRequest(data []string, hashMap map[string]string) string {
	fmt.Printf("%s %s", data[4], data[5])
	key, value := data[4], data[5]

	fmt.Println("AQUI")

	fmt.Printf("Key: %s, Value: %s", key, value)
	hashMap[key] = value

	return "OK"
}
