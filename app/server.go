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

		go handlePings(conn, connCount)
	}
}

func handlePings(conn net.Conn, connID int) {
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

		fmt.Println(string(buf))

		data := strings.Split(string(buf), "\r\n")

		returnMessage := processRequest(data)

		_, err = conn.Write([]byte(fmt.Sprintf("+%s\r\n", returnMessage)))

		if err != nil {
			fmt.Println("Error writing to connection:", err.Error())
		}
	}
}

func processRequest(data []string) string {
	endpoint := data[2]

	switch endpoint {
	case "ECHO":
		return data[4]
	case "PING":
		return "PONG"
	default:
		return ""
	}
}
