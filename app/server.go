package main

import (
	"fmt"
	"net"
	"os"
)

func main() {
	l, err := net.Listen("tcp", "0.0.0.0:6379")

	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}

	defer l.Close()

	go handleConnections(l)
}

func handleConnections(listener net.Listener) {
	connCount := 0

	for {
		connCount++

		conn, err := listener.Accept()

		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		} else {
			fmt.Printf("Connection %d establised !", connCount)
		}

		go handlePings(conn, connCount)
	}
}

func handlePings(conn net.Conn, connID int) {
	defer conn.Close()

	pingCount := 0

	for {
		pingCount++

		fmt.Printf("Connection %d: %d pings received\n", connID, pingCount)

		_, err := conn.Write([]byte("+PONG\r\n"))

		if err != nil {
			fmt.Println("Error writing to connection:", err.Error())
			break
		}
	}
}
