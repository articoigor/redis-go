package main

import (
	"fmt"
	"net"
	"os"
)

func main() {
	// Bind to all interfaces on port 6379
	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379:", err)
		os.Exit(1)
	}
	defer l.Close() // Ensure the listener is closed when the application exits

	fmt.Println("Listening on 0.0.0.0:6379")
	handleConnections(l)
}

func handleConnections(listener net.Listener) {
	connCount := 0
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection:", err.Error())
			continue // Skip this connection and continue accepting new connections
		}

		connCount++
		fmt.Printf("Connection %d established!\n", connCount)
		go handlePings(conn, connCount)
	}
}

func handlePings(conn net.Conn, connID int) {
	defer conn.Close() // Ensure the connection is closed when the function exits
	pingCount := 0

	for {
		pingCount++
		fmt.Printf("Connection %d: %d pings received\n", connID, pingCount)

		buf := make([]byte, 1024)
		_, err := conn.Read(buf)
		if err != nil {
			fmt.Println("Error reading from connection:", err.Error())
			break // Exit the loop and close the connection if there's an error
		}

		_, err = conn.Write([]byte("+PONG\r\n"))
		if err != nil {
			fmt.Println("Error writing to connection:", err.Error())
			break // Exit the loop and close the connection if there's an error
		}
	}
}
