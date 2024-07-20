package main

import (
	"fmt"
	"net"
	"os"
)

func main() {
	// Bind to all interfaces on port 6379
	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil { // [Highlight: Error message detail]
		fmt.Println("Failed to bind to port 6379:", err) // [Highlight: Added err message]
		os.Exit(1)
	}
	defer l.Close() // [Highlight: Ensure listener is closed]

	fmt.Println("Listening on 0.0.0.0:6379") // [Highlight: Added informational log]
	handleConnections(l)
}

func handleConnections(listener net.Listener) {
	connCount := 0
	for {
		conn, err := listener.Accept()
		if err != nil { // [Highlight: Improved error handling]
			fmt.Println("Error accepting connection:", err.Error()) // [Highlight: Added err message]
			continue                                                // [Highlight: Skip this connection and continue]
		}

		connCount++
		fmt.Printf("Connection %d established!\n", connCount)
		go handlePings(conn, connCount) // [Highlight: Pass connCount to handlePings]
	}
}

func handlePings(conn net.Conn, connID int) { // [Highlight: Added connID parameter]
	defer conn.Close() // [Highlight: Ensure connection is closed]
	pingCount := 0

	for {
		pingCount++
		fmt.Printf("Connection %d: %d pings received\n", connID, pingCount) // [Highlight: Added connID to log]

		buf := make([]byte, 1024)
		_, err := conn.Read(buf)
		if err != nil { // [Highlight: Error handling on read]
			fmt.Println("Error reading from connection:", err.Error()) // [Highlight: Added err message]
			break                                                      // [Highlight: Exit loop on error]
		}

		_, err = conn.Write([]byte("+PONG\r\n"))
		if err != nil { // [Highlight: Error handling on write]
			fmt.Println("Error writing to connection:", err.Error()) // [Highlight: Added err message]
			break                                                    // [Highlight: Exit loop on error]
		}
	}
}
