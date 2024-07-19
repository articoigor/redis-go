package main

import (
	"bufio"
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

	fmt.Println("Listening on 0.0.0.0:6379")

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Failed to accept connection")
			continue
		}

		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()

	reader := bufio.NewReader(conn)

	for {
		req, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println("Failed to read from connection:", err)
			return
		}

		req = strings.TrimSpace(req)
		fmt.Println("Received:", req)

		if req == "PING" {
			_, err := conn.Write([]byte("+PONG\r\n"))
			if err != nil {
				fmt.Println("Failed to write to connection:", err)
				return
			}
		} else {
			_, err := conn.Write([]byte("-ERR unknown command\r\n"))
			if err != nil {
				fmt.Println("Failed to write to connection:", err)
				return
			}
		}
	}
}
