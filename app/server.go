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

	for i := 0; i < 2; i++ {
		bytes := make([]byte, 256)
		_, err := conn.Read(bytes)
		if err != nil {
			fmt.Println("Failed to read from connection")
			return
		}

		req := string(bytes)
		fmt.Println(req)

		if req[:4] == "PING" {
			conn.Write([]byte("+PONG\r\n"))
		} else {
			conn.Write([]byte("-ERR unknown command\r\n"))
		}
	}
}
