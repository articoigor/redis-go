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

	conn, _ := l.Accept()

	bytes := make([]byte, 256)

	_, _ = conn.Read(bytes)

	req := string(bytes)

	fmt.Println(req)

	conn.Write([]byte("+PONG\r\n"))
}
