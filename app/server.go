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

	conn, err := l.Accept()

	bytes := make([]byte, 256)

	conn.Write(bytes)

	fmt.Println("*****")

	fmt.Println(string(bytes))

	fmt.Println("*****")

	conn.Write([]byte("+PONG\r\n"))
}
