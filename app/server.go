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

	_, err = conn.Read(bytes)

	req := string(bytes)

	fmt.Println("*****")

	fmt.Println(req)

	fmt.Println("*****")

	conn.Write([]byte("+PONG\r\n"))
}
