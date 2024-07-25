package main

import (
	"crypto/rand"
	"fmt"
	"math"
	"net"
	"time"
)

func generateRepId() string {
	byteArray := make([]byte, 40)

	rand.Read(byteArray)

	for i, b := range byteArray {
		byteArray[i] = alphaNumeric[b%byte(len(alphaNumeric))]
	}
	return string(byteArray)
}

func propagateToReplica(server Server, key, value string) {
	replicaConn, err := net.Dial("tcp", fmt.Sprintf("0.0.0.0:%s", server.replica))

	if err != nil {
		fmt.Println("Error dialing to subscriber:", err.Error())

		return
	} else {
		message := fmt.Sprintf("*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(key), key, len(value), value)

		_, err = replicaConn.Write([]byte(message))

		if err != nil {
			fmt.Println("Error writing to connection:", err.Error())
		}
	}

	replicaConn.Close()
}

func retrieveTimePassed(mapObj HashMap) int64 {
	milli := float64(time.Now().UnixMilli())

	createdAt := float64(mapObj.createdAt)

	return int64(math.Abs(milli - createdAt))
}
