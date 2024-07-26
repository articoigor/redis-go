package main

import (
	"crypto/rand"
	"math"
	"strings"
	"time"
)

func (sv *ServerClient) processData(req string) []string {
	splitData := strings.Split(req, "\r\n")

	request := []string{}

	for _, line := range splitData {
		if !strings.Contains(line, "%") &&
			!strings.Contains(line, "*") &&
			!strings.Contains(line, "$") {
			request = append(request, line)
		}
	}

	return request
}

func generateRepId() string {
	byteArray := make([]byte, 40)

	rand.Read(byteArray)

	for i, b := range byteArray {
		byteArray[i] = alphaNumeric[b%byte(len(alphaNumeric))]
	}
	return string(byteArray)
}

func retrieveTimePassed(mapObj HashMap) int64 {
	milli := float64(time.Now().UnixMilli())

	createdAt := float64(mapObj.createdAt)

	return int64(math.Abs(milli - createdAt))
}
