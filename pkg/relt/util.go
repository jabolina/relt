package relt

import (
	crand "crypto/rand"
	"fmt"
	"net"
)

// Generates a random 128-bit UUID, panic if not possible.
func GenerateUID() string {
	buf := make([]byte, 16)
	if _, err := crand.Read(buf); err != nil {
		panic(fmt.Errorf("failed generating uid: %v", err))
	}
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%12x",
		buf[0:4],
		buf[4:6],
		buf[6:8],
		buf[8:10],
		buf[10:16])
}

// Generates a random valid address to listen into.
func GenerateRandomIP() (string, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	defer listener.Close()

	if err != nil {
		return "", err
	}

	return listener.Addr().String(), nil
}
