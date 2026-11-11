package main

import (
	"fmt"
	"net"
	"strconv"
)

// metricsAddress validates the optional listener before storage initialization.
func metricsAddress(apiAddr, rawPort string) (string, error) {
	port, err := strconv.Atoi(rawPort)
	if err != nil || port < 0 || port > 65535 {
		return "", fmt.Errorf("metrics port must be an integer from 0 to 65535")
	}
	if port == 0 {
		return "", nil
	}
	host, apiPort, err := net.SplitHostPort(apiAddr)
	if err != nil {
		return "", fmt.Errorf("metrics listener requires an API host:port address")
	}
	n, err := net.LookupPort("tcp", apiPort)
	if err != nil {
		return "", fmt.Errorf("metrics listener requires a valid API port")
	}
	if n == port {
		return "", fmt.Errorf("metrics port must differ from the API port")
	}
	return net.JoinHostPort(host, strconv.Itoa(port)), nil
}
