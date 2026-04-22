package main

import (
	"fmt"
	"net"
	"os"
	"strings"
)

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	listener, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	for {
		conn, err := listener.Accept()

		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		go handleconnection(conn)
	}
}

func handleconnection(conn net.Conn) {
	buf := make([]byte, 1024)
	for {
		n, err := conn.Read(buf)
		if err != nil {
			break
		}
		fmt.Printf("Received: %s", string(buf[:n]))
		// convert to string and split by CRLF
		input := string(buf[:n])
		parts := strings.Split(input, "\r\n")
		// check if string is less than 2 parts and continue
		// The key idea is REDIS commands start at part 2
		// The first part contain for an RESP ARRAY [number of characters]
		// The second is the [array length of the first part]
		// The third part contains the first command followed by the carriage return
		if len(parts) < 3 {
			continue
		}
		// convert to upper case
		command := strings.ToUpper(parts[2])
		// create switch case
		switch command {
		// switch PING and return PONG
		case "PING":
			conn.Write([]byte("+PONG\r\n"))

		// swtich ECHO and generate response of bulk string with length and argument
		case "ECHO":
			if len(parts) >= 5 {
				arg := parts[4]
				// Create bulk string with length and argument
				response := fmt.Sprintf("$%d\r\n%s\r\n", len(arg), arg)
				conn.Write([]byte(response))
			}
		// Return error if command is not PING or ECHO
		default:
			conn.Write([]byte("-ERR unknown command\r\n"))
		}
	}
}
