package main

import (
	"fmt"
	"net"
	"os"
	"strings"
)

var db = make(map[string]string)

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
				// Create bulk string with length and argument as a bulk string
				response := fmt.Sprintf("$%d\r\n%s\r\n", len(arg), arg)
				conn.Write([]byte(response))
			}
		// switch SET contains key and value
		case "SET":
			if len(parts) >= 5 {
				// get the key from the command
				set_key := parts[4]
				// get the value from the command
				set_value := parts[6]
				// store the key value pair to a map
				db[set_key] = set_value
				// return OK as a Simple string
				response := fmt.Sprintf("+%s\r\n", "OK")
				conn.Write([]byte(response))
			}
		// switch GET contains key
		case "GET":
			if len(parts) >= 5 {
				// get the key from the command
				get_key := parts[4]
				// query from the map the value of the key
				query_get_value, key_exist := db[get_key]
				if key_exist {
					response := fmt.Sprintf("$%d\r\n%s\r\n", len(query_get_value), query_get_value)
					conn.Write([]byte(response))
				} else {
					response := fmt.Sprintf("$%d\r\n", -1)
					conn.Write([]byte(response))
					// return the value
				}
			}

		// Return error if command is not PING or ECHO
		default:
			conn.Write([]byte("-ERR unknown command\r\n"))
		}
	}
}
