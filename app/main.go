package main

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

type CreateGetSet struct {
	value     string
	expiresAt int64 //Unix timestamp in milliseconds
}

var db = make(map[string]CreateGetSet)
var dbArray = make(map[string][]string)

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
		// fmt.Printf("Received: %s", string(buf[:n]))
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
				conn.Write([]byte(parseBulkString(arg)))
			}
		// switch SET contains key and value
		case "SET":
			if len(parts) >= 7 {
				// get the key from the command
				setKey := parts[4]
				// get the value from the command
				setValue := parts[6]
				var expiresAt int64 = 0

				// get the command key of the command at parts 8
				if len(parts) >= 11 {
					ttlType := strings.ToUpper(parts[8])
					ttlValueStr := parts[10]

					ttlValue, _ := strconv.Atoi(ttlValueStr)

					switch ttlType {
					case "EX":
						expiresAt = time.Now().Add(time.Duration(ttlValue) * time.Second).UnixMilli()
					case "PX":
						expiresAt = time.Now().Add(time.Duration(ttlValue) * time.Millisecond).UnixMilli()
					}
				}
				// what should be done if it is EX or PX
				// store the key value pair to a map
				db[setKey] = CreateGetSet{
					value:     setValue,
					expiresAt: expiresAt,
				}
				// get the duration of the respective command at parts 10
				// if time expires, remove the key value pair from the datamap
				// return OK as a Simple string
				conn.Write([]byte(parseSimpleString("OK")))
			}
		// switch GET contains key
		case "GET":
			if len(parts) >= 5 {
				// get the key from the command
				getKey := parts[4]
				// query from the map the value of the key
				item, key_exists := db[getKey]
				if !key_exists {
					conn.Write([]byte(parseNullBulkString(-1)))
					return
				}
				// check expiry
				if item.expiresAt > 0 && time.Now().UnixMilli() > item.expiresAt {
					delete(db, getKey) //remove expired key
					conn.Write([]byte(parseNullBulkString(-1)))
					return
				}
				conn.Write([]byte(parseBulkString(item.value)))
			}
		case "RPUSH":
			// gets name of list
			totalRpushCount := len(parts)
			listName := parts[4]
			if len(parts) >= totalRpushCount {
				// for the range
				for i := 6; i < totalRpushCount; i++ {
					if i%2 == 0 {
						listValue := parts[i]
						dbArray[listName] = append(dbArray[listName], listValue)
					}
				}
				fmt.Printf("dbArray\t%v\n", dbArray)
				dbArrayCount := len(dbArray[listName])
				conn.Write([]byte(parseIntgers(dbArrayCount)))
				// return RESP integer

			}
		// Return error if command is not PING or ECHO
		default:
			conn.Write([]byte(parseSimpleErrors("ERR unknown command")))
		}
	}
}

func parseBulkString(bulk_string string) (response string) {
	response = fmt.Sprintf("$%d\r\n%s\r\n", len(bulk_string), bulk_string)
	return
}

func parseSimpleString(simple_string string) (response string) {
	response = fmt.Sprintf("+%s\r\n", simple_string)
	return
}

func parseNullBulkString(null_bulk_int int) (response string) {
	response = fmt.Sprintf("$%d\r\n", null_bulk_int)
	return
}

func parseSimpleErrors(simple_error string) (response string) {
	response = fmt.Sprintf("-%s\r\n", simple_error)
	return
}

func parseIntgers(dbArrayCount int) (response string) {
	response = fmt.Sprintf(":%d\r\n", dbArrayCount)
	return
}
