package main

import (
	"bufio"
	"fmt"
	"os"
	"rindb"
	"strings"
)

func main() {
	db, err := rindb.InitRinDB()
	if err != nil {
		fmt.Println("Error initializing database:", err)
		return
	}
	fmt.Println("rindb started. Commands: put <key> <value>, get <key>, remove <key>, exit")

	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		input := scanner.Text()
		parts := strings.Fields(input)
		if len(parts) == 0 {
			continue
		}

		switch parts[0] {
		case "put":
			if len(parts) != 3 {
				fmt.Println("Usage: put <key> <value>")
				continue
			}
			err := db.Put(rindb.Bytes(parts[1]), rindb.Bytes(parts[2]))
			if err != nil {
				fmt.Println("Error:", err)
			} else {
				fmt.Println("OK")
			}
		case "get":
			if len(parts) != 2 {
				fmt.Println("Usage: get <key>")
				continue
			}
			value, err := db.Get(rindb.Bytes(parts[1]))
			if err != nil {
				fmt.Println("Error:", err)
			} else {
				fmt.Println(string(value))
			}
		case "remove":
			if len(parts) != 2 {
				fmt.Println("Usage: remove <key>")
				continue
			}
			err := db.Remove(rindb.Bytes(parts[1]))
			if err != nil {
				fmt.Println("Error:", err)
			} else {
				fmt.Println("OK")
			}
		case "exit":
			return
		default:
			fmt.Println("Unknown command")
		}
	}
}
