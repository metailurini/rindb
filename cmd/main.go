package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/metailurini/rindb"
)

func main() {
	db, err := rindb.InitRinDB()
	if err != nil {
		fmt.Println("Error initializing database:", err)
		return
	}
	fmt.Println("rindb started. Commands: put <key> <value>, get <key>, remove <key>, range <start> <end>, exit")
	defer db.Close()

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
		case "range":
			if len(parts) != 3 {
				fmt.Println("Usage: range <start> <end>")
				continue
			}
			iter, err := db.IRange(rindb.Bytes(parts[1]), rindb.Bytes(parts[2]))
			if err != nil {
				fmt.Println("Error:", err)
				continue
			}
			for iter.HasNext() {
				rec, err := iter.Next()
				if err != nil {
					fmt.Println("Error:", err)
					break
				}
				fmt.Printf("%s:%s\n", rec.GetKey(), rec.GetValue())
			}
			rindb.CloseIterator(iter)
		case "exit":
			return
		default:
			fmt.Println("Unknown command")
		}
	}
}
