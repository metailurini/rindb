package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/metailurini/rindb"
)

func main() {
	ctx := context.Background()
	db, err := rindb.InitRinDB(ctx)
	if err != nil {
		fmt.Println("Error initializing database:", err)
		return
	}
	fmt.Println("rindb started. Commands: put <key> <value>, get <key>, remove <key>, range <start> <end>, stats, exit")
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
			err := db.Put(ctx, rindb.Bytes(parts[1]), rindb.Bytes(parts[2]))
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
			value, err := db.Get(ctx, rindb.Bytes(parts[1]))
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
			err := db.Remove(ctx, rindb.Bytes(parts[1]))
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
			iter, err := db.IRange(ctx, rindb.Bytes(parts[1]), rindb.Bytes(parts[2]))
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
			if err := iter.Close(); err != nil {
				fmt.Println("Error:", err)
			}
		case "stats":
			s := db.Stats()
			fmt.Printf("MemtableBytes=%d SequenceNumber=%d WALBytes=%d WALRecords=%d SSTablesPerLevel=%v GetCalls=%d PutCalls=%d RemoveCalls=%d IRangeCalls=%d Flushes=%d\n",
				s.MemtableBytes, s.SequenceNumber, s.WALBytes, s.WALRecords, s.SSTablesPerLevel,
				s.GetCalls, s.PutCalls, s.RemoveCalls, s.IRangeCalls, s.Flushes)
		case "exit":
			return
		default:
			fmt.Println("Unknown command")
		}
	}
}
