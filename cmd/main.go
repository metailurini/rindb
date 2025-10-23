package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/metailurini/rindb"
)

func main() {
	cacheBytes := flag.Int64("cache-bytes", 0, "table cache byte budget")
	cacheShards := flag.Int("cache-shards", 0, "number of table cache shards")
	dbDir := flag.String("db-dir", "rindat", "database directory")
	flag.Parse()

	ctx := context.Background()
	var opts []rindb.Option
	if *cacheBytes > 0 {
		opts = append(opts, rindb.WithCacheBytes(*cacheBytes))
	}
	if *cacheShards > 0 {
		opts = append(opts, rindb.WithCacheShards(*cacheShards))
	}
	if dbDir != nil && *dbDir != "" {
		opts = append(opts, rindb.WithDatabaseDir(*dbDir))
	}
	db, err := rindb.InitRinDB(ctx, opts...)
	if err != nil {
		fmt.Println("Error initializing database:", err)
		return
	}
	fmt.Println("rindb started. Commands: put <key> <value>, get <key>, remove <key>, range <start> <end> [asc|desc], stats, exit")
	defer db.Close()

	scanner := bufio.NewScanner(os.Stdin)
	fmt.Print(">> ")
	for scanner.Scan() {
		input := scanner.Text()
		parts := strings.Fields(input)
		if len(parts) == 0 {
			fmt.Print(">> ")
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
			if len(parts) < 3 || len(parts) > 4 {
				fmt.Println("Usage: range <start> <end> [asc|desc]")
				continue
			}
			order := rindb.RangeAsc
			if len(parts) == 4 {
				switch strings.ToLower(parts[3]) {
				case "asc", "ascending":
					order = rindb.RangeAsc
				case "desc", "descending":
					order = rindb.RangeDesc
				default:
					fmt.Println("Usage: range <start> <end> [asc|desc]")
					continue
				}
			}

			opts := make([]rindb.RangeOption, 0, 1)
			if order == rindb.RangeDesc {
				opts = append(opts, rindb.IRangeOrder(order))
			}

			iter, err := db.IRange(ctx, rindb.Bytes(parts[1]), rindb.Bytes(parts[2]), opts...)
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
			fmt.Printf("MemtableBytes: %d\n", s.MemtableBytes)
			fmt.Printf("SequenceNumber: %d\n", s.SequenceNumber)
			fmt.Printf("ActiveSnapshots: %d\n", s.ActiveSnapshots)
			fmt.Printf("WALBytes: %d\n", s.WALBytes)
			fmt.Printf("WALRecords: %d\n", s.WALRecords)
			fmt.Printf("SSTablesPerLevel: %v\n", s.SSTablesPerLevel)
			fmt.Printf("GetCalls: %d\n", s.GetCalls)
			fmt.Printf("PutCalls: %d\n", s.PutCalls)
			fmt.Printf("RemoveCalls: %d\n", s.RemoveCalls)
			fmt.Printf("IRangeCalls: %d\n", s.IRangeCalls)
			fmt.Printf("Flushes: %d\n", s.Flushes)
			tc := db.TableCacheStats()
			fmt.Printf("TableCacheUsedBytes: %d\n", tc.UsedBytes)
			fmt.Printf("TableCacheHits: %d\n", tc.Hits)
			fmt.Printf("TableCacheMisses: %d\n", tc.Misses)
		case "exit":
			return
		default:
			fmt.Println("Unknown command")
		}
	}
}
