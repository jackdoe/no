package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	pb "github.com/jackdoe/no/go/grpc/indexer/api"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

func main() {
	flag.Parse()
	args := flag.Args()
	if len(args) < 2 {
		fmt.Println("usage `go run client.go localhost:8004 path_to_srl_files`")
		os.Exit(1)
	}

	log.Println("connection to", args[0])
	conn, err := grpc.Dial(args[0], grpc.WithInsecure())
	defer conn.Close()
	if err != nil {
		log.Fatal("failed to connect", err)
	}

	var wg sync.WaitGroup
	files, _ := filepath.Glob(args[1] + "/*.srl")
	total, bytes := 0, 0

	send := func() {
		defer wg.Done()

		client := pb.NewIndexerClient(conn)

		var msg pb.Message
		msg.Header = &pb.Header{}
		msg.Frames = append(msg.Frames, &pb.Frame{[]byte(""), nil})

		for _, file := range files {
			msg.Frames[0].Data, err = ioutil.ReadFile(file)
			_, err = client.IndexMessage(context.Background(), &msg)
			if err != nil {
				log.Fatal("failed to send message", err)
			}

			// total++
			// bytes += len(msg.Frames[0].Data)
			// if total%100 == 0 {
			// log.Printf("sent %d files of size %d", total, bytes)
			// }
		}
	}

	start := time.Now()

	for i := 0; i < 3; i++ {
		wg.Add(1)
		go send()
		total += len(files)
	}

	wg.Wait()
	log.Printf("sent %d messages of size %d in %.2fs", total, bytes, time.Since(start).Seconds())
}
