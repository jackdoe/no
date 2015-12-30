package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
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

	files, _ := filepath.Glob(args[1] + "/*.srl")
	log.Printf("reading %d files", len(files))

	var content [][]byte
	for _, file := range files {
		c, err := ioutil.ReadFile(file)
		if err != nil {
			log.Fatal("failed to read file", file, err)
		}

		content = append(content, c)
	}

	var total int64
	var wg sync.WaitGroup
	send := func() {
		defer wg.Done()

		log.Println("connection to", args[0])
		conn, err := grpc.Dial(args[0], grpc.WithInsecure())
		defer conn.Close()
		if err != nil {
			log.Fatal("failed to connect", err)
		}

		client := pb.NewIndexerClient(conn)

		var msg pb.Message
		var frame pb.Frame
		msg.Header = &pb.Header{Tags: []string{"a", "b", "c", "d", "e", "f"}}
		msg.Frames = append(msg.Frames, &frame)

		for _, c := range content {
			frame.Data = c
			_, err = client.IndexMessage(context.Background(), &msg)
			if err != nil {
				log.Fatal("failed to send message", err)
			}

			atomic.AddInt64(&total, 1)
			if atomic.LoadInt64(&total)%100 == 0 {
				log.Printf("sent %d files", total)
			}
		}
	}

	start := time.Now()
	for {
		for i := 0; i < 16; i++ {
			wg.Add(1)
			go send()
		}

		wg.Wait()
	}

	log.Printf("sent %d messages in %.2fs", total, time.Since(start).Seconds())
}
