package main

import (
	"flag"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	pb "github.com/jackdoe/no/go/grpc/indexer/api"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

func shuffle(slc [][]byte) {
	for i := 1; i < len(slc); i++ {
		r := rand.Intn(i + 1)
		if i != r {
			slc[r], slc[i] = slc[i], slc[r]
		}
	}
}

func main() {
	var proc = flag.Int("proc", 8, "parallelizm")
	var srv = flag.String("srv", "localhost:8004", "destination host:port")
	var path = flag.String("path", ".", "path_to_srl_files")
	flag.Parse()

	if !flag.Parsed() {
		flag.PrintDefaults()
		os.Exit(1)
	}

	files, _ := filepath.Glob(*path + "/*.srl")
	log.Printf("reading %d files", len(files))

	var content [][]byte
	for _, file := range files {
		c, err := ioutil.ReadFile(file)
		if err != nil {
			log.Fatal("failed to read file", file, err)
		}

		content = append(content, c)
	}

	shuffle(content)

	var total, bytes int64
	go func(ch <-chan time.Time) {
		for {
			<-ch
			t := atomic.SwapInt64(&total, 0)
			b := atomic.SwapInt64(&bytes, 0)
			log.Printf("sent %d %d bytes", t, b)
		}
	}(time.Tick(time.Second))

	var wg sync.WaitGroup
	send := func() {
		defer wg.Done()

		log.Println("connection to", *srv)
		conn, err := grpc.Dial(*srv, grpc.WithInsecure())
		defer conn.Close()
		if err != nil {
			log.Fatal("failed to connect", err)
		}

		client := pb.NewIndexerClient(conn)

		var msg pb.Message
		var frame pb.Frame
		msg.Header = &pb.Header{Tags: []string{"a", "b", "c", "d", "e", "f"}}
		msg.Frames = append(msg.Frames, &frame)

		for {
			for _, c := range content {
				frame.Data = c
				_, err = client.IndexMessage(context.Background(), &msg)
				if err != nil {
					log.Println("failed to send message", err)
				}

				atomic.AddInt64(&total, 1)
				atomic.AddInt64(&bytes, int64(len(c)))
			}
		}
	}

	log.Printf("spawning %d goroutines", *proc)
	for i := 0; i < *proc; i++ {
		wg.Add(1)
		go send()
	}

	wg.Wait()
}
