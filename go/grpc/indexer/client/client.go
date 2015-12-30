package main

import (
	"flag"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	pb "github.com/jackdoe/no/go/grpc/indexer/api"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

func main() {
	var proc = flag.Int("proc", 8, "parallelizm")
	var srv = flag.String("srv", "localhost:8004", "destination host:port")
	var path = flag.String("path", ".", "path_to_srl_files")
	flag.Parse()

	if !flag.Parsed() {
		flag.PrintDefaults()
		os.Exit(1)
	}

	hosts := strings.Split(*srv, ",")
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
		src := rand.NewSource(time.Now().UnixNano())
		rnd := rand.New(src)

		var clients []pb.IndexerClient
		for _, host := range hosts {
			log.Println("connection to", host)
			conn, err := grpc.Dial(host, grpc.WithInsecure())
			defer conn.Close()
			if err != nil {
				log.Fatal("failed to connect", err)
			}

			clients = append(clients, pb.NewIndexerClient(conn))
		}

		var msg pb.Message
		var frame pb.Frame
		msg.Header = &pb.Header{Tags: []string{"a", "b", "c", "d", "e", "f"}}
		msg.Frames = append(msg.Frames, &frame)

		for {
			for _ = range content {
				c := content[rnd.Intn(len(content))]
				client := clients[rnd.Intn(len(hosts))]

				frame.Data = c
				if _, err := client.IndexMessage(context.Background(), &msg); err != nil {
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
