package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
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
		fmt.Println("usage `go run client.go <FROM_ADDRESS:FROM_PORT(localhost:8005)> <TO_ADDRESS:TO_PORT(localhost:8004)>`")
		os.Exit(1)
	}

	receiver_address := args[1]
	sender_address := args[0]

	log.Println("connection to receiver", receiver_address)
	conn, err := grpc.Dial(receiver_address, grpc.WithInsecure())
	defer conn.Close()
	if err != nil {
		log.Fatal("failed to connect", err)
	}

	log.Println("connection to sender", sender_address)
	ServerAddr, err := net.ResolveUDPAddr("udp", sender_address)
	if err != nil {
		fmt.Println("Resolve UDP Addr error:", err)
		os.Exit(1)
	}

	log.Println("starting to listen", sender_address)
	ServerConn, err := net.ListenUDP("udp", ServerAddr)
	if err != nil {
		fmt.Println("Listen UDP error:", err)
		os.Exit(1)
	}
	log.Println("listening", sender_address)
	defer ServerConn.Close()

	var wg sync.WaitGroup
	r_total, r_bytes, s_total, s_bytes := 0, 0, 0, 0
	start := time.Now()

	send := func(in chan *pb.Message) {
		for {
			msg := <-in
			client := pb.NewIndexerClient(conn)
			_, err = client.IndexMessage(context.Background(), msg)
			if err != nil {
				log.Fatal("failed to send message", err)
			} else {
				s_total += 1
				s_bytes += msg.Size()
				fmt.Println("Sent:", msg)
			}
		}
	}

	receive := func(out chan *pb.Message) {
		for {
			buf := make([]byte, 64000)
			n, addr, err := ServerConn.ReadFromUDP(buf)
			if err != nil {
				fmt.Println("Read from UDP Error:", err)
			} else {
				r_bytes += n
				r_total += 1
				srl := buf[0:n]
				fmt.Println("Received", string(srl), "from", addr)
				msg := repack(srl)
				if msg != nil {
					out <- msg
				} else {
					fmt.Println("Couldn't unpack:", srl)
				}
			}
		}
	}

	cm := make(chan *pb.Message)
	go send(cm)
	go receive(cm)

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c

	wg.Wait()
	t := time.Since(start).Seconds()
	log.Printf("received %d messages of size %d in %.2fs", r_total, r_bytes, t)
	log.Printf("sent %d messages of size %d in %.2fs", s_total, s_bytes, t)
}

func repack(srl []byte) *pb.Message {
	var msg pb.Message

	timestamp := time.Now().Unix()
	tags := []string{"TAG1", "TAG2", "TAG3"}
	msg.Header = &pb.Header{timestamp, tags}
	msg.Frames = append(msg.Frames, &pb.Frame{[]byte(""), nil})
	msg.Frames[0].Data = srl
	return &msg
}
