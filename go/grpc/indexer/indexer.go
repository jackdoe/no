package main

import (
	"flag"
	"log"
	"net"
	"net/http"
	"os"

	pb "github.com/jackdoe/no/go/grpc/indexer/api"

	_ "net/http/pprof"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

var rootDir string

type indexerServer struct{}

func (isrv *indexerServer) IndexMessage(ctx context.Context, msg *pb.Message) (*pb.Response, error) {
	log.Println("new message", msg)
	return &pb.Response{}, nil
}

func main() {
	var root = flag.String("root", ".", "root directory")
	var srv = flag.String("srv", "0.0.0.0:8004", "server socket")
	var netprofile = flag.Bool("netprofile", false, "open socket for remote profiling")
	flag.Parse()

	if *netprofile {
		go func() {
			addr := "0.0.0.0:6061"
			log.Println("start debug HTTP server", addr)
			log.Println(http.ListenAndServe(addr, nil))
		}()
	}

	rootDir = *root
	os.MkdirAll(rootDir, 0775)
	log.Println("set rootDir to", rootDir)

	log.Println("listen to", *srv)
	conn, err := net.Listen("tcp", *srv)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterIndexerServer(grpcServer, &indexerServer{})
	grpcServer.Serve(conn)
}
