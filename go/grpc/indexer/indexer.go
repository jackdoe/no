package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/Shopify/sarama"
	"github.com/google/flatbuffers/go"
	pb "github.com/jackdoe/no/go/grpc/indexer/api"
	fb "github.com/jackdoe/no/go/grpc/indexer/format"

	_ "net/http/pprof"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

const anyTag = "ANY"
const maxTagLength = 40
const maxTagsInMessage = 256
const persistedTopic = "persisted"
const indexTopic = "index"

type reverseIndex map[string]map[int32]*deque

type indexBuilderMessage struct {
	tags      []string
	partition int32
	offset    int64
}

type indexDumperMessage struct {
	index reverseIndex
	t     time.Time
}

type indexerServer struct {
	producer      sarama.SyncProducer
	appendIndexCh chan<- *indexBuilderMessage
}

func (isrv *indexerServer) IndexMessage(ctx context.Context, msg *pb.Message) (*pb.Response, error) {
	tags := msg.Header.GetTags()
	if len(tags) > maxTagsInMessage {
		return nil, fmt.Errorf("too many tags in message %d", len(tags))
	}

	for _, tag := range tags {
		if len(tag) > maxTagLength {
			return nil, fmt.Errorf("too long tag name %d", len(tag))
		}
	}

	frames := msg.GetFrames()
	for i := 0; i < len(frames)-1; i++ {
		if frames[i].GetName() > frames[i+1].GetName() {
			return nil, fmt.Errorf("frames must be sorted")
		}
	}

	uuid := msg.Header.GetUuid()
	value, err := msg.Marshal()
	if err != nil {
		return nil, fmt.Errorf("failed to marshal message: %v", err)
	}

	start := time.Now()
	partition, offset, err := isrv.producer.SendMessage(&sarama.ProducerMessage{
		Topic: persistedTopic,
		Key:   sarama.StringEncoder(uuid),
		Value: sarama.ByteEncoder(value),
	})

	if err != nil {
		return nil, fmt.Errorf("failed to store message: %v", err)
	}

	statIncrementTook(&stat.sendMessageTook, start)
	statIncrementSize(&stat.sendMessageSize, len(value))

	// partition, offset := int32(0), int64(0)
	isrv.appendIndexCh <- &indexBuilderMessage{
		tags:      tags,
		partition: partition,
		offset:    offset,
	}

	return &pb.Response{}, nil
}

func newProducer(brokerList []string) sarama.SyncProducer {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForLocal
	config.Producer.Retry.Max = 10

	producer, err := sarama.NewSyncProducer(brokerList, config)
	if err != nil {
		log.Fatalln("Failed to start Sarama producer:", err)
	}

	return producer
}

func startIndexBuilder(dumperCh chan<- *indexDumperMessage, tickCh <-chan time.Time, wg *sync.WaitGroup) chan<- *indexBuilderMessage {
	wg.Add(1)
	builderCh := make(chan *indexBuilderMessage, 1)

	go func() {
		defer wg.Done()
		var elapsed int64
		index := make(reverseIndex)
		cnt := 0

		for {
			select {
			case t := <-tickCh:
				log.Printf("index builder: got new tick for %d, collected %d messages, took %d ms", t.Unix(), cnt, elapsed/1000000)
				dumperCh <- &indexDumperMessage{index, t.UTC()}
				index = make(reverseIndex)
				elapsed = 0
				cnt = 0

			case msg := <-builderCh:
				if msg == nil {
					log.Printf("exiting index builder")
					t := <-tickCh
					log.Printf("index builder: got new tick for %d, collected %d messages, took %d ms", t.Unix(), cnt, elapsed/1000000)
					dumperCh <- &indexDumperMessage{index, t.UTC()}
					close(dumperCh)
					return
				}

				start := time.Now()
				msg.tags = append(msg.tags, anyTag)
				for _, tag := range msg.tags {
					sidx, ok := index[tag]
					if !ok {
						sidx = make(map[int32]*deque)
						index[tag] = sidx
					}

					d, ok := sidx[msg.partition]
					if !ok {
						d = newDeque()
						sidx[msg.partition] = d
					}

					d.Append(msg.offset)
					cnt++
				}

				elapsed += time.Since(start).Nanoseconds()
			}
		}
	}()

	return builderCh
}

func startIndexDumper(wg *sync.WaitGroup) chan<- *indexDumperMessage {
	ch := make(chan *indexDumperMessage, 300) // 5 min
	wg.Add(1)

	go func() {
		defer wg.Done()

		for {
			msg := <-ch
			if msg == nil {
				log.Println("exiting index dumper")
				return
			}

			start := time.Now()
			t, index := msg.t, msg.index
			log.Printf("index dumper got index for %d (%s)", t.Unix(), t.String())

			bytes := 0
			total := 0
			var tags []string
			for tag, partitions := range index {
				tags = append(tags, tag)

				bytes += len(tag)
				for _, d := range partitions {
					total += d.size
					bytes += d.size * flatbuffers.SizeInt64
				}
			}

			sort.Strings(tags)

			var fbtags []flatbuffers.UOffsetT
			builder := flatbuffers.NewBuilder(bytes)

			for _, tag := range tags {
				name := builder.CreateString(tag)

				cnt := 0
				builder.StartVector(flatbuffers.SizeInt64, 0, 0)
				for partition, d := range index[tag] {
					for di := d.head; di != nil; di = di.next {
						for i := 0; i < di.cnt; i++ {
							builder.PrependInt64(encodePartitionAndOffset(partition, di.vals[i]))
							cnt++
						}
					}
				}

				offsetsVector := builder.EndVector(cnt)

				fb.TagStart(builder)
				fb.TagAddName(builder, name)
				fb.TagAddOffsets(builder, offsetsVector)
				fbtags = append(fbtags, fb.TagEnd(builder))
			}

			fb.IndexStartTagsVector(builder, len(fbtags))
			for _, offset := range fbtags {
				builder.PrependUOffsetT(offset)
			}

			tagsVector := builder.EndVector(len(fbtags))

			fb.IndexStart(builder)
			fb.IndexAddTags(builder, tagsVector)
			builder.Finish(fb.IndexEnd(builder))

			buf := builder.FinishedBytes()

			statIncrementSize(&stat.sendIndexSize, len(buf))

			// _, _, err := isrv.producer.SendMessage(&sarama.ProducerMessage{
			// Topic: indexTopic,
			// Key:   sarama.StringEncoder(t.Unix()),
			// Value: sarama.ByteEncoder(buf),
			// })

			// if err != nil {
			// log.Printf("failed to store message: %v", err)
			// }

			elapsed := time.Since(start)
			log.Printf("finished serializing index for %d, %d tags, %d offsets, size %db, took: %d ms",
				t.Unix(), len(tags), total, len(buf), elapsed.Nanoseconds()/1000000)
		}
	}()

	return ch
}

func main() {
	var srv = flag.String("srv", "0.0.0.0:8004", "server socket")
	var netprofile = flag.Bool("netprofile", false, "open socket for remote profiling")
	var brokers = flag.String("brokers", "./brokers", "file containing Kafka brokers to connect to")
	flag.Parse()

	if *netprofile {
		go func() {
			addr := "0.0.0.0:6061"
			log.Println("start debug HTTP server", addr)
			log.Println(http.ListenAndServe(addr, nil))
		}()
	}

	statInit()

	wg := &sync.WaitGroup{}
	tickCh := time.Tick(time.Second)
	indexDumperCh := startIndexDumper(wg)
	indexBuilderCh := startIndexBuilder(indexDumperCh, tickCh, wg)

	brokerContent, err := ioutil.ReadFile(*brokers)
	if err != nil {
		log.Fatalf("failed to read file '%s': %v", brokers, err)
	}

	brokerList := strings.Split(string(brokerContent), "\n")
	log.Printf("kafka brokers: %s", strings.Join(brokerList, ", "))

	// var producer *sarama.SyncProducer
	producer := newProducer(brokerList)
	defer producer.Close()

	log.Println("listen to", *srv)
	conn, err := net.Listen("tcp", *srv)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer(grpc.MaxConcurrentStreams(3))
	pb.RegisterIndexerServer(grpcServer, &indexerServer{producer, indexBuilderCh})

	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-c
		log.Println("stopping gRPC")
		grpcServer.Stop()
	}()

	grpcServer.Serve(conn)
	close(indexBuilderCh)

	log.Println("waiting completion of goroutines")
	wg.Wait()
	log.Println("bye")
}

func encodePartitionAndOffset(partition int32, offset int64) int64 {
	return int64((int64(partition&0x000000FF) << 56) | (offset & 0x00FFFFFFFFFFFFFFF))
}
