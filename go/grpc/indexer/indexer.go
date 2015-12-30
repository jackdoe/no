package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"google.golang.org/grpc"

	"github.com/Shopify/sarama"
	"github.com/google/flatbuffers/go"
	pb "github.com/jackdoe/no/go/grpc/indexer/api"
	fb "github.com/jackdoe/no/go/grpc/indexer/format"

	_ "expvar"
	_ "net/http/pprof"

	"golang.org/x/net/trace"

	"golang.org/x/net/context"
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
	index   reverseIndex
	t       time.Time
	msgs    int
	offsets int
}

type indexerServer struct {
	producer      sarama.SyncProducer
	appendIndexCh chan<- *indexBuilderMessage
}

func (isrv *indexerServer) IndexMessage(ctx context.Context, msg *pb.Message) (*pb.Response, error) {
	statIncrementCnt(&stat.msgCnt)

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

	start := time.Now()
	value, err := msg.Marshal()
	statIncrementTook(&stat.msgSerializeTook, start)
	statIncrementSize(&stat.msgSendToKafkaSize, len(value))

	if err != nil {
		return nil, fmt.Errorf("failed to marshal message: %v", err)
	}

	start = time.Now()
	// partition, offset := int32(0), int64(0)
	partition, offset, err := isrv.producer.SendMessage(&sarama.ProducerMessage{
		Topic: persistedTopic,
		Value: sarama.ByteEncoder(value),
	})

	statIncrementTook(&stat.msgSendToKafkaTook, start)

	if err != nil {
		return nil, fmt.Errorf("failed to store message: %v", err)
	}

	start = time.Now()
	isrv.appendIndexCh <- &indexBuilderMessage{
		tags:      tags,
		partition: partition,
		offset:    offset,
	}

	statIncrementTook(&stat.msgSendToChTook, start)
	return &pb.Response{}, nil
}

func newKafkaClient(proc int, brokerList []string, hostname string) (sarama.Client, error) {
	sarama.MaxRequestSize = 100 * 1024 * 1024
	sarama.MaxResponseSize = 100 * 1024 * 1024

	config := sarama.NewConfig()
	config.Net.MaxOpenRequests = proc * 2
	config.Producer.MaxMessageBytes = int(sarama.MaxRequestSize)
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	config.Metadata.RefreshFrequency = 10 * time.Second
	config.ClientID = "indexer"
	// config.Producer.Compression = sarama.CompressionGZIP
	// config.Producer.Flush.MaxMessages = 10000

	cl, err := sarama.NewClient(brokerList, config)
	if err != nil {
		return nil, err
	}

	// partitionerCreator := func(topic string) sarama.Partitioner {
	// return newLocalAwarePartitioner(cl, topic, hostname)
	// }

	// config.Producer.Partitioner = partitionerCreator
	return cl, nil
}

func startIndexBuilder(proc int, dumperCh chan<- *indexDumperMessage, tickCh <-chan time.Time, wg *sync.WaitGroup) chan<- *indexBuilderMessage {
	wg.Add(1)
	builderCh := make(chan *indexBuilderMessage, proc*2)

	go func() {
		defer wg.Done()
		index := make(reverseIndex)
		offsets := 0
		msgs := 0

		for {
			select {
			case t := <-tickCh:
				// log.Printf("index builder: got new tick for %d, collected %d messages", t.Unix(), msgs)
				dumperCh <- &indexDumperMessage{index, t.UTC(), msgs, offsets}
				index = make(reverseIndex)
				offsets = 0
				msgs = 0

			case msg := <-builderCh:
				if msg == nil {
					log.Printf("exiting index builder")
					t := <-tickCh
					log.Printf("index builder: got new tick for %d, collected %d messages", t.Unix(), msgs)
					dumperCh <- &indexDumperMessage{index, t.UTC(), msgs, offsets}
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
					offsets++
				}

				msgs++
				statIncrementTook(&stat.msgAppendToIdxTook, start)
			}
		}
	}()

	return builderCh
}

func startIndexDumper(producer sarama.SyncProducer, wg *sync.WaitGroup) chan<- *indexDumperMessage {
	ch := make(chan *indexDumperMessage, 300) // 5 min
	wg.Add(1)

	go func() {
		defer wg.Done()
		buf8 := make([]byte, 8)
		builder := flatbuffers.NewBuilder(1024 * 1024)

		for {
			msg := <-ch
			if msg == nil {
				log.Println("exiting index dumper")
				return
			}

			start := time.Now()
			t, index := msg.t, msg.index
			// log.Printf("index dumper got index for %d", t.Unix())

			var tags []string
			for tag := range index {
				tags = append(tags, tag)
			}

			sort.Strings(tags)

			builder.Reset()
			var fbtags []flatbuffers.UOffsetT

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

			encoded := builder.FinishedBytes()
			binary.LittleEndian.PutUint64(buf8, uint64(t.Unix()))

			statIncrementTook(&stat.idxSerializeTook, start)
			statIncrementSize(&stat.idxSendToKafkaSize, len(encoded))

			start = time.Now()
			_, _, err := producer.SendMessage(&sarama.ProducerMessage{
				Topic: indexTopic,
				Key:   sarama.ByteEncoder(buf8),
				Value: sarama.ByteEncoder(encoded),
			})

			statIncrementTook(&stat.idxSendToKafkaTook, start)

			if err != nil {
				log.Printf("failed to store message: %v", err)
			}

			// log.Printf("finished serializing index for %d, %d msgs, %d tags, %d offsets",
			// t.Unix(), msg.msgs, len(tags), msg.offsets)
		}
	}()

	return ch
}

func main() {
	var proc = flag.Int("proc", 16, "max concurency")
	var srv = flag.String("srv", "0.0.0.0:8004", "server socket")
	var netprofile = flag.Bool("netprofile", false, "open socket for remote profiling")
	var brokers = flag.String("brokers", "./brokers", "file containing Kafka brokers to connect to")
	flag.Parse()

	if *netprofile {
		go func() {
			trace.AuthRequest = func(req *http.Request) (any, sensitive bool) {
				return true, true
			}
			addr := "0.0.0.0:6061"
			log.Println("start debug HTTP server", addr)
			log.Println(http.ListenAndServe(addr, nil))
		}()
	}

	statInit()
	hostname, _ := os.Hostname()
	log.Println("GOMAXPROCS", runtime.GOMAXPROCS(0))
	log.Println("hostname", hostname)

	brokerContent, err := ioutil.ReadFile(*brokers)
	if err != nil {
		log.Fatalf("failed to read file '%s': %v", brokers, err)
	}

	brokerList := strings.Split(string(brokerContent), "\n")
	log.Printf("kafka brokers: %s", strings.Join(brokerList, ", "))

	kafkaClient, err := newKafkaClient(*proc, brokerList, hostname)
	if err != nil {
		log.Fatalf("failed to connect to kafka: %v", err)
	}

	defer kafkaClient.Close()
	dataProducer, _ := sarama.NewSyncProducerFromClient(kafkaClient)
	defer dataProducer.Close()
	indexProducer, _ := sarama.NewSyncProducerFromClient(kafkaClient)
	defer indexProducer.Close()

	wg := &sync.WaitGroup{}
	tickCh := time.Tick(time.Second)
	indexDumperCh := startIndexDumper(indexProducer, wg)
	indexBuilderCh := startIndexBuilder(*proc, indexDumperCh, tickCh, wg)

	log.Println("listen to", *srv)
	conn, err := net.Listen("tcp", *srv)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer(grpc.MaxConcurrentStreams(uint32(*proc * 2)))
	pb.RegisterIndexerServer(grpcServer, &indexerServer{dataProducer, indexBuilderCh})

	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-c
		log.Println("stopping gRPC")
		grpcServer.Stop()
	}()

	grpcServer.Serve(conn)
	time.Sleep(100 * time.Millisecond) // let gRPC's goroutines to complete
	close(indexBuilderCh)

	log.Println("waiting completion of goroutines")
	wg.Wait()
	log.Println("bye")
}

func encodePartitionAndOffset(partition int32, offset int64) int64 {
	return int64((int64(partition&0x000000FF) << 56) | (offset & 0x00FFFFFFFFFFFFFFF))
}
