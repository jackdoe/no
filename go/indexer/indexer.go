package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"net"
	"os"
	"os/signal"
	"runtime"
	"sort"
	"sync"
	"syscall"
	"time"

	msgpb "github.com/jackdoe/no/go/msgpb"
)

import (
	"net/http"
	_ "net/http/pprof"
)

const maxTagLength = 40
const maxTagsInMessage = 256
const indexingWindow = 5
const anyTag = "ANY"

const indexedTagSize = 8

var rootDir string

type msg struct {
	buf []byte
	err chan error
}

type job struct {
	partition int
	totalMsg  int
	index     map[string]*deque
	dir       *dir
}

type epoch int64
type tick struct {
	base epoch
	send epoch
	ch   chan<- job
}

func resetMessage(msg *msgpb.Message) {
	msg.Frames = msg.Frames[:0]
	if msg.Header != nil {
		msg.Header.Tags = msg.Header.Tags[:0]
	}
}

func main() {
	var udpsrv = flag.String("udp", "0.0.0.0:8003", "UDP socket")
	var httpsrv = flag.String("http", "0.0.0.0:8004", "HTTP socket")
	var root = flag.String("root", ".", "root directory")
	var proc = flag.Int("proc", 2, "number of workers")
	var netprofile = flag.Bool("netprofile", false, "open socket for remote profiling")
	flag.Parse()

	if *netprofile {
		go func() {
			addr := "0.0.0.0:6061"
			log.Println("start debug HTTP server", addr)
			log.Println(http.ListenAndServe(addr, nil))
		}()
	}

	gomaxprocs := *proc * 3
	runtime.GOMAXPROCS(gomaxprocs)
	log.Println("set GOMAXPROCS to", gomaxprocs)

	rootDir = *root
	os.MkdirAll(rootDir, 0775)

	wg := &sync.WaitGroup{}
	quit := make(chan struct{}, 1)

	startChan := make([]chan epoch, 2, 2)
	startChan[0], startChan[1] = make(chan epoch), make(chan epoch)
	msgchs, ticks := startWorkers(*proc, startChan, quit, wg)

	go func(t <-chan time.Time) {
		// synchronize workers
		for _ = range ticks {
			<-startChan[0]
		}

		base := epoch(time.Now().UTC().Unix())
		send, _ := calcIndexingWindow(base)
		for _ = range ticks {
			startChan[1] <- send
		}

		for ; ; send = base {
			base, _ = calcIndexingWindow(epoch((<-t).UTC().Unix()))

			var jbs []<-chan job
			for _, tk := range ticks {
				ch := make(chan job, 1)
				tk <- tick{base, send, ch}
				jbs = append(jbs, ch)
			}

			go indexWorker(*proc, send, jbs, wg)
		}
	}(time.Tick(time.Second))

	if udpsrv != nil {
		startUDPServer(*udpsrv, *proc, msgchs, quit, wg)
	}

	if httpsrv != nil {
		startHTTPServer(*httpsrv, msgchs, quit, wg)
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
	<-c

	log.Println("exit, waiting for workers to complete")
	close(quit)
	wg.Wait()
	log.Println("exiting")
}

func startUDPServer(hostport string, proc int, msgchs []chan<- msg, quit <-chan struct{}, wg *sync.WaitGroup) {
	log.Println("starting UDP server", hostport)

	addr, err := net.ResolveUDPAddr("udp4", hostport)
	if err != nil {
		log.Fatal("failed to resolve addr: ", err)
	}

	conn, err := net.ListenUDP("udp4", addr)
	if err != nil {
		log.Fatal("failed to lister UDP socket: ", err)
	}

	for i := 0; i < proc; i++ {
		go func(id int) {
			log.Printf("starting UDP server %d", id)

			wg.Add(1)
			defer wg.Done()

			r := rand.New(rand.NewSource(time.Now().UnixNano()))
			buf := make([]byte, 65536, 65536+16) // + 16, in case of worker will need to encode object
			done := make(chan error, 1)          // make writes to channel non-blocking
			msg := msg{buf, done}
			l := len(msgchs)

			timeout := time.Duration(50 * time.Millisecond)
			conn.SetReadDeadline(time.Now().Add(timeout))

			for op := int64(0); ; op++ {
				if length, err := conn.Read(buf); err != nil {
					if nerr, ok := err.(net.Error); ok && nerr.Timeout() == false {
						log.Printf("UDP server [%d]: read error '%s'", id, err.Error())
					} else {
						conn.SetReadDeadline(time.Now().Add(timeout))
					}
				} else {
					msg.buf = buf[:length]
					msgchs[r.Intn(l)] <- msg
					if err = <-done; err != nil {
						log.Printf("UDP server [%d]: failed to process message '%s'", id, err.Error())
					}
				}

				if op&0x7 == 0x7 {
					select {
					default:
					case <-quit:
						log.Printf("UDP server [%d]: quit", id)
						return
					}
				}
			}
		}(i)
	}
}

func startHTTPServer(hostport string, msgchs []chan<- msg, quit <-chan struct{}, wg *sync.WaitGroup) {
	var bufPool sync.Pool
	var msgPool sync.Pool
	msgPool.New = func() interface{} {
		return &msg{nil, make(chan error, 1)}
	}

	var rndPool sync.Pool
	rndPool.New = func() interface{} {
		return rand.New(rand.NewSource(time.Now().UnixNano()))
	}

	handler := func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()

		cl := int(r.ContentLength)
		if cl <= 0 {
			http.Error(w, http.StatusText(http.StatusLengthRequired), http.StatusLengthRequired)
			return
		}

		buf, ok := bufPool.Get().([]byte)
		if ok && cap(buf) < cl+16 { // + 16, in case of worker will need to encode object
			bufPool.Put(buf)
			buf = make([]byte, cl, cl+16)
		} else if ok {
			buf = buf[:cl]
		} else {
			buf = make([]byte, cl, cl+16)
		}

		defer bufPool.Put(buf)

		if _, err := io.ReadFull(r.Body, buf); err != nil {
			log.Println("HTTP server: failed to read body", err)
			http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
			return
		}

		msg := msgPool.Get().(*msg)
		defer msgPool.Put(msg)
		msg.buf = buf

		rnd := rndPool.Get().(*rand.Rand)
		defer rndPool.Put(rnd)

		msgchs[rnd.Intn(len(msgchs))] <- *msg
		if err := <-msg.err; err != nil {
			log.Println("HTTP server: failed to process message", err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
	}

	addr, err := net.ResolveTCPAddr("tcp4", hostport)
	if err != nil {
		log.Fatal("failed to resolve addr: ", err)
	}

	conn, err := net.ListenTCP("tcp4", addr)
	if err != nil {
		log.Fatal("failed to lister UDP socket: ", err)
	}

	http.HandleFunc("/", handler)
	srv := &http.Server{Addr: hostport}
	go func() {
		log.Println("starting HTTP server", hostport)

		wg.Add(1)
		defer wg.Done()

		srv.Serve(newStoppableListener(conn, quit))
	}()
}

func startWorkers(proc int, start []chan epoch, quit <-chan struct{}, wg *sync.WaitGroup) (msgchs []chan<- msg, ticks []chan<- tick) {
	worker := func(partition int, ch <-chan msg, tick <-chan tick) {
		log.Printf("start worker %d", partition)

		wg.Add(1)
		defer wg.Done()

		var err error
		omsg := msgpb.Message{}

		jobs := make([]*job, indexingWindow)
		getJob := func(e epoch) (j *job, bucket int) {
			bucket = int(e) % len(jobs)
			if jobs[bucket] == nil {
				jobs[bucket] = &job{
					partition: partition,
					index:     make(map[string]*deque),
					dir:       newEpochDir(rootDir, int64(e), partition),
				}
			}
			return jobs[bucket], bucket
		}

		start[0] <- 0
		min := <-start[1]
		max := min + indexingWindow

	mainLoop:
		for {
			select {
			case t := <-tick:
				log.Printf("worker [%d]: got new base %d, will send %d", partition, t.base, t.send)

				if t.send != min {
					log.Printf("worker [%d]: send epoch %d doesn't match min epoch %d, this results in data loss!!!", partition, t.send, min)
					t.ch <- job{partition: partition}
					jobs[int(t.send)%len(jobs)] = nil
					break
				}

				j, bucket := getJob(t.send)
				jobs[bucket] = nil
				t.ch <- *j
				close(t.ch)

				select {
				case <-quit:
					log.Printf("worker [%d]: should quit", partition)

					// if t.base > max {
					log.Printf("worker [%d]: quiting", partition)
					return
					// }

					// log.Printf("worker [%d]: wait %d seconds and quit", partition, max-t.base)
					// min = t.base

				default:
					min, max = t.base, t.base+indexingWindow
				}

			case msg := <-ch:
				resetMessage(&omsg)
				needEncode := false

				if err = omsg.Unmarshal(msg.buf); err != nil {
					msg.err <- err
					continue mainLoop
				}

				header := omsg.Header
				if header.Time == 0 {
					needEncode = true
					header.Time = time.Now().UTC().Unix()
				}

				htime := epoch(header.Time)
				if htime < min || htime > max {
					msg.err <- fmt.Errorf("Message time is outside acceptable window time=%d min=%d max=%d", htime, min, max)
					continue mainLoop
				}

				tags := omsg.Header.Tags
				if len(tags) > maxTagsInMessage {
					msg.err <- fmt.Errorf("Too many tags in message %d", len(tags))
					continue mainLoop
				}

				for _, tag := range tags {
					if len(tag) > maxTagLength {
						msg.err <- fmt.Errorf("Too long tag name %d", len(tag))
						continue mainLoop
					}
				}

				j, _ := getJob(htime)

				var f *dfile
				if f, err = j.dir.getDataFile(); err != nil {
					msg.err <- err
					continue mainLoop
				}

				if needEncode {
					msg.buf = msg.buf[:cap(msg.buf)]
					if length, err := omsg.MarshalTo(msg.buf); err != nil {
						msg.err <- err
						continue mainLoop
					} else {
						msg.buf = msg.buf[:length]
					}
				}

				var offset int64
				if offset, err = f.writeBuffer(msg.buf); err != nil {
					msg.err <- err
					continue mainLoop
				}

				index := j.index
				tags = append(tags, anyTag)
				for _, tag := range tags {
					deque, ok := index[string(tag)]
					if !ok {
						deque = newDeque(partition)
						index[string(tag)] = deque
					}

					deque.Append(uint32(offset))
				}

				j.totalMsg++
				msg.err <- nil
			}
		}
	}

	for i := 0; i < proc; i++ {
		t := make(chan tick, 1)
		ticks = append(ticks, t)

		m := make(chan msg, 1)
		msgchs = append(msgchs, m)
		go worker(i, m, t)
	}

	return msgchs, ticks
}

func indexWorker(proc int, e epoch, jobs []<-chan job, wg *sync.WaitGroup) {
	wg.Add(1)
	defer wg.Done()

	log.Printf("new index worker for epoch %d", e)

	totalMsg := 0
	jbs := make([]job, proc, proc)

	for i := range jobs {
		jbs[i] = <-jobs[i]
		totalMsg += jbs[i].totalMsg
		log.Printf("index worker [%d]: got job %d", e, i)
	}

	log.Printf("index worker [%d]: got %d messages", e, totalMsg)
	if totalMsg > 0 {
		tagStringsSize := 0
		var tagsArray []string
		tags := make(map[string]*deque)

		for _, j := range jbs {
			for tag, td := range j.index {
				if deque, ok := tags[tag]; !ok {
					tags[tag] = td
					tagStringsSize += len(tag) + 1 // + 1 for size byte
					tagsArray = append(tagsArray, tag)
				} else {
					deque.AppendAll(td)
				}
			}
		}

		sort.Strings(tagsArray)

		buf4 := make([]byte, 4)
		idxwr, err := jbs[0].dir.getIndexBufferedFile()
		if err != nil {
			log.Printf("index worker [%d]: failed to get index file %s", e, err.Error())
			return
		}

		defer idxwr.Flush()

		/*			 header				    tagsLength	 array of indexedTags */
		tagOffset := len(indexFileHeader) + uint32size + (len(tagsArray) * indexedTagSize)
		dequeOffset := tagOffset + tagStringsSize

		if _, err := idxwr.Write(putUint32(buf4, len(tagsArray))); err != nil {
			log.Printf("index worker [%d]: write IO failed: %s", e, err.Error())
			return
		}

		for _, t := range tagsArray {
			if _, err := idxwr.Write(putUint32(buf4, tagOffset)); err != nil {
				log.Printf("index worker [%d]: write IO failed: %s", e, err.Error())
				return
			}

			if _, err := idxwr.Write(putUint32(buf4, dequeOffset)); err != nil {
				log.Printf("index worker [%d]: write IO failed: %s", e, err.Error())
				return
			}

			d := tags[t]

			/*		       size          partition + its size		      offsets */
			dequeOffset += uint32size + (2 * d.partitions * uint32size) + (d.size * uint32size)
			tagOffset += len(t) + 1 // + 1 for size
		}

		for _, t := range tagsArray {
			if err := idxwr.WriteByte(byte(len(t))); err != nil {
				log.Printf("index worker [%d]: write IO failed: %s", e, err.Error())
				return
			}

			if _, err := idxwr.WriteString(t); err != nil {
				log.Printf("index worker [%d]: write IO failed: %s", e, err.Error())
				return
			}
		}

		for _, t := range tagsArray {
			d := tags[t]
			if _, err := idxwr.Write(putUint32(buf4, d.size+2*d.partitions)); err != nil {
				log.Printf("index worker [%d]: write IO failed: %s", e, err.Error())
				return
			}

			partition := -1
			for di := d.head; di != nil; di = di.next {
				if di.partition != partition {
					partition = di.partition
					if _, err := idxwr.Write(putUint32(buf4, partition)); err != nil {
						log.Printf("index worker [%d]: write IO failed: %s", e, err.Error())
						return
					}

					if _, err := idxwr.Write(putUint32(buf4, di.partitionSize())); err != nil {
						log.Printf("index worker [%d]: write IO failed: %s", e, err.Error())
						return
					}
				}

				if _, err := idxwr.Write(di.valsAsByteSlice()); err != nil {
					log.Printf("index worker [%d]: write IO failed: %s", e, err.Error())
					return
				}
			}
		}
	}

	log.Printf("index worker [%d]: finished processing", e)
	if err := jbs[0].dir.rename(); err != nil {
		log.Printf("index worker [%d]: failed to rename", e, err.Error())
	}
}

func calcIndexingWindow(e epoch) (min, max epoch) {
	min = e - epoch(math.Ceil(float64(indexingWindow/2)))
	max = min + indexingWindow
	return min, max
}
