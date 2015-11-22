package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"sort"
	"sync"
	"syscall"
	"time"
	"unsafe"

	pb "github.com/jackdoe/no/go/datapb"
)

const numProccessors = 2
const filePath = "/Users/ikruglov/tmp/indexer/"
const hostPort = "127.0.0.1:8003"
const maxTagLength = 40
const maxTagsInMessage = 255
const dequeItemSize = 1024
const dequeItemSizeBytes = dequeItemSize * uint32Size
const indexFileHeader = "=idxi\x01"
const dataFileHeader = "=idxd\x01"
const indexedTagSize = 8
const uint32Size = 4

type dequeItem struct {
	idx, partition int
	vals           [dequeItemSize]uint32
	next           *dequeItem
}

type deque struct {
	head, current    *dequeItem
	size, partitions int
}

func newDeque(partition int) *deque {
	head := newDequeItem(partition)
	return &deque{head, head, 0, 1}
}

func newDequeItem(partition int) *dequeItem {
	return &dequeItem{partition: partition}
}

func (d *deque) Append(v uint32) {
	if d.current.idx >= len(d.current.vals) {
		next := newDequeItem(d.current.partition)
		d.current.next = next
		d.current = next
	}

	d.current.vals[d.current.idx] = v
	d.current.idx++
	d.size++
}

func (d *deque) AppendAll(da *deque) {
	if da.current.partition != d.current.partition {
		d.partitions++
	}

	d.size += da.size
	d.current.next = da.head
	d.current = da.current
}

type compactionJob struct {
	partition int
	totalMsg  int
	index     map[string]*deque
}

type compactionTick struct {
	t  time.Time
	ch chan compactionJob
}

func main() {
	log.Println("start UDP server", hostPort)
	addr, err := net.ResolveUDPAddr("udp4", hostPort)
	if err != nil {
		log.Fatal("Failed to resolve addr:", err)
	}

	conn, err := net.ListenUDP("udp4", addr)
	if err != nil {
		log.Fatal("Failed to lister UDP socket:", err)
	}

	var wg sync.WaitGroup
	quit := make(chan struct{})
	ticks := make([]chan compactionTick, numProccessors)

	for i := range ticks {
		ticks[i] = make(chan compactionTick)
		go processor(i, conn, ticks[i], quit, &wg)
	}

	go func(tick <-chan time.Time) {
		for {
			ctick := compactionTick{(<-tick).UTC(), make(chan compactionJob)}
			go compactor(ctick.t, ctick.ch, &wg)
			for _, tick := range ticks {
				tick <- ctick
			}
		}
	}(time.Tick(time.Second))

	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
	<-c

	for _ = range ticks {
		quit <- struct{}{}
	}

	log.Println("waiting for workers to complete")
	// wg.Wait() TODO
	log.Println("exiting")
}

func compactor(t time.Time, c chan compactionJob, wg *sync.WaitGroup) {
	wg.Add(1)
	defer wg.Done()

	totalMsg := 0
	epoch := t.Unix()
	log.Printf("new compactor for epoch %d", epoch)

	var jobs [numProccessors]compactionJob
	for _ = range jobs {
		cjob := <-c
		totalMsg += cjob.totalMsg
		jobs[cjob.partition] = cjob
		log.Printf("got compation job [%d] for epoch %d", cjob.partition, epoch)
	}

	log.Printf("got %d messages to compact for epoch %d", totalMsg, epoch)
	if totalMsg == 0 {
		return
	}

	dir, dirTmp, err := makePath(t)
	if err != nil {
		log.Println("Failed to create", dirTmp, err)
		return
	}

	fileName := fmt.Sprintf("%s%d.idx", dirTmp, epoch)
	file, err := createFile(fileName)
	if err != nil {
		log.Println("Failed to open file", fileName, err)
		return
	}

	defer file.Close()

	tagStringsSize := 0
	tags := make(map[string]*deque)
	for _, job := range jobs {
		for tag, td := range job.index {
			if deque, ok := tags[tag]; !ok {
				tags[tag] = td
				tagStringsSize += len(tag) + 1 // + 1 for size byte
			} else {
				deque.AppendAll(td)
			}
		}
	}

	log.Printf("will dump %d tags for epoch %d", len(tags), epoch)

	tagsArray := make([]string, 0, len(tags))
	for t := range tags {
		tagsArray = append(tagsArray, t)
	}

	sort.Strings(tagsArray)

	buf4 := make([]byte, 4, 4)
	tagsLength := len(tagsArray)
	/*			 header				      tagsLength	array of indexedTags*/
	tagOffset := len(indexFileHeader) + uint32Size + (tagsLength * indexedTagSize)
	dequeOffset := tagOffset + tagStringsSize

	file.WriteString(indexFileHeader)
	file.Write(intToByteArray(uint32(tagsLength), buf4))

	for _, t := range tagsArray {
		file.Write(intToByteArray(uint32(tagOffset), buf4))
		file.Write(intToByteArray(uint32(dequeOffset), buf4))
		d := tags[t]

		/*		       size          partition + its size		    offsets */
		dequeOffset += uint32Size + (2 * d.partitions * uint32Size) + (d.size * uint32Size)
		tagOffset += len(t) + 1 // + 1 for size
	}

	for _, t := range tagsArray {
		buf4[0] = byte(len(t))
		file.Write(buf4[:1])
		file.WriteString(t)
	}

	partition := -1
	calcPartitionSize := func(di *dequeItem) (res int) {
		for p := di.partition; di != nil && p == di.partition; di, res = di.next, res+di.idx {
		}
		return res
	}

	for _, t := range tagsArray {
		d := tags[t]
		file.Write(intToByteArray(uint32(d.size+2*d.partitions), buf4))
		for di := d.head; di != nil; di = di.next {
			if di.partition != partition {
				partition = di.partition
				file.Write(intToByteArray(uint32(partition), buf4))
				file.Write(intToByteArray(uint32(calcPartitionSize(di)), buf4))
			}

			vals := *(*[dequeItemSizeBytes]byte)(unsafe.Pointer(&di.vals[0]))
			file.Write(vals[:di.idx*uint32Size])
		}
	}

	log.Printf("rename %s to %s", dirTmp, dir)
	if err = os.Rename(dirTmp, dir); err != nil {
		log.Println("Failed to rename", dirTmp, dir, err)
	}
}

func processor(partition int, conn *net.UDPConn, tick <-chan compactionTick, quit chan struct{}, wg *sync.WaitGroup) {
	wg.Add(1)
	defer wg.Done()

	shouldQuit := false
	buf := make([]byte, 65536+4, 65536+4)
	index := make(map[string]*deque)
	data := pb.Data{}
	totalMsg := 0

	log.Printf("new processor %d", partition)

	var file *os.File
	defer file.Close()
	getFile := func(t time.Time) (*os.File, int64) {
		if file == nil {
			_, fileName, err := makePath(t)
			if err != nil {
				log.Println("Failed to create path", fileName, err)
				return nil, 0
			}

			fileName += fmt.Sprintf("%d.%d.data", t.Unix(), partition)
			log.Printf("[%d] open new file %s for epoch %d", partition, fileName, t.Unix())

			if file, err = createFile(fileName); err != nil {
				log.Println("Failed to open file", fileName, err)
				return nil, 0
			}

			if _, err = file.WriteString(dataFileHeader); err != nil {
				log.Println("Failed to write to file", err)
				return nil, 0
			}
		}

		offset, _ := file.Seek(0, 1)
		return file, offset
	}

	ctick := <-tick

loop:
	for {
		select {
		case <-quit:
			shouldQuit = true
			log.Printf("[%d] will quit", partition)

		case ct := <-tick:
			log.Printf("[%d] send job to compactor %d\n", partition, ctick.t.Unix())
			ctick.ch <- compactionJob{partition, totalMsg, index}
			index = make(map[string]*deque)
			file.Close()
			file = nil
			totalMsg = 0
			ctick = ct

			if shouldQuit {
				log.Printf("[%d] quiting", partition)
				break loop
			}

		default:
			conn.SetReadDeadline(time.Now().Add(time.Millisecond))
			length, err := conn.Read(buf[4:])
			if err != nil {
				if nerr, ok := err.(net.Error); ok && nerr.Timeout() == false {
					log.Println("UDP read error", err)
				}
				continue
			}

			data.Reset()
			if err := data.Unmarshal(buf[4 : length+4]); err != nil {
				log.Println("Failed to decode", err)
				continue
			}

			file, offset := getFile(ctick.t)
			if file == nil {
				log.Println("Failed to get file")
				continue
			}

			intToByteArray(uint32(length), buf[0:4])
			if _, err := file.Write(buf[:length+4]); err != nil {
				log.Println("Failed to write to file", err)
				continue
			}

			tags := data.GetHeader().GetTags()
			if len(tags) > maxTagsInMessage {
				log.Println("Too many tags in message ", len(tags))
				continue
			}

			for _, tag := range tags {
				if len(tag) > maxTagLength {
					log.Println("Too long tag")
					continue
				}

				deque, ok := index[tag]
				if !ok {
					deque = newDeque(partition)
					index[tag] = deque
				}

				deque.Append(uint32(offset))
			}

			totalMsg++
		}
	}
}

func createFile(fileName string) (*os.File, error) {
	return os.OpenFile(fileName, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0664)
}

func makePath(t time.Time) (string, string, error) {
	final := fmt.Sprintf("%s/%s/%2d/%d", filePath, t.Format("2006010215"), t.Minute(), t.Unix())
	tmp := final + ".tmp"
	return final + "/", tmp + "/", os.MkdirAll(tmp, 0775)
}

func intToByteArray(v uint32, out []byte) []byte {
	out[3] = byte(v >> 24)
	out[2] = byte(v >> 16)
	out[1] = byte(v >> 8)
	out[0] = byte(v)
	return out
}
