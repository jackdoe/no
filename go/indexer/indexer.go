package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"sort"
	"syscall"
	"time"
	"unsafe"

	pb "github.com/jackdoe/no/go/datapb"
)

const numProccessors = 2
const filePath = "/Users/ikruglov/tmp/indexer/"
const hostPort = "127.0.0.1:8003"
const maxOffset = 0x7FFFFFFF
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
	id, total int
	index     map[string]*deque
}

type compactionTick struct {
	t  time.Time
	ch chan compactionJob
}

func main() {
	addr, err := net.ResolveUDPAddr("udp4", hostPort)
	if err != nil {
		log.Fatal("Failed to resolve addr:", err)
	}

	conn, err := net.ListenUDP("udp4", addr)
	if err != nil {
		log.Fatal("Failed to lister UDP socket:", err)
	}

	quit := make(chan struct{})
	done := make(chan struct{})
	ticks := make([]chan compactionTick, numProccessors)

	for i := 0; i < numProccessors; i++ {
		ticks[i] = make(chan compactionTick)
		go processor(i, conn, ticks[i], quit, done)
	}

	go func(tick <-chan time.Time) {
		for {
			ctick := &compactionTick{<-tick, make(chan compactionJob)}
			go compactor(ctick.t, ctick.ch)
			for _, tick := range ticks {
				tick <- *ctick
			}
		}
	}(time.Tick(1 * time.Second))

	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
	<-c
}

func compactor(t time.Time, c chan compactionJob) {
	total := 0
	epoch := t.Unix()
	log.Printf("new compactor for epoch %d", epoch)

	var jobs [numProccessors]compactionJob
	for _ = range jobs {
		cjob := <-c
		total += cjob.total
		jobs[cjob.id] = cjob
		log.Printf("[%d] got compation job for epoch %d", cjob.id, epoch)
	}

	log.Printf("got %d messages to compact for epoch %d", total, epoch)
	if total == 0 {
		return
	}

	fileName := fmt.Sprintf("%s%d-index", filePath, epoch)
	file, err := os.OpenFile(fileName, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0664)
	if err != nil {
		log.Println("Failed to open file "+fileName, err)
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
}

func processor(id int, conn *net.UDPConn, tick <-chan compactionTick, quit, done chan struct{}) {
	buf := make([]byte, 65536+4, 65536+4)
	index := make(map[string]*deque)
	data := pb.Data{}
	total := 0

	log.Printf("new processor %d", id)

	var file *os.File
	defer file.Close()
	getFile := func(t time.Time) (*os.File, int64) {
		if file == nil {
			epoch := t.Unix()
			fileName := fmt.Sprintf("%s%d-%d", filePath, epoch, id)
			log.Printf("[%d] open new file %s for epoch %d", id, fileName, epoch)

			var err error
			file, err = os.OpenFile(fileName, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0664)
			if err != nil {
				log.Println("Failed to open file "+fileName, err)
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
		case ct := <-tick:
			log.Printf("[%d] send job to compactor\n", id)
			ctick.ch <- compactionJob{id, total, index}

			index = make(map[string]*deque)
			file.Close()
			file = nil
			total = 0

			ctick = ct

			select {
			case <-quit:
				break loop
			default:
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

			if offset > maxOffset {
				// TODO possible open new file
				log.Println("Too big offset")
				continue
			}

			intToByteArray(uint32(length), buf[0:4])
			if _, err := file.Write(buf[:length+4]); err != nil {
				log.Println("Failed to write to file", err)
				continue
			}

			// file.Sync()

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
					deque = newDeque(id)
					index[tag] = deque
				}

				deque.Append(uint32(offset))
			}

			total++
		}
	}

	done <- struct{}{}
}

func intToByteArray(v uint32, out []byte) []byte {
	out[3] = byte(v >> 24)
	out[2] = byte(v >> 16)
	out[1] = byte(v >> 8)
	out[0] = byte(v)
	return out
}
