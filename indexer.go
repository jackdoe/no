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

	"github.com/golang/protobuf/proto"
)

const numProccessors = 1
const filePath = "/Users/ikruglov/tmp/indexer/"
const hostPort = "127.0.0.1:8003"
const max_offset = 0x7FFFFFFF
const max_tag_length = 40
const max_tags_in_message = 255
const deque_vals_in_item = 1024
const index_file_header = "=idxi\x01"
const data_file_header = "=idxd\x01"
const indexed_tag_size = 8
const uint32_size = 4

// type indexedTag struct {
// tag     uint32
// offsets uint32
// }

type deque_item struct {
	id   int // hack, remove later
	idx  uint32
	vals [deque_vals_in_item]uint32
	next *deque_item
}

type deque struct {
	head, current *deque_item
	size, id      int
}

func newDeque(id int) *deque {
	head := &deque_item{idx: 0, next: nil}
	return &deque{head, head, 0, id}
}

func (d *deque) Append(v uint32) {
	if d.current.idx < deque_vals_in_item {
		d.current.vals[d.current.idx] = v
		d.current.idx++
	} else {
		next := &deque_item{id: d.id, idx: 1, next: nil}
		next.vals[0] = v
		d.current.next = next
		d.current = next
	}

	d.size++
}

func (d *deque) AppendAll(da *deque) {
	d.current.next = da.head
	d.size += da.size
}

type compaction_job struct {
	id, total int
	index     map[string]*deque
}

type compaction_tick struct {
	t  time.Time
	ch chan compaction_job
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
	ticks := make([]chan compaction_tick, numProccessors)

	for i := 0; i < numProccessors; i++ {
		ticks[i] = make(chan compaction_tick)
		go processor(i, conn, ticks[i], quit, done)
	}

	go func(tick <-chan time.Time) {
		for {
			ctick := &compaction_tick{<-tick, make(chan compaction_job)}
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

func compactor(t time.Time, c chan compaction_job) {
	total := 0
	epoch := t.Unix()
	log.Printf("new compactor for epoch %d", epoch)

	var jobs [numProccessors]compaction_job
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
		for tag, tag_deque := range job.index {
			if deque, ok := tags[tag]; !ok {
				tags[tag] = tag_deque
				tagStringsSize += len(tag) + 1 // + 1 for size byte
			} else {
				deque.AppendAll(tag_deque)
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
	tagOffset := len(index_file_header) + uint32_size + (tagsLength * indexed_tag_size)
	dequeOffset := tagOffset + tagStringsSize

	file.WriteString(index_file_header)
	file.Write(intToByteArray(uint32(tagsLength), buf4))

	for _, t := range tagsArray {
		file.Write(intToByteArray(uint32(tagOffset), buf4))
		file.Write(intToByteArray(uint32(dequeOffset), buf4))
		tagOffset += len(t) + 1 // + 1 for size
		/*			   number of offsets  offsets */
		dequeOffset += uint32_size + (tags[t].size * uint32_size)
	}

	for _, t := range tagsArray {
		buf4[0] = byte(len(t))
		file.Write(buf4[:1])
		file.WriteString(t)
	}

	for _, t := range tagsArray {
		id := -1
		d := tags[t]
		deque_item := d.head
		file.Write(intToByteArray(uint32(d.size), buf4))

		for deque_item != nil {
			if deque_item.id != id {
				id = deque_item.id
				file.Write(intToByteArray(uint32(0x80000000|id), buf4))
			}

			for i := uint32(0); i < deque_item.idx; i++ {
				file.Write(intToByteArray(deque_item.vals[i], buf4)) // TODO use unsafe
			}

			deque_item = deque_item.next
		}
	}
}

func processor(id int, conn *net.UDPConn, tick <-chan compaction_tick, quit, done chan struct{}) {
	buf := make([]byte, 65536+4, 65536+4)
	index := make(map[string]*deque)
	data := &Data{}
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

			if _, err = file.WriteString(data_file_header); err != nil {
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
			ctick.ch <- compaction_job{id, total, index}

			total = 0
			index = make(map[string]*deque)
			if file != nil {
				file.Close()
				file = nil
			}

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
				if err.(net.Error).Timeout() == false {
					log.Println("UDP read error", err)
				}
				continue
			}

			if err := proto.Unmarshal(buf[4:length+4], data); err != nil {
				log.Println("Failed to decode", err)
				continue
			}

			file, offset := getFile(ctick.t)
			if file == nil {
				log.Println("Failed to get file")
				continue
			}

			if offset > max_offset {
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
			if len(tags) > max_tags_in_message {
				log.Println("Too many tags in message")
				continue
			}

			for _, tag := range tags {
				if len(tag) > max_tag_length {
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
