package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"reflect"
	"runtime"
	"sort"
	"syscall"
	"unsafe"

	pb "github.com/jackdoe/no/go/datapb"
)

const filePath = "/Users/ikruglov/tmp/indexer/"
const hostPort = "127.0.0.1:8005"
const indexHeader = "=idxi"
const indexHeaderSize = len(indexHeader)
const uint32Size = 4

var maxAllowedMappedSize = int64(4 * 1024 * 1024 * 1024) // 4GB

type indexedTag struct {
	tag     uint32
	offsets uint32
}

type index struct {
	tags    []indexedTag
	data    []byte
	file    *os.File
	version byte
}

type indexPartitions map[uint32][]uint32

func (ip *indexPartitions) getSize() (res int) {
	for _, p := range *ip {
		res += len(p)
	}
	return res
}

func (idx *index) getTag(i int) []byte {
	off := idx.tags[i].tag
	strlen := uint32(idx.data[off])
	return idx.data[off+1 : off+1+strlen]
}

func (idx *index) findTag(tag string) (offsets []byte, ok bool) {
	t := []byte(tag)
	comparator := func(i int) bool { return bytes.Compare(idx.getTag(i), t) >= 0 }
	pos := sort.Search(len(idx.tags), comparator)
	if pos < len(idx.tags) && bytes.Equal(idx.getTag(pos), t) {
		offsets := idx.tags[pos].offsets
		return idx.data[offsets:], true
	}

	return nil, false
}

func (idx *index) getTagPartitions(tag string) (indexPartitions, bool) {
	data, ok := idx.findTag(tag)
	if !ok || len(data) < 4 {
		return nil, false
	}

	count := byteArrayToInt(data[:4])
	if count <= 0 || count*uint32Size >= len(data) { //count can't be 0
		fmt.Println("count <= 0 || count*uint32Size >= len(data)", count)
		return nil, false
	}

	sliceHeader := reflect.SliceHeader{
		Data: uintptr(unsafe.Pointer(&data[4])),
		Len:  count,
		Cap:  count,
	}

	pdata := *(*[]uint32)(unsafe.Pointer(&sliceHeader))
	result := make(indexPartitions)
	pos := 0

	for pos+2 < len(pdata) {
		partition := pdata[pos]
		size := int(pdata[pos+1])
		if pos+size+2 > len(pdata) {
			fmt.Println("pos+size+2 > len(pdata)", pos, size, len(pdata), partition)
			return nil, false
		}
		result[partition] = pdata[pos+2 : pos+2+size]
		pos += 2 + size
	}

	return result, true
}

func getIndex(fileName string) *index {
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatal("failed to open file "+fileName, err)
	}

	defer file.Close()
	fd := int(file.Fd())
	stat, err := file.Stat()
	if err != nil {
		log.Fatal(err)
	}

	pageAlignedSize := (stat.Size() + 4095) &^ 4095
	if maxAllowedMappedSize-pageAlignedSize < 0 {
		log.Fatal("mmaped to much")
	}

	data, err := syscall.Mmap(fd, 0, int(pageAlignedSize), syscall.PROT_READ, syscall.MAP_PRIVATE)
	if err != nil {
		log.Fatal("Failed to mmap file", err)
	}

	defer func() { syscall.Munmap(data) }()

	err = syscall.Mlock(data)
	if err != nil {
		log.Fatal("Failed to mlock file", err)
	}

	defer func() { syscall.Munlock(data) }()

	pos := 0
	if bytes.Compare([]byte(indexHeader), data[:indexHeaderSize]) != 0 {
		log.Fatal("index file has incorrect format")
	}

	pos += indexHeaderSize
	version := data[pos]
	if version != 1 {
		log.Fatal("unsupported version ", version)
	}

	pos++

	var itag indexedTag
	tagsCount := byteArrayToInt(data[pos : pos+4])
	tagsArraySize := tagsCount * int(unsafe.Sizeof(itag))
	if int64(tagsArraySize) > stat.Size() {
		log.Fatal("tagsCount has invalid value")
	}

	pos += 4
	tagsArray := data[pos : pos+tagsArraySize]
	sliceHeader := reflect.SliceHeader{
		Data: uintptr(unsafe.Pointer(&tagsArray[0])),
		Len:  tagsCount,
		Cap:  tagsCount,
	}

	idx := &index{
		tags:    *(*[]indexedTag)(unsafe.Pointer(&sliceHeader)),
		data:    data,
		file:    file,
		version: version,
	}

	runtime.SetFinalizer(idx, func(idx *index) {
		maxAllowedMappedSize += pageAlignedSize
		syscall.Munlock(idx.data)
		syscall.Munmap(idx.data)
		idx.file.Close()
	})

	file = nil
	data = nil
	maxAllowedMappedSize -= pageAlignedSize
	return idx
}

func getIndexForEpoch(epoch int) *index {
	return getIndex(fmt.Sprintf("%s%d-index", filePath, epoch))
}

func mergeAnd(a, b indexPartitions) indexPartitions {
	result := make(indexPartitions)
	if len(a) == 0 || len(b) == 0 {
		return result
	}

	var ok bool
	var p uint32
	var valsA, valsB []uint32
	for p, valsA = range a {
		if valsB, ok = b[p]; !ok {
			continue
		}

		var r []uint32
		posA, posB := 0, 0
		for posA < len(valsA) && posB < len(valsB) {
			valA, valB := valsA[posA], valsB[posB]
			if valA < valB {
				posA++
			} else if valA > valB {
				posB++
			} else {
				r = append(r, valA)
				posA++
				posB++
			}
		}

		if len(r) > 0 {
			result[p] = r
		}
	}

	return result
}

func mergeOr(a, b indexPartitions) indexPartitions {
	if len(a) == 0 {
		return b
	} else if len(b) == 0 {
		return a
	}

	var ok bool
	var p uint32
	var valsA, valsB []uint32
	result := make(indexPartitions)

	for p, valsA = range a {
		if valsB, ok = b[p]; !ok {
			result[p] = valsA
			continue
		}

		var r []uint32
		posA, posB := 0, 0
		for posA < len(valsA) && posB < len(valsB) {
			valA, valB := valsA[posA], valsB[posB]
			if valA < valB {
				r = append(r, valA)
				posA++
			} else if valA > valB {
				r = append(r, valB)
				posB++
			} else {
				r = append(r, valA)
				posA++
				posB++
			}
		}

		if posA < len(valsA) {
			r = append(r, valsA[posA:]...)
		} else if posB < len(valsB) {
			r = append(r, valsB[posB:]...)
		}

		if len(r) > 0 {
			result[p] = r
		}
	}

	for p, valsB = range b {
		if _, ok = a[p]; !ok {
			result[p] = valsB
		}
	}

	return result
}

type query struct {
	From, To int
	And, Or  []interface{}
}

type mergeFunc func(a, b indexPartitions) indexPartitions

func parseQuery(idx *index, f mergeFunc, inf interface{}) indexPartitions {
	switch value := inf.(type) {
	case map[string]interface{}:
		if len(value) != 1 {
			return nil
		}

		for k, v := range value {
			if k == "and" {
				return parseQuery(idx, mergeAnd, v)
			}
			return parseQuery(idx, mergeOr, v)
		}

	case []interface{}:
		if len(value) < 2 {
			return nil
		}

		r := f(parseQuery(idx, f, value[0]), parseQuery(idx, f, value[1]))
		for i := 2; i < len(value); i++ {
			r = f(r, parseQuery(idx, f, value[i]))
		}

		return r

	case string:
		p, _ := idx.getTagPartitions(value)
		return p
	}

	return nil
}

func handler(w http.ResponseWriter, r *http.Request) {
	fmt.Println("new request from", r.RemoteAddr)
	defer r.Body.Close()

	var q query
	dec := json.NewDecoder(r.Body)
	if err := dec.Decode(&q); err != nil {
		fmt.Println(err)
		http.Error(w, http.StatusText(400), 400)
		return
	}

	idx := getIndexForEpoch(q.From)

	var v indexPartitions
	if len(q.And) > 0 {
		v = parseQuery(idx, mergeAnd, q.And)
	} else {
		v = parseQuery(idx, mergeOr, q.Or)
	}

	var substreams []string
	for k, v := range r.URL.Query() {
		if k == "sub" {
			substreams = v
			break
		}
	}

	fmt.Println("going to send N offsets", v.getSize(), substreams)

	h := r.Header
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache")

	buf4 := make([]byte, 4, 4)
	buf64K := make([]byte, 65536, 65536)

	send := func(length int) bool {
		if wlen, err := w.Write(buf4); err != nil || wlen != len(buf4) {
			fmt.Println("Failed to write response", err)
			return false
		}

		for length > 0 {
			if wlen, err := w.Write(buf64K[:length]); err != nil {
				fmt.Println("Failed to write response", err)
				return false
			} else {
				length -= wlen
			}
		}

		return true
	}

	data := pb.Data{}
	for p, offsets := range v {
		dfile, ok := getDatabaseFile(q.From, int(p))
		if !ok {
			continue
		}

		defer dfile.Close()

		for _, offset := range offsets {
			if _, err := dfile.Seek(int64(offset), 0); err != nil {
				fmt.Println("Failed to seek", err)
				break
			}

			if rlen, err := dfile.Read(buf4); err != nil || rlen != len(buf4) {
				fmt.Println("Failed to read length", err)
				break
			}

			length := byteArrayToInt(buf4)
			if length >= len(buf64K) {
				fmt.Println("Too long message")
				break
			}

			if rlen, err := dfile.Read(buf64K[:length]); err != nil || rlen != length {
				fmt.Println("Failed to read body", rlen, length, err)
				break
			}

			if len(substreams) > 0 {
				data.Reset()
				if err := data.Unmarshal(buf64K[:length]); err != nil {
					fmt.Println("Failed to decode", err)
					break
				}

				var payload []*pb.Payload
				for _, sub := range substreams {
					for _, frame := range data.GetFrames() {
						if frame.GetId() == sub {
							payload = append(payload, frame)
						}
					}
				}

				var err error
				data.Frames = payload
				if length, err = data.MarshalTo(buf64K); err != nil {
					fmt.Println("Failed to encode", err)
					break
				}
			}

			if !send(length) {
				return
			}
		}
	}
}

func getDatabaseFile(epoch, partition int) (*os.File, bool) {
	fileName := fmt.Sprintf("%s%d-%d", filePath, epoch, partition)
	if file, err := os.Open(fileName); err != nil {
		fmt.Println("failed to open file "+fileName, err)
		return nil, false
	} else {
		return file, true
	}
}

func main() {
	http.HandleFunc("/", handler)
	log.Fatal(http.ListenAndServe(hostPort, nil))
}

func byteArrayToInt(in []byte) (res int) {
	res |= int(in[3])
	res <<= 8
	res |= int(in[2])
	res <<= 8
	res |= int(in[1])
	res <<= 8
	res |= int(in[0])
	return res
}
