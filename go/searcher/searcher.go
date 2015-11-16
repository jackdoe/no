package main

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"os/signal"
	"reflect"
	"sort"
	"syscall"
	"unsafe"
)

const filePath = "/Users/ikruglov/tmp/indexer/"
const hostPort = "127.0.0.1:8005"
const indexHeader = "=idxi"
const indexHeaderSize = len(indexHeader)
const uint32_size = 4

var max_allowed_mapped_size = int64(4 * 1024 * 1024 * 1024) // 4GB

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

type database struct {
}

func (idx *index) getTag(i int) []byte {
	t_off := idx.tags[i].tag
	strlen := uint32(idx.data[t_off])
	return idx.data[t_off+1 : t_off+1+strlen]
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

func (idx *index) getOffsetsForTag(tag string) func() (id int, offset uint32, ok bool) {
	data, ok := idx.findTag(tag)
	if !ok || len(data) == 0 {
		return nil
	}

	dataLen := len(data) / uint32_size
	if dataLen < 1 { // count
		fmt.Println("too short dataLen")
		return nil
	}

	sliceHeader := reflect.SliceHeader{
		Data: uintptr(unsafe.Pointer(&data[0])),
		Len:  dataLen,
		Cap:  dataLen,
	}

	offsets := *(*[]uint32)(unsafe.Pointer(&sliceHeader))
	count := int(offsets[0])
	if count <= 0 || count >= len(offsets) { // count can't be 0
		fmt.Println("count <= 0 || count >= len(offsets)")
		return nil
	}

	id := uint32(offsets[1])
	if id&0x80000000 != 0x80000000 {
		fmt.Println("no ID")
		return nil
	}

	pos := 0
	return func() (int, uint32, bool) {
		for {
			if pos >= count || pos >= len(offsets) {
				return 0, 0, false
			}

			pos++
			offset := offsets[pos]
			if offset&0x80000000 == 0x80000000 {
				id = offset & 0x7FFFFFFF
				count++
			} else {
				return int(id), offset, true
			}
		}
	}
}

func getIndex(fileName string) (*index, func()) {
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
	if max_allowed_mapped_size-pageAlignedSize < 0 {
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

	pos += 1

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

	index := index{
		tags:    *(*[]indexedTag)(unsafe.Pointer(&sliceHeader)),
		data:    data,
		file:    file,
		version: version,
	}

	f := func() {
		max_allowed_mapped_size += pageAlignedSize
		syscall.Munlock(data)
		syscall.Munmap(data)
		file.Close()
	}

	file = nil
	data = nil
	max_allowed_mapped_size -= pageAlignedSize

	return &index, f
}

func main() {
	idx, f := getIndex("/Users/ikruglov/tmp/indexer/1447617150-index")
	defer f()

	nextOffset := idx.getOffsetsForTag("WEB")
	for {
		id, offset, ok := nextOffset()
		if !ok {
			fmt.Println("STOP")
			break
		}

		fmt.Println(id, offset)
	}

	// offsets, ok := idx.findTag("WEB")
	// fmt.Println(ok)
	// fmt.Println(byteArrayToInt(offsets[:4]))

	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
	// <-c
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
