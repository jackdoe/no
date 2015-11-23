package main

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"os"
	"reflect"
	"runtime"
	"sort"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"
)

var itag indexedTag

const uint32Size = 4
const indexHeader = "=idxi"
const indexHeaderSize = len(indexHeader)
const databaseHeader = "=idxd"
const databaseHeaderSize = len(databaseHeader)
const indexedTagSize = int(unsafe.Sizeof(itag))

type atomicCounter struct {
	cnt int32
}

func (c *atomicCounter) counter() int32 {
	return atomic.LoadInt32(&c.cnt)
}

func (c *atomicCounter) borrow() int32 {
	return atomic.AddInt32(&c.cnt, 1)
}

func (c *atomicCounter) release() int32 {
	v := atomic.AddInt32(&c.cnt, -1)
	if v < 0 {
		log.Println("WARN: v < 0!!!!")
	}
	return v
}

type indexPartitions map[uint32][]uint32

func (ip *indexPartitions) size() (res int) {
	for _, p := range *ip {
		res += len(p)
	}
	return res
}

type indexedTag struct {
	tag     uint32
	offsets uint32
}

type index struct {
	tags []indexedTag
	data []byte
	time time.Time
	md   *mmapData
	ver  byte
	atomicCounter
}

func readIndex(root string, t time.Time) (*index, error) {
	epoch := t.Unix()
	fileName := buildIndexFile(root, t)
	log.Printf("read index for %d from %s", epoch, fileName)

	md, err := mmapFile(fileName)
	if err != nil {
		return nil, err
	}

	pos := 0
	data := md.d
	if bytes.Compare([]byte(indexHeader), data[:indexHeaderSize]) != 0 {
		return nil, fmt.Errorf("index file has incorrect format")
	}

	pos += indexHeaderSize
	version := data[pos]
	if version != 1 {
		return nil, fmt.Errorf("unsupported version %d", version)
	}

	pos++

	tagsCount := byteArrayToInt(data[pos : pos+4])
	tagsArraySize := tagsCount * indexedTagSize
	if tagsArraySize > len(data) {
		return nil, errors.New("tagsArraySize > len(data)")
	}

	pos += 4
	tagsArray := data[pos : pos+tagsArraySize]
	sliceHeader := reflect.SliceHeader{
		Data: uintptr(unsafe.Pointer(&tagsArray[0])),
		Len:  tagsCount,
		Cap:  tagsCount,
	}

	return &index{
		tags: *(*[]indexedTag)(unsafe.Pointer(&sliceHeader)),
		time: t,
		data: md.d,
		md:   md,
		ver:  version,
	}, nil
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
	if count <= 0 || count*uint32Size > len(data) { //count can't be 0
		log.Println("count <= 0 || count*uint32Size > len(data)", count)
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
			log.Println("pos+size+2 > len(pdata)", pos, size, len(pdata), partition)
			return nil, false
		}
		result[partition] = pdata[pos+2 : pos+2+size]
		pos += 2 + size
	}

	return result, true
}

type mergeFunc func(a, b indexPartitions) indexPartitions

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

type database struct {
	data      []byte
	partition int
	t         time.Time
	md        *mmapData
	ver       byte
	atomicCounter
}

func readDatabase(root string, t time.Time, partition int) (*database, error) {
	epoch := t.Unix()
	fileName := buildDatabaseFile(root, t, partition)
	log.Printf("read database for %d-%d from %s", epoch, partition, fileName)

	md, err := mmapFile(fileName)
	if err != nil {
		return nil, err
	}

	if bytes.Compare([]byte(databaseHeader), md.d[:databaseHeaderSize]) != 0 {
		return nil, fmt.Errorf("database file has incorrect format")
	}

	version := md.d[databaseHeaderSize]
	if version != 1 {
		return nil, fmt.Errorf("unsupported version %d", version)
	}

	return &database{
		data:      md.d,
		partition: partition,
		t:         t,
		md:        md,
		ver:       version,
	}, nil
}

type mmapData struct {
	f *os.File
	d []byte
}

func mmapFile(fileName string) (*mmapData, error) {
	file, err := os.Open(fileName)
	if err != nil {
		return nil, err
	}

	stat, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}

	size := stat.Size()
	pageAlignedSize := (size + 4095) &^ 4095
	data, err := syscall.Mmap(int(file.Fd()), 0, int(pageAlignedSize),
		syscall.PROT_READ, syscall.MAP_PRIVATE)

	if err != nil {
		file.Close()
		return nil, err
	}

	err = syscall.Mlock(data)
	if err != nil {
		syscall.Munmap(data)
		file.Close()
		return nil, err
	}

	md := &mmapData{file, data[:size]}
	runtime.SetFinalizer(md, func(m *mmapData) {
		syscall.Munlock(m.d)
		syscall.Munmap(m.d)
		m.f.Close()
	})

	return md, nil
}

func buildPath(root string, t time.Time) string {
	return fmt.Sprintf("%s/%s/%2d/%d/", root, t.Format("2006010215"), t.Minute(), t.Unix())
}

func buildIndexFile(root string, t time.Time) string {
	return fmt.Sprintf("%s%d.idx", buildPath(root, t), t.Unix())
}

func buildDatabaseFile(root string, t time.Time, partition int) string {
	return fmt.Sprintf("%s%d.%d.data", buildPath(root, t), t.Unix(), partition)
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
