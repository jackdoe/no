package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"os"
	"time"
)

const dirMode = 0755
const fileMode = 0644
const dataFileHeader = "=idxd\x01"
const indexFileHeader = "=idxi\x01"
const bufferSize = 1024 * 1024

type dir struct {
	path, tmp string
	epoch     int64
	partition int
	df        *dfile
}

type dfile struct {
	f      *os.File
	offset int64
	tbuf4  []byte
}

func newEpochDir(root string, epoch int64, partition int) *dir {
	t := time.Unix(epoch, 0)
	path := fmt.Sprintf("%s/%s/%02d/%d", root, t.Format("2006010215"), t.Minute(), t.Unix())
	tmp := path + ".tmp"

	os.MkdirAll(tmp, dirMode)
	return &dir{
		path:      path,
		tmp:       tmp,
		epoch:     epoch,
		partition: partition,
	}
}

func (d *dir) rename() error {
	return os.Rename(d.tmp, d.path)
}

func (d *dir) getDataFile() (df *dfile, err error) {
	if d.df != nil {
		return d.df, nil
	}

	var f *os.File
	fn := fmt.Sprintf("%s/%d-%d.data", d.tmp, d.epoch, d.partition)
	if f, err = os.OpenFile(fn, os.O_APPEND|os.O_WRONLY|os.O_CREATE, fileMode); err != nil {
		return nil, err
	}

	if _, err = f.WriteString(dataFileHeader); err != nil {
		os.Remove(fn)
		return nil, err
	}

	d.df = &dfile{f, int64(len(dataFileHeader)), make([]byte, 4)}
	return d.df, nil
}

func (d *dir) getIndexBufferedFile() (w *bufio.Writer, err error) {
	var f *os.File
	fn := fmt.Sprintf("%s/%d.idx", d.tmp, d.epoch)
	if f, err = os.OpenFile(fn, os.O_APPEND|os.O_WRONLY|os.O_CREATE, fileMode); err != nil {
		return nil, err
	}

	if _, err = f.WriteString(indexFileHeader); err != nil {
		os.Remove(fn)
		return nil, err
	}

	return bufio.NewWriterSize(f, bufferSize), nil
}

func (f *dfile) writeBuffer(buf []byte) (int64, error) {
	offset := f.offset

	wlen, err := f.f.Write(putUint32(f.tbuf4, len(buf)))
	f.offset += int64(wlen)
	if err != nil {
		return 0, err
	}

	wlen, err = f.f.Write(buf)
	f.offset += int64(wlen)
	if err != nil {
		return 0, err
	}

	return offset, nil
}

func putUint32(b []byte, v int) []byte {
	binary.LittleEndian.PutUint32(b, uint32(v))
	return b
}
