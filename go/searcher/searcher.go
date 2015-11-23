package main

import (
	"encoding/json"
	"log"
	"net/http"
	"time"

	pb "github.com/jackdoe/no/go/datapb"
)

const filePath = "/Users/ikruglov/tmp/indexer/"
const hostPort = "127.0.0.1:8005"

var mcache *mCache

type query struct {
	From, To int
	And, Or  []interface{}
}

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
	log.Println("new request from", r.RemoteAddr)
	defer r.Body.Close()

	var q query
	var err error
	dec := json.NewDecoder(r.Body)
	if err = dec.Decode(&q); err != nil {
		log.Println("failed to decode request", err)
		http.Error(w, http.StatusText(400), 400)
		return
	}

	t := time.Unix(int64(q.From), 0).UTC()
	idx, err := mcache.getIndex(t)
	if err != nil {
		log.Println("failed to get index", err)
		http.Error(w, http.StatusText(400), 400)
		return
	}

	idx.borrow()
	defer idx.release()

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

	log.Println("going to send N offsets", v.size(), substreams)

	h := r.Header
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache")

	var buf []byte
	buf4 := make([]byte, 4, 4)
	buf64K := make([]byte, 65536, 65536)

	send := func(b []byte) error {
		if _, err = w.Write(intToByteArray(len(b), buf4)); err != nil {
			return err
		}

		if _, err = w.Write(b); err != nil {
			return err
		}

		return nil
	}

	data := pb.Data{}
	for p, offsets := range v {
		db, err := mcache.getDatabase(int(p), t)
		if err != nil {
			log.Println(err)
			continue
		}

		db.borrow()
		defer db.release()

		for _, off := range offsets {
			offset := int(off)
			if offset+uint32Size > len(db.data) {
				log.Println("invalid offset", offset, len(db.data))
				break
			}

			length := byteArrayToInt(db.data[offset : offset+uint32Size])
			if offset+length+uint32Size > len(db.data) {
				log.Println("invalid length", offset+length+uint32Size, len(db.data))
				break
			}

			buf = db.data[offset+uint32Size : offset+uint32Size+length]

			if len(substreams) > 0 {
				data.Reset()
				if err = data.Unmarshal(buf); err != nil {
					log.Println("Failed to decode", err)
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

				data.Frames = payload
				if length, err = data.MarshalTo(buf64K); err != nil {
					log.Println("Failed to encode", err)
					break
				}

				if err = send(buf64K[:length]); err != nil {
					log.Println(err)
					break
				}
			} else {
				if err = send(buf); err != nil {
					log.Println(err)
					break
				}
			}
		}
	}
}

func main() {
	mcache = newmCache(filePath, time.Hour, 2*time.Hour)
	http.HandleFunc("/", handler)
	log.Println("start HTTP server", hostPort)
	log.Fatal(http.ListenAndServe(hostPort, nil))
}

func intToByteArray(v int, out []byte) []byte {
	out[3] = byte(v >> 24)
	out[2] = byte(v >> 16)
	out[1] = byte(v >> 8)
	out[0] = byte(v)
	return out
}
