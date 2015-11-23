package main

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

type mCache struct {
	ring, leak []*index
	soft, hard time.Duration
	mutex      sync.Mutex
	root       string
	// maxMapped  int64 // TODO
}

func newmCache(root string, soft, hard time.Duration) *mCache {
	ringSize := int(hard.Seconds())
	log.Println("ringSize", ringSize)

	mc := &mCache{
		ring: make([]*index, ringSize, ringSize),
		leak: make([]*index, 0),
		root: root,
		soft: soft,
		hard: hard,
	}

	go func(tick <-chan time.Time) {
		for {
			waterMark := (<-tick).UTC()
			waterMark = waterMark.Add(-mc.hard)

			epoch := int(waterMark.Unix())
			for bucket := epoch % len(mc.ring); bucket > 0; bucket-- {
				idx := mc.fetchIndex(bucket)
				if idx == nil || idx.time.After(waterMark) {
					break
				}

				log.Printf("evict %d (%s) next round of mCache eviction, water mark %d (%s)",
					idx.time.Unix(), idx.time.Format("15:04:05"),
					waterMark.Unix(), waterMark.Format("15:04:05"))

				mc.storeIndex(bucket, nil)
			}
		}
	}(time.Tick(time.Second))

	return mc
}

func (mc *mCache) fetchIndex(bucket int) *index {
	ptr := (*unsafe.Pointer)(unsafe.Pointer(&mc.ring[bucket]))
	return (*index)(atomic.LoadPointer(ptr))
}

func (mc *mCache) storeIndex(bucket int, idx *index) {
	ptr := (*unsafe.Pointer)(unsafe.Pointer(&mc.ring[bucket]))
	if oidx := (*index)(atomic.SwapPointer(ptr, unsafe.Pointer(idx))); oidx != nil {
		log.Printf("evicting idx for %d (bucket %d)", oidx.time.Unix(), bucket)
		if oidx.counter() > 0 {
			log.Printf("LEAKED INDEX STRUCT %d!!!!!!!!", oidx.time.Unix())
			mc.leak = append(mc.leak, oidx)
		}
	}
}

func (mc *mCache) getIndex(t time.Time) (*index, error) {
	waterMark := time.Now().UTC().Add(-mc.soft)
	if t.Before(waterMark) {
		return nil, fmt.Errorf("requested time %d is outside kept duration %s",
			t.Unix(), mc.soft.String())
	}

	bucket := int(t.Unix()) & len(mc.ring)
	idx := mc.fetchIndex(bucket)

	if idx != nil {
		log.Println("use cached index for", t.Unix())
		return idx, nil
	}

	mc.mutex.Lock()
	defer mc.mutex.Unlock()

	var err error
	if idx, err = readIndex(mc.root, t); err != nil {
		return nil, err
	}

	log.Println("cache index for", t.Unix())
	mc.storeIndex(bucket, idx)
	return idx, nil
}

// func getDatabase() // TODO
