package main

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

const maxPartitions = 64

type mCache struct {
	idxRing    []*index
	dbRings    [maxPartitions][]*database
	idxLeak    []*index
	dbLeak     []*database
	soft, hard time.Duration
	mutex      sync.Mutex
	root       string
	// maxMapped  int64 // TODO
}

func newmCache(root string, soft, hard time.Duration) *mCache {
	idxRingSize := int(hard.Seconds())
	log.Println("idxRingSize", idxRingSize)

	mc := &mCache{
		idxRing: make([]*index, idxRingSize, idxRingSize),
		idxLeak: make([]*index, 0),
		root:    root,
		soft:    soft,
		hard:    hard,
	}

	go func(tick <-chan time.Time) {
		for {
			waterMark := (<-tick).UTC().Add(-mc.hard)
			for bucket := range mc.idxRing {
				idx := mc.fetchIndex(bucket)
				if idx != nil && idx.time.Before(waterMark) {
					log.Printf("evict idx %d %s (bucket %d)",
						idx.time.Unix(), idx.time.Format("15:04:05"), bucket)
					mc.storeIndex(bucket, nil)
				}
			}

			for p, ring := range mc.dbRings {
				for bucket := range ring {
					db := mc.fetchDb(p, bucket)
					if db != nil && db.time.Before(waterMark) {
						log.Printf("evict db %d %s (p%d bucket %d)",
							db.time.Unix(), db.time.Format("15:04:05"), p, bucket)
						mc.storeDb(p, bucket, nil)
					}
				}
			}
		}
	}(time.Tick(time.Second))

	return mc
}

func (mc *mCache) fetchIndex(bucket int) *index {
	ptr := (*unsafe.Pointer)(unsafe.Pointer(&mc.idxRing[bucket]))
	return (*index)(atomic.LoadPointer(ptr))
}

func (mc *mCache) storeIndex(bucket int, idx *index) {
	ptr := (*unsafe.Pointer)(unsafe.Pointer(&mc.idxRing[bucket]))
	if oidx := (*index)(atomic.SwapPointer(ptr, unsafe.Pointer(idx))); oidx != nil {
		log.Printf("forced eviction of idx for %d (bucket %d)", oidx.time.Unix(), bucket)
		if oidx.counter() > 0 {
			log.Printf("LEAKED INDEX STRUCT %d!!!!!!!!", oidx.time.Unix())
			mc.idxLeak = append(mc.idxLeak, oidx)
		}
	}
}

func (mc *mCache) getIndex(t time.Time) (*index, error) {
	waterMark := time.Now().UTC().Add(-mc.soft)
	if t.Before(waterMark) {
		return nil, fmt.Errorf("requested time %d is outside kept duration %s",
			t.Unix(), mc.soft.String())
	}

	bucket := int(t.Unix()) & len(mc.idxRing)
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

func (mc *mCache) fetchDb(p, bucket int) *database {
	ptr := (*unsafe.Pointer)(unsafe.Pointer(&mc.dbRings[p][bucket]))
	return (*database)(atomic.LoadPointer(ptr))
}

func (mc *mCache) storeDb(p, bucket int, db *database) {
	ptr := (*unsafe.Pointer)(unsafe.Pointer(&mc.dbRings[p][bucket]))
	if odb := (*database)(atomic.SwapPointer(ptr, unsafe.Pointer(db))); odb != nil {
		log.Printf("forced eviction of db for %d (p%d bucket %d)", odb.time.Unix(), p, bucket)
		if odb.counter() > 0 {
			log.Printf("LEAKED DATABASE STRUCT %d p%d!!!!!!!!", odb.time.Unix(), p)
			mc.dbLeak = append(mc.dbLeak, odb)
		}
	}
}

func (mc *mCache) getDatabase(partition int, t time.Time) (*database, error) {
	waterMark := time.Now().UTC().Add(-mc.soft)
	if t.Before(waterMark) {
		return nil, fmt.Errorf("requested time %d is outside kept duration %s",
			t.Unix(), mc.soft.String())
	}

	if partition < 0 || partition >= maxPartitions {
		return nil, fmt.Errorf("invalid requested partition")
	}

	bucket := int(t.Unix()) & len(mc.dbRings[partition])
	db := mc.fetchDb(partition, bucket)

	if db != nil {
		log.Println("use cached database for", partition, t.Unix())
		return db, nil
	}

	mc.mutex.Lock()
	defer mc.mutex.Unlock()

	var err error
	if db, err = readDatabase(mc.root, t, partition); err != nil {
		return nil, err
	}

	log.Println("cache index for", t.Unix())
	mc.storeDb(partition, bucket, db)
	return db, nil
}
