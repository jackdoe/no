package main

import (
	"log"
	"sync/atomic"
	"time"
)

type statType struct {
	sendMessageTook int64
	sendMessageSize int64
	sendIndexTook   int64
	sendIndexSize   int64
}

var stat statType

func statInit() {
	t := time.Tick(time.Second)
	go func() {
		for {
			<-t
			log.Printf("STAT sendMessageTook: %dms sendMessageSize: %.3fMB\n"+
				"\t\t\t sendIndexTook: %dms sendIndexSize: %.3fMB\n",
				getAndResetInt64(&stat.sendMessageTook)/1000000,
				float64(getAndResetInt64(&stat.sendMessageSize))/1024/1024,
				getAndResetInt64(&stat.sendIndexTook)/1000000,
				float64(getAndResetInt64(&stat.sendIndexSize))/1024/1024,
			)
		}
	}()
}

func statIncrementSize(c *int64, val int) {
	atomic.AddInt64(c, int64(val))
}

func statIncrementTook(c *int64, t time.Time) {
	atomic.AddInt64(c, time.Since(t).Nanoseconds())
}

func getAndResetInt64(c *int64) int64 {
	res := atomic.LoadInt64(c)
	atomic.StoreInt64(c, 0)
	return res
}
