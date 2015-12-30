package main

import (
	"log"
	"sync/atomic"
	"time"
)

type statType struct {
	msgCnt             int64
	msgSerializeTook   int64
	msgSendToKafkaSize int64
	msgSendToKafkaTook int64
	msgSendToChTook    int64

	msgAppendToIdxTook int64

	idxSerializeTook   int64
	idxSendToKafkaSize int64
	idxSendToKafkaTook int64
}

var stat statType

func statInit() {
	t := time.Tick(time.Second)
	go func() {
		for {
			<-t
			log.Printf(
				// "\nSTAT msg: cnt %d serializeTook %dms sendToKafkaTook %dms sendToKafkaSize %.3fMB sendToChTook %dms\n"+
				// "STAT prc: appendToIdxTook %dms\n"+
				// "STAT idx: serializeTook %dms sendToKafkaTook %dms sendToKafkaSize %.3fMB\n",
				"MSG: cnt %-5d srl %-4dms tokafka %-5dms size %.2fMB toch %-3dms PROC: append %-3dms IDX: srl %-3dms tokafka %-2dms size %.2fMB",

				getAndResetInt64(&stat.msgCnt),
				getAndResetInt64(&stat.msgSerializeTook)/1000000,
				getAndResetInt64(&stat.msgSendToKafkaTook)/1000000,
				float64(getAndResetInt64(&stat.msgSendToKafkaSize))/1024/1024,
				getAndResetInt64(&stat.msgSendToChTook)/1000000,

				getAndResetInt64(&stat.msgAppendToIdxTook)/1000000,

				getAndResetInt64(&stat.idxSerializeTook)/1000000,
				getAndResetInt64(&stat.idxSendToKafkaTook)/1000000,
				float64(getAndResetInt64(&stat.idxSendToKafkaSize))/1024/1024,
			)
		}
	}()
}

func statIncrementCnt(c *int64) {
	atomic.AddInt64(c, 1)
}

func statIncrementSize(c *int64, val int) {
	atomic.AddInt64(c, int64(val))
}

func statIncrementTook(c *int64, t time.Time) {
	atomic.AddInt64(c, time.Since(t).Nanoseconds())
}

func getAndResetInt64(c *int64) int64 {
	return atomic.SwapInt64(c, 0)
}
