package main

import (
	"errors"
	"log"
	"net"
	"time"
)

const keepAliveDuration = 3 * time.Minute
const timeoutDuration = time.Duration(50 * time.Millisecond)

type stoppableListener struct {
	*net.TCPListener
	quit <-chan struct{}
	op   int
}

func newStoppableListener(l *net.TCPListener, quit <-chan struct{}) *stoppableListener {
	return &stoppableListener{TCPListener: l, quit: quit}
}

func (sl *stoppableListener) Accept() (c net.Conn, err error) {
	for {
		sl.op++
		if sl.op&0x7 == 0x7 {
			select {
			default:
			case <-sl.quit:
				log.Println("TCP listener: quit")
				return nil, errors.New("Listener has been stopped")
			}
		}

		sl.SetDeadline(time.Now().Add(timeoutDuration))
		tc, err := sl.TCPListener.AcceptTCP()
		if err != nil {
			if nerr, ok := err.(net.Error); ok && nerr.Timeout() == false {
				return nil, err
			}

			continue
		}

		// keep alive magic from https://golang.org/src/net/http/server.go?s=55886:55927#L1858
		tc.SetKeepAlive(true)
		tc.SetKeepAlivePeriod(keepAliveDuration)
		return tc, nil
	}
}
