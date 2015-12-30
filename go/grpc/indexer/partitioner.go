package main

import (
	"log"
	"math/rand"
	"strings"
	"time"

	"github.com/Shopify/sarama"
)

type localAwarePartitioner struct {
	client    sarama.Client
	partition int32
	topic     string
	hostname  string
	generator *rand.Rand
}

// NewRandomPartitioner returns a Partitioner which chooses a random partition each time.
func newLocalAwarePartitioner(client sarama.Client, topic, hostname string) sarama.Partitioner {
	p := &localAwarePartitioner{
		client:    client,
		partition: -1,
		topic:     topic,
		hostname:  hostname,
		generator: rand.New(rand.NewSource(time.Now().UTC().UnixNano())),
	}

	go func(ch <-chan time.Time) {
		for {
			<-ch
			p.updatePartitions()
		}
	}(time.Tick(100 * time.Millisecond))

	return p
}

func (p *localAwarePartitioner) Partition(msg *sarama.ProducerMessage, numPartitions int32) (int32, error) {
	if p.partition >= 0 {
		return p.partition, nil
	}

	return int32(p.generator.Intn(int(numPartitions))), nil
}

func (p *localAwarePartitioner) RequiresConsistency() bool {
	return false
}

func (p *localAwarePartitioner) updatePartitions() {
	var err error
	if p.client != nil {
		partitions, err := p.client.WritablePartitions(p.topic)
		if err != nil {
			goto handleError
		}

		for _, partition := range partitions {
			br, err := p.client.Leader(p.topic, partition)
			if err != nil {
				goto handleError
			}

			if strings.Contains(br.Addr(), p.hostname) {
				if p.partition != partition {
					log.Printf("detected local partition %d for topic '%s'", partition, p.topic)
					p.partition = partition // it's fine to not use atomic update here
				}
				break
			}
		}

		return
	}

handleError:
	p.partition = -1
	log.Printf("failed to update partition for topic: %v", p.topic, err)
}
