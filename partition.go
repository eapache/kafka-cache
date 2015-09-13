package main

import (
	"sync"

	"github.com/Shopify/sarama"
)

type Partition struct {
	l     sync.Mutex
	base  int64
	cache []*sarama.Message
}

const maxCache = 1024

func NewPartition() *Partition {
	return &Partition{}
}

func (p *Partition) Produce(set *sarama.MessageSet) int64 {
	p.l.Lock()
	defer p.l.Unlock()
	firstOffset := p.nextOffset()
	for _, msg := range set.Messages {
		if msg.Msg.Set != nil {
			p.Produce(msg.Msg.Set)
		} else {
			p.cache = append(p.cache, msg.Msg)
		}
	}
	if len(p.cache) > maxCache {
		trim := len(p.cache) - maxCache
		p.base += int64(trim)
		p.cache = p.cache[trim:]
	}
	return firstOffset
}

func (p *Partition) Offsets() (oldest, newest int64) {
	p.l.Lock()
	defer p.l.Unlock()
	return p.oldestOffset(), p.nextOffset()
}

func (p *Partition) oldestOffset() int64 {
	return p.base
}

func (p *Partition) nextOffset() int64 {
	return p.base + int64(len(p.cache))
}

func (p *Partition) Fetch(req *sarama.FetchRequestBlock) *sarama.FetchResponseBlock {
	p.l.Lock()
	defer p.l.Unlock()

	response := &sarama.FetchResponseBlock{
		Err:                 sarama.ErrNoError,
		HighWaterMarkOffset: p.nextOffset(),
	}

	if req.FetchOffset > p.nextOffset() {
		response.Err = sarama.ErrOffsetOutOfRange
	} else if req.FetchOffset == p.nextOffset() {
	} else {
		for i, msg := range p.cache[req.FetchOffset-p.base:] {
			response.MsgSet.Messages = append(response.MsgSet.Messages, &sarama.MessageBlock{
				Offset: req.FetchOffset + int64(i),
				Msg:    msg,
			})
		}
	}

	return response
}
