package main

import (
	"sync"

	"github.com/Shopify/sarama"
)

type Partition struct {
	l     sync.Mutex
	cache []*sarama.Message
}

func NewPartition() *Partition {
	return &Partition{}
}

func (p *Partition) Produce(set *sarama.MessageSet) int64 {
	p.l.Lock()
	defer p.l.Unlock()
	nextOffset := int64(len(p.cache))
	for _, msg := range set.Messages {
		if msg.Msg.Set != nil {
			p.Produce(msg.Msg.Set)
		} else {
			p.cache = append(p.cache, msg.Msg)
		}
	}
	return nextOffset
}

func (p *Partition) Offsets() (oldest, newest int64) {
	p.l.Lock()
	defer p.l.Unlock()
	return 0, int64(len(p.cache))
}

func (p *Partition) Fetch(req *sarama.FetchRequestBlock) *sarama.FetchResponseBlock {
	p.l.Lock()
	defer p.l.Unlock()

	response := &sarama.FetchResponseBlock{
		Err:                 sarama.ErrNoError,
		HighWaterMarkOffset: int64(len(p.cache)),
	}

	if req.FetchOffset > int64(len(p.cache)) {
		response.Err = sarama.ErrOffsetOutOfRange
	} else if req.FetchOffset == int64(len(p.cache)) {
	} else {
		for i, msg := range p.cache[req.FetchOffset:] {
			response.MsgSet.Messages = append(response.MsgSet.Messages, &sarama.MessageBlock{
				Offset: req.FetchOffset + int64(i),
				Msg:    msg,
			})
		}
	}

	return response
}
