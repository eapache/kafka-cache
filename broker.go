package main

import (
	"time"

	"github.com/Shopify/sarama"
)

type Broker struct {
	addr       string
	partitions map[string]map[int32]*Partition
}

func NewBroker(addr string) *Broker {
	return &Broker{
		addr:       addr,
		partitions: make(map[string]map[int32]*Partition),
	}
}

func (b *Broker) Produce(req *sarama.ProduceRequest) *sarama.ProduceResponse {
	response := &sarama.ProduceResponse{}
	for topic, partitions := range req.MsgSets {
		if b.partitions[topic] == nil {
			b.partitions[topic] = make(map[int32]*Partition)
			b.partitions[topic][0] = NewPartition()
		}

		for partition, set := range partitions {
			p := b.partitions[topic][partition]
			if p == nil {
				response.AddTopicPartition(topic, partition, sarama.ErrUnknownTopicOrPartition)
			} else {
				offset := p.Produce(set)
				response.AddTopicPartition(topic, partition, sarama.ErrNoError)
				response.Blocks[topic][partition].Offset = offset
			}
		}
	}
	return response
}

func (b *Broker) Fetch(req *sarama.FetchRequest) *sarama.FetchResponse {
	response := &sarama.FetchResponse{Blocks: make(map[string]map[int32]*sarama.FetchResponseBlock)}
	any := false
	for topic, partitions := range req.Blocks {
		if b.partitions[topic] == nil {
			b.partitions[topic] = make(map[int32]*Partition)
			b.partitions[topic][0] = NewPartition()
		}

		for partition, block := range partitions {
			p := b.partitions[topic][partition]
			if p == nil {
				response.AddError(topic, partition, sarama.ErrUnknownTopicOrPartition)
			} else {
				if response.Blocks[topic] == nil {
					response.Blocks[topic] = make(map[int32]*sarama.FetchResponseBlock)
				}
				response.Blocks[topic][partition] = p.Fetch(block)
				if len(response.Blocks[topic][partition].MsgSet.Messages) > 0 {
					any = true
				}
			}
		}
	}
	if !any {
		time.Sleep(time.Duration(req.MaxWaitTime) * time.Millisecond)
	}
	return response
}

func (b *Broker) Metadata(req *sarama.MetadataRequest) *sarama.MetadataResponse {
	response := &sarama.MetadataResponse{}
	response.Brokers = []*sarama.Broker{sarama.NewBroker(b.addr)}
	if len(req.Topics) == 0 {
		for topic := range b.partitions {
			response.Topics = append(response.Topics, b.metadataForTopic(topic))
		}
	} else {
		for _, topic := range req.Topics {
			response.Topics = append(response.Topics, b.metadataForTopic(topic))
		}
	}
	return response
}

func (b *Broker) metadataForTopic(topic string) *sarama.TopicMetadata {
	if b.partitions[topic] == nil {
		b.partitions[topic] = make(map[int32]*Partition)
		b.partitions[topic][0] = NewPartition()
	}
	return &sarama.TopicMetadata{
		Err:  sarama.ErrNoError,
		Name: topic,
		Partitions: []*sarama.PartitionMetadata{
			{
				Err:      sarama.ErrNoError,
				ID:       0,
				Leader:   -1,
				Replicas: []int32{-1},
				Isr:      []int32{-1},
			},
		},
	}
}

func (b *Broker) Offset(req *sarama.OffsetRequest) *sarama.OffsetResponse {
	response := &sarama.OffsetResponse{}
	for topic, partitions := range req.Blocks {
		if b.partitions[topic] == nil {
			b.partitions[topic] = make(map[int32]*Partition)
			b.partitions[topic][0] = NewPartition()
		}

		for partition, block := range partitions {
			p := b.partitions[topic][partition]
			oldest, newest := p.Offsets()
			if block.Time == sarama.OffsetNewest {
				response.AddTopicPartition(topic, partition, newest)
			} else if block.Time == sarama.OffsetOldest {
				response.AddTopicPartition(topic, partition, oldest)
			}
		}
	}
	return response
}
