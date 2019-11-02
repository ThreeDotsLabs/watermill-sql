package sql

import (
	"context"

	"github.com/ThreeDotsLabs/watermill/message"
)

type BinlogSubscriber struct{}

func NewBinlogSubscriber() (*BinlogSubscriber, error) {
	return &BinlogSubscriber{}, nil
}

func (BinlogSubscriber) Subscribe(ctx context.Context, topic string) (<-chan *message.Message, error) {
	panic("implement me")
}

func (BinlogSubscriber) Close() error {
	panic("implement me")
}
