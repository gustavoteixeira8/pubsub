package pubsub

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
)

type PubSub struct {
	redisClient *redis.Client
	ctx         context.Context
	cancel      context.CancelFunc
}

func (ps *PubSub) Pub(topic string, msg *Message) error {
	bs, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	resp := ps.redisClient.Publish(ps.ctx, topic, bs)

	if resp.Err() != nil {
		return fmt.Errorf("error publishing message in topic %s: %w", topic, resp.Err())
	}

	return nil
}

func (ps *PubSub) Sub(topic string, cb SubCallback) error {

	subscriber := ps.redisClient.Subscribe(ps.ctx, topic)

	go func() {

		for {
			msg, err := subscriber.ReceiveMessage(ps.ctx)
			if err != nil {
				logrus.Warnf("error receiving message from topic %s: %v", msg.Channel, err)
				continue
			}

			psMessage := &Message{}
			err = json.Unmarshal([]byte(msg.Payload), psMessage)
			if err != nil {
				logrus.Warnf("error converting message from topic %s: %v", msg.Channel, err)
				continue
			}

			err = cb(psMessage)
			if err != nil {
				logrus.Warnf("error running callback from topic %s: %v", msg.Channel, err)
				continue
			}
			time.Sleep(time.Second)
		}

	}()

	return nil
}

func (ps *PubSub) Wait() error {
	<-ps.ctx.Done()

	err := ps.redisClient.Conn().Close()
	return err
}

func (ps *PubSub) Close() {
	logrus.Printf("closing pubsub at %s", time.Now().String())
	ps.cancel()
}

func New(config *redis.Options) (*PubSub, error) {
	rdb := redis.NewClient(config)

	ctx := context.Background()

	pingresp := rdb.Ping(ctx)

	if pingresp.Err() != nil {
		return nil, fmt.Errorf("can't connect in redis: %w", pingresp.Err())
	}

	ctx, cancel := context.WithCancel(ctx)

	return &PubSub{redisClient: rdb, ctx: ctx, cancel: cancel}, nil
}
