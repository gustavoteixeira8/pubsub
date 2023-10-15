package main

import (
	"fmt"
	"time"

	"github.com/gustavoteixeira8/pubsub"
	"github.com/redis/go-redis/v9"
)

func main() {
	ps, err := pubsub.New(&redis.Options{
		Addr:     "localhost:6379",
		Password: "Aa1234", // no password set
		DB:       0,        // use default DB,
	})

	if err != nil {
		panic(err)
	}

	err = ps.Sub("test.message", func(msg *pubsub.Message) error {
		fmt.Println("MESSAGE: ", msg.Payload)

		return nil
	})
	if err != nil {
		panic(err)
	}

	err = ps.Sub("test.message", func(msg *pubsub.Message) error {
		fmt.Println("MESSAGE 2: ", msg.Payload)

		return nil
	})
	if err != nil {
		panic(err)
	}

	go func() {
		err = ps.Pub("test.message", &pubsub.Message{Payload: map[string]any{"status": "success"}})
		if err != nil {
			panic(err)
		}
	}()

	go func() {
		time.Sleep(time.Second * 5)
		ps.Close()
	}()

	ps.Wait()
}
