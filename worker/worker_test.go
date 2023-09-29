package worker

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestWorkerRun(t *testing.T) {
	t.Run("Test multiple runs are executed concurrently and all messages are processed correctly", func(t *testing.T) {
		input := make(chan *Message, 10)
		output := make(chan *Message, 10)

		worker := NewWorker(uuid.New(), input, output, func(w *Worker, received *Message) *Message {
			fmt.Println("Processed message:", received.GetData(), time.Now())
			return received
		})
		go worker.Run()
		for id := range [100]int{} {
			input <- NewMessage(nil, map[string]any{"messageID": id})
		}
		for range [100]int{} {
			fmt.Println("Output message:", (<-output).GetData(), time.Now())
		}
		close(input)
	})

	t.Run("Test worker with nil out channel works correctly", func(t *testing.T) {
		input := make(chan *Message, 10)
		var wg = sync.WaitGroup{}
		wg.Add(100)

		worker := NewWorker(uuid.New(), input, nil, func(w *Worker, received *Message) *Message {
			fmt.Println("Processed message:", received.GetData(), time.Now())
			wg.Done()
			return received
		})
		go worker.Run()
		for id := range [100]int{} {
			input <- NewMessage(nil, map[string]any{"messageID": id})
		}
		wg.Wait()
		close(input)
	})
}
