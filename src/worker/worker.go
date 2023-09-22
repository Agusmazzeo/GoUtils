package worker

import (
	"fmt"

	uuid "github.com/google/uuid"
)

type IWorker interface {
	Run()
}

type Task func(*Worker, *Message) *Message

type Worker struct {
	id     uuid.UUID
	input  <-chan *Message
	output chan<- *Message
	task   Task
}

func NewWorker(id uuid.UUID, input <-chan *Message, output chan<- *Message, task Task) *Worker {
	if input == nil {
		panic("Input channel cannot be nil")
	}
	return &Worker{
		id:     id,
		input:  input,
		output: output,
		task:   task,
	}
}

func (w *Worker) Run() {
	defer close(w.output)
	fmt.Printf("Started Worker %s\n", w.id.String())
	for received := range w.input {
		go w.RunTask(received)
	}
}

func (w *Worker) RunTask(received *Message) {
	result := w.task(w, received)

	w.PushMessage(result)
}

func (w *Worker) PushMessage(message *Message) {
	if w.output == nil || message == nil {
		return
	}
	w.output <- message
}
