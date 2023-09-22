package messenger

import (
	"fmt"
	"github.com/Agusmazzeo/GoUtils/src/servicebus"
	"github.com/Agusmazzeo/GoUtils/src/worker"
	"sync"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/google/uuid"
)

type MessengerType string

const (
	Receiver MessengerType = "receiver"
	Sender   MessengerType = "sender"
)

type Messenger struct {
	sbClient *servicebus.ServiceBusClient
	mutex    *sync.Mutex
}

func (m *Messenger) SbReceiverTask(w *worker.Worker, received *worker.Message) *worker.Message {
	data := received.GetData()
	m.mutex.Lock()
	sbMessages := m.sbClient.GetMessages(data["messages"].(int))
	m.mutex.Unlock()
	for _, sbMessage := range sbMessages {
		message := worker.NewMessage(nil, map[string]any{"message": *sbMessage})
		fmt.Println("Received Message:", sbMessage.MessageID)
		w.PushMessage(message)
	}
	return nil
}

// The ServiceBus Sender Worker is made for sending messages to a ServiceBus Queue and completing messages, when requested.
func (m *Messenger) SbSenderTask(w *worker.Worker, received *worker.Message) *worker.Message {
	data := received.GetData()
	sbMessage := data["message"].(azservicebus.ReceivedMessage)
	fmt.Println("Completed Message:", sbMessage.MessageID)
	m.sbClient.CompleteMessage(&sbMessage)
	return worker.NewMessage(nil, map[string]any{"messages": 1})
}

// The ServiceBus Receiver Worker is made for getting messages from a ServiceBus Queue
// only when the configured amount of messages to be processed by a single worker has not been reached.
func NewSbWorker(connectionString, sbQueueName string, mType MessengerType, input <-chan *worker.Message, output chan<- *worker.Message) *worker.Worker {
	id := uuid.New()
	var task worker.Task
	var mutex = &sync.Mutex{}
	client := servicebus.NewClient(connectionString, sbQueueName)
	messenger := &Messenger{sbClient: client, mutex: mutex}
	if mType == "receiver" {
		task = messenger.SbReceiverTask
	} else {
		task = messenger.SbSenderTask
	}
	return worker.NewWorker(id, input, output, task)
}
