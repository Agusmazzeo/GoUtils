package messenger

import (
	"github.com/Agusmazzeo/GoUtils/src/servicebus"
	"github.com/Agusmazzeo/GoUtils/src/worker"
	"testing"
	"time"
)

func TestMessenger(t *testing.T) {
	connectionString := "Endpoint=sb://carvana-vdi-dev.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=n/JXm+yt//V3Bg356p8EfSi5zJDVj2F/p/OsehYsnJs="
	queueName := "go-sb-concept"

	receiverInputChan := make(chan *worker.Message, 100)
	receiverOutputChan := make(chan *worker.Message, 100)
	senderInputChan := receiverOutputChan
	senderOutputChan := receiverInputChan

	receiverMessenger := NewSbWorker(connectionString, queueName, Receiver, receiverInputChan, receiverOutputChan)
	senderMessenger := NewSbWorker(connectionString, queueName, Sender, senderInputChan, senderOutputChan)

	testClient := servicebus.NewClient(connectionString, queueName)
	for range [10]int{} {
		testClient.SendMessage("Hello World!")
	}

	go receiverMessenger.Run()
	go senderMessenger.Run()

	receiverInputChan <- worker.NewMessage(nil, map[string]any{"messages": 10})

	time.Sleep(20 * time.Second)
}
