package servicebus

import (
	"context"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
)

type ServiceBusClient struct {
	client    *azservicebus.Client
	sender    *azservicebus.Sender
	receiver  *azservicebus.Receiver
	queueName string
}

func NewClient(connectionString, queueName string) *ServiceBusClient {

	client, err := azservicebus.NewClientFromConnectionString(connectionString, nil)
	if err != nil {
		panic(err)
	}

	sender, err := client.NewSender(queueName, nil)
	if err != nil {
		panic(err)
	}

	receiver, err := client.NewReceiverForQueue(queueName, nil)
	if err != nil {
		panic(err)
	}
	return &ServiceBusClient{
		client:    client,
		sender:    sender,
		receiver:  receiver,
		queueName: queueName,
	}
}

func (s *ServiceBusClient) SendMessage(message string) {
	sbMessage := &azservicebus.Message{
		Body: []byte(message),
	}
	err := s.sender.SendMessage(context.TODO(), sbMessage, nil)
	if err != nil {
		panic(err)
	}
}

func (s *ServiceBusClient) SendMessageBatch(messages []string) {
	batch, err := s.sender.NewMessageBatch(context.TODO(), nil)
	if err != nil {
		panic(err)
	}

	for _, message := range messages {
		if err := batch.AddMessage(&azservicebus.Message{Body: []byte(message)}, nil); err != nil {
			panic(err)
		}
	}
	if err := s.sender.SendMessageBatch(context.TODO(), batch, nil); err != nil {
		panic(err)
	}
}

func (s *ServiceBusClient) GetMessages(messagesToGet int) []*azservicebus.ReceivedMessage {
	messages, err := s.receiver.ReceiveMessages(context.TODO(), messagesToGet, nil)
	if err != nil {
		panic(err)
	}
	return messages
}

func (s *ServiceBusClient) CompleteMessage(message *azservicebus.ReceivedMessage) {
	err := s.receiver.CompleteMessage(context.TODO(), message, nil)
	if err != nil {
		panic(err)
	}
}

func (s *ServiceBusClient) Close() {
	s.sender.Close(context.TODO())
	s.receiver.Close(context.TODO())
	s.client.Close(context.TODO())
}
