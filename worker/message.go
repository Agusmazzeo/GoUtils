package worker

type IMessage interface {
	GetData() *map[string]any
}

type Message struct {
	destination *string
	data        map[string]any
}

func NewMessage(destination *string, data map[string]any) *Message {
	return &Message{destination: destination, data: data}
}

func (m *Message) GetData() map[string]any {
	return m.data
}

func (m *Message) GetDestination() string {
	return *m.destination
}
