package worker

type Distributor struct {
	input          <-chan *Message
	workerChannels []chan<- *Message
}

func NewDistributor(input <-chan *Message, workerChannels []chan<- *Message) *Distributor {
	return &Distributor{input: input, workerChannels: workerChannels}
}
