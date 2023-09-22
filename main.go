package main

import "GoUtils/src/worker"

func main() {

	inputChannel1 := make(chan worker.IMessage, 10)
	outputChannel1 := make(chan worker.IMessage, 10)
	outputChannel2 := make(chan worker.IMessage, 10)
	outputChannel3 := make(chan worker.IMessage, 10)

	defer close(inputChannel1)
	defer close(outputChannel1)
	defer close(outputChannel2)
	defer close(outputChannel3)

	worker1 := worker.NewWorker(1, inputChannel1, outputChannel1)
	worker2 := worker.NewWorker(2, outputChannel1, outputChannel2)
	worker3 := worker.NewWorker(3, outputChannel2, outputChannel3)

	go worker1.Run()
	go worker2.Run()
	go worker3.Run()

	data := make(map[string]any)
	data["message"] = "Im a test message"
	message := worker.NewMessage(&data)

	inputChannel1 <- message

	<-outputChannel3

}
