package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/IBM/sarama"
)

func main() {
	topic := "comments"
	worker, err := connectConsumer([]string{"localhost:29092"})
	if err != nil {
		panic(err)

	}

	consumer, err := worker.ConsumePartition(topic, 0, sarama.OffsetOldest) // to perform the consumer partition of the worker and then input the arguaments of topic then partition as 0 and then set offsetoldest
	if err != nil {
		panic(err)
	}

	fmt.Println("consumer started") // to print the statement that the consumer started

	sigchan := make(chan os.Signal, 1)                      // Now here we will be creating a Channel with sygnal channel and sycalls
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM) // this is for the graceful shutdown of the channel where SIGTERM is for listennign to the messages

	msgCount := 0 // to initiate the messages numbers to be zero and then consume the messages and count from the queue or from the stream

	doneCh := make(chan struct{}) // here we will be notifying that i m done consuming from the channel

	//Now here we will start a go routine
	go func() {
		for {
			select {
			case err := <-consumer.Errors():
				fmt.Println(err)
			case msg := <-consumer.Messages():
				msgCount++
				fmt.Printf("Received message COunt: %d: | Topic (%s) | Message(%s)\n", msgCount, string(msg.Topic), string(msg.Value))
			case <-sigchan:
				fmt.Println("Interuption detected") // if we found any interruption in the service then we will have it shut gracefully
				doneCh <- struct{}{}                // advertise on the done channel to tag that i m done for the consuming the message on the stream
				return                              // to exit the goroutine
			}
		}
	}()

	<-doneCh // to end the channel that we ahave counted in the worker by printing the message count as well
	fmt.Println("Processed", msgCount, "messages")
	if err := worker.Close(); err != nil { // if we found any error then just close the worker and raect as acoording to the code of panic(err)
		panic(err)
	}
}
func connectConsumer(brokersURL []string) (sarama.Consumer, error) { // here the connect consumer will be providing brokersURL in the string form
	config := sarama.NewConfig() // here configuration will be done through sarama for connecting consumer to the worker
	config.Consumer.Return.Errors = true
	conn, err := sarama.NewConsumer(brokersURL, config) // here the brokers URL will be provided for the configuration and then the error will be checked if we catched any
	if err != nil {
		return nil, err
	}
	return conn, nil
}
