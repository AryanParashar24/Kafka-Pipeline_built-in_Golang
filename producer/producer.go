package main

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/IBM/sarama"
	"github.com/gofiber/fiber/v2"
)

type Comment struct {
	Text string `form:"text" json:"text"` // so the form will be in the text format and also the json will be in the text format
}

func main() {
	app := fiber.New()         // here this is the functtion which will help in creating new server and application using fiber
	api := app.Group("api/v1") // cerating a group in api which will further work on the post method
	api.Post("/comment", createComment)
	app.Listen(":3000") // Here it iwll be set to listen to the application instead of fucntioning with the api server by setting up the port which is responsible for listenning the comments and wherer the server will be residing is set
} // Now the main will call the function createComment wihc will further call and connect all the other functions as well inthe queue.

func ConnectProducer(brokerURL []string) (sarama.SyncProducer, error) { // will be using some sarama library as well in the function for defining different parts
	// so it will take brokerURL as its only input and use sarama to SyncProducer or else will produce error if it gets stuck in the process
	config := sarama.NewConfig()                     // Now here we will set up a new config and then will set up new values for our configuration
	config.Producer.Return.Successes = true          // defining for the success status
	config.Producer.RequiredAcks = sarama.WaitForAll // defining for the requireemnts to be fulfilled by the producer engine before it exit the program
	config.Producer.Retry.Max = 5                    // maximum amount of retrys allowed in the producer program

	conn, err := sarama.NewSyncProducer(brokerURL, config) //so here i just need to pass the brokers URL & the configuration as mentioned above to set up a connection with NewSyncProducer
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func PushCommentToQueue(topic string, message []byte) error {
	brokersURL := []string{"localhost:29092"}    // creating the brokers url and then calling the connectProducer in the next line
	producer, err := ConnectProducer(brokersURL) // now here our function will forward to the function that we ccreated which spcifies Connection to Producer while Pushing Comment to the Queue
	if err != nil {                              // specify the error if present and then close the push comment window for a moment to the queue
		return err
	}
	defer producer.Close()          // defer to close the profucer window so that the request that has been passed to be pussed to the queue could be stored and executed
	msg := &sarama.ProducerMessage{ // now here after closing the funtion for connecting producer i will cast a very nice message on sarama which requires two things one is topic and the another one is value
		Topic: topic,                         // as defined in the explanation about the topic of the input that would be stored of the message on Kafka
		Value: sarama.StringEncoder(message), // here it specifies about the value of the message that would be pushed ot the kafka
	} // now here then after casting the message in the sarama we will send the message

	partition, offset, err := producer.SendMessage(msg) // message will be sent using producer which has connection to the broker through SendMessage
	if err != nil {
		return err
	} // so now here the error got while casting and then while sedning the message will be printed otherwise the message will be printed showing the places where the messahe has been stored shoring its topic, partition and offset.

	fmt.Printf("message is store in topic(%s)/partition(%d)/offset(%d)\n", topic, partition, offset)
	return nil
}

func createComment(c *fiber.Ctx) error { // so the body that we sent to the API and the request will be available with the help of the fibre context c
	cmt := new(Comment)                       // new instance of comment struct is been producaed and is then later stored in cmt
	if err := c.BodyParser(cmt); err != nil { // Here we'll convert it into comment struct. with the bodyparser then it will convert whtever body is sent to format of comemnt i defined for in the format aboev in the Comment struct
		log.Println(err) // if error is not null hten flag the error statemnt by printing it as according to the errors been defined below in the algo below in the same function body
		c.Status(400).JSON(&fiber.Map{
			"success": false,
			"message": err,
		})
		return err
	}
	cmtInBytes, err := json.Marshal(cmt) // Marshal the comment struct to JSON bytes
	if err != nil {                      // Check for errors during marshaling
		log.Println(err)                      // Log the error
		return c.Status(500).JSON(&fiber.Map{ // Return a 500 status with the error message
			"success": false,
			"message": "Error marshalling comment",
		})
	}
	err = PushCommentToQueue("comments", cmtInBytes) // Push the comment to Kafka
	if err != nil {                                  // Check for errors during pushing to the queue
		log.Println(err)                      // Log the error
		return c.Status(500).JSON(&fiber.Map{ // Return a 500 status with the error message
			"success": false,
			"message": "Error sending comment to the queue",
		})
	}
	err = c.JSON(&fiber.Map{
		"success": true,
		"message": "Comment has been sent to the queue",
		"comment": cmt,
	})
	return err
}
