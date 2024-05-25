package main
 
import (
  "encoding/json"
  "fmt"
 
  "github.com/IBM/sarama"
  "github.com/gofiber/fiber/v2"
)
 
type Comment struct {
  Text string `form:"text" json:"text"`
}
 
func main() {
  app := fiber.New()
  api := app.Group("/api/v1")
  api.Post("/comments", createComment)
 
  app.Listen(":5000")
}
 
func ConnectProducer(brokersUrl []string) (sarama.SyncProducer, error) {
  config := sarama.NewConfig()
  config.Producer.Return.Successes = true
  config.Producer.RequiredAcks = sarama.WaitForAll
  config.Producer.Retry.Max = 5
 
  conn, err := sarama.NewSyncProducer(brokersUrl, config)
  if err != nil {
    return nil, err
  }
 
  return conn, err
}
 
func PushCommentToQueue(topic string, message []byte) error {
  brokersUrl := []string{"localhost:9092"}
  producer, err := ConnectProducer(brokersUrl)
  if err != nil {
    return err
  }
  defer producer.Close()
  msg := &sarama.ProducerMessage{
    Topic: topic,
    Value: sarama.StringEncoder(message),
  }
 
  partition, offset, err := producer.SendMessage(msg)
  if err != nil {
    return err
  }
 
  fmt.Printf("Message stored successfully in topic(%s)/partition(%d)/offset(%d)", topic, partition, offset)
  return nil
}
 
func createComment(c *fiber.Ctx) error {
  cmt := new(Comment)
  err := c.BodyParser(cmt)
  if err != nil {
    c.Status(400).JSON(&fiber.Map{
      "success": false,
      "message": err,
    })
    return err
  }
 
  cmtInBytes, err := json.Marshal(cmt)
  if err != nil {
    return err
  }
  PushCommentToQueue("comments", cmtInBytes)
 
  err = c.JSON(&fiber.Map{
    "success": true,
    "message": "Comment pushed successfully",
    "comment": cmt,
  })
  if err != nil {
    c.Status(500).JSON(&fiber.Map{
      "success": false,
      "message": "Error creating product",
    })
    return err
  }
 
  return err
}
 