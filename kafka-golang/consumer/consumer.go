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
  worker, err := connectConsumer([]string{"localhost:9092"})
  if err != nil {
    panic(err)
  }
 
  consumer, err := worker.ConsumePartition(topic, 0, sarama.OffsetOldest)
  if err != nil {
    panic(err)
  }
 
  fmt.Println("Consumer started")
  sigchan := make(chan os.Signal, 1)
  signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)
 
  msgCount := 0
 
  doneCh := make(chan struct{})
 
  go func() {
    for {
      select {
      case err := <-consumer.Errors():
        fmt.Println(err)
      case msg := <-consumer.Messages():
        msgCount++
        fmt.Printf("Received message Count: %d | Topic (%s) | Message(%s)\n", msgCount, string(msg.Topic), string(msg.Value))
      case <-sigchan:
        fmt.Println("Interruption detected")
        doneCh <- struct{}{}
      }
 
    }
  }()
 
  <-doneCh
  fmt.Println("Processed", msgCount, "messages")
  err = worker.Close()
  if err != nil {
    panic(err)
  }
}
 
func connectConsumer(brokersUrl []string) (sarama.Consumer, error) {
  config := sarama.NewConfig()
  config.Consumer.Return.Errors = false
  conn, err := sarama.NewConsumer(brokersUrl, config)
  if err != nil {
    return nil, err
  }
 
  return conn, nil
 
}
 