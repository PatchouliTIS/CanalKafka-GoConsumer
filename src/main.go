package main

import (
	consumer "c_k"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"

	"github.com/IBM/sarama"
)

var (
	ConsumerNumber uint
	Topic          string
	IPv4           string
	Port           string
)

type consumerGroupHandler struct {
	name string
}

func isValid(ipStr string) bool {
	ip := net.ParseIP(ipStr)
	return ip != nil && ip.To4() != nil
}

func (h consumerGroupHandler) Setup(session sarama.ConsumerGroupSession) error {

	return nil
}
func (h consumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }
func (h consumerGroupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession,
	claim sarama.ConsumerGroupClaim) error {

	for msg := range claim.Messages() {

		// 手动确认消息，将offset后移
		sess.MarkMessage(msg, "")

		// PrintProtoMessage(msg)
		// PrintJSONMessage(msg)
		if err := consumer.PrintJSONMessage(msg); err != nil {
			fmt.Printf("errors occur in retrieving messages:\t%s\n", err)
		}
		// PrintSimpleMSG(msg)

	}

	// if txCnt > 0 {
	// 	fmt.Printf("多个事物在这里执行\n")
	// 	txCnt = 0
	// }
	return nil
}

func main() {

	flag.UintVar(&ConsumerNumber, "csmr", 1, "config consumer number")
	flag.StringVar(&Topic, "topic", "", "config topic")
	flag.StringVar(&IPv4, "ipaddr", "127.0.0.1", "config kafka ipaddr")
	flag.StringVar(&Port, "port", "9092", "config kafka port")

	flag.Parse()
	if Topic == "" || len(Topic) == 0 {
		panic("Topic Unset!")
	}
	if isValid(IPv4) == false {
		panic("IPv4 Invalid!")
	}

	cgt := map[string]consumer.ConsumeTopic{}
	cgt["proto"] = consumer.ConsumeTopic{
		ConsumeNum: int(ConsumerNumber),
		Topics:     []string{Topic}, //canal_test_canal_logs
		Callback:   consumerGroupHandler{},
	}
	cClient := consumer.CreateNewConsumer([]string{IPv4 + ":" + Port}, cgt, nil)

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	select {
	case <-signals:
	}
	cClient.Close()
}
