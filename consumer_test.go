package consumer

import (
	"fmt"
	"os"
	"os/signal"
	"testing"

	"github.com/IBM/sarama"
)

type consumerGroupHandler struct {
	name string
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
		if err := PrintJSONMessage(msg); err != nil {
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

func TestCreateNewConsumer(t *testing.T) {
	cgt := map[string]ConsumeTopic{}
	cgt["proto"] = ConsumeTopic{
		ConsumeNum: 3,
		Topics:     []string{"canal_proto"}, //canal_test_canal_logs
		Callback:   consumerGroupHandler{},
	}
	cClient := CreateNewConsumer([]string{"9.135.130.71:9092"}, cgt, nil)

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	select {
	case <-signals:
	}
	cClient.Close()
}
