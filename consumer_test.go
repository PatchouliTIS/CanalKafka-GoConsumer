package consumer

import (
	"os"
	"os/signal"
	"testing"

	"github.com/IBM/sarama"
)

type consumerGroupHandler struct {
	name string
}

func (h consumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (h consumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }
func (h consumerGroupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession,
	claim sarama.ConsumerGroupClaim) error {

	for msg := range claim.Messages() {

		PrintProtoMessage(msg)
		// PrintFlatMessage(msg)
		// PrintPureFlat(msg)
		// PrintSimpleMSG(msg)

		// 手动确认消息，将offset后移
		sess.MarkMessage(msg, "")
	}

	// if txCnt > 0 {
	// 	fmt.Printf("多个事物在这里执行\n")
	// 	txCnt = 0
	// }
	return nil
}

func TestCreateNewConsumer(t *testing.T) {
	cgt := map[string]ConsumeTopic{}
	cgt["id01"] = ConsumeTopic{
		ConsumeNum: 1,
		Topics:     []string{"cavalry_division"}, //canal_test_canal_logs
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
