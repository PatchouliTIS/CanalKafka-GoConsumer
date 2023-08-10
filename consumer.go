package consumer

import (
	"context"

	"github.com/IBM/sarama"
)

type ConsumeTopic struct {
	//一个消费者组里包含几个消费者
	ConsumeNum int
	//消费者组监听的主题
	Topics []string
	//回调的Handler, 需要调用者自己实现
	Callback sarama.ConsumerGroupHandler
}

type consumer struct {
	//kafka地址集合: 例如[]string{域名:9092, ip:9093, ...}
	//addressSet []string
	//消费者组绑定主题: key为group主题id
	consumerGroupTopic map[string]ConsumeTopic
	//consumer配置
	consumerConfig *sarama.Config
	client         sarama.Client

	consumerGroup []sarama.ConsumerGroup
}

// 创建消费者对象
// addressSet: kafka地址集合
// consumerGroupTopic: 消费者组信息
// consumerConfig: 消费者配置信息，如果为空就采用默认的配置
func CreateNewConsumer(addressSet []string, consumerGroupTopic map[string]ConsumeTopic, consumerConfig *sarama.Config) *consumer {
	consumer := consumer{
		//addressSet:         addressSet,
		consumerGroupTopic: consumerGroupTopic,
		consumerConfig:     consumerConfig,
	}
	//启用默认配置
	if consumer.consumerConfig == nil {
		consumer.consumerConfig = sarama.NewConfig()
		consumer.consumerConfig.Consumer.Return.Errors = false
		consumer.consumerConfig.Version = sarama.V2_6_0_0
		consumer.consumerConfig.Consumer.Offsets.Initial = sarama.OffsetOldest
	}

	client, err := sarama.NewClient(addressSet, consumer.consumerConfig)
	if err != nil {
		panic(err)
	}
	consumer.client = client

	//开始
	consumer.init()
	return &consumer
}

// 初始化
func (c *consumer) init() {
	for groupId, v := range c.consumerGroupTopic {
		// 1. 消费者组：不可控制分区，但是多线程消费
		consumerGroup, err := sarama.NewConsumerGroupFromClient(groupId, c.client)
		if err != nil {
			panic(err)
		}

		for i := 0; i < v.ConsumeNum; i++ {
			go c.consume(&consumerGroup, c.consumerGroupTopic[groupId].Topics, c.consumerGroupTopic[groupId].Callback)
		}

		c.consumerGroup = append(c.consumerGroup, consumerGroup)
	}
}

func (c *consumer) consume(group *sarama.ConsumerGroup, topics []string, consumerGroupHandler sarama.ConsumerGroupHandler) {
	ctx := context.Background()
	for {
		err := (*group).Consume(ctx, topics, consumerGroupHandler)
		if err != nil {
			panic(err)
		}
	}
}

// 关闭
func (c *consumer) Close() {
	for _, v := range c.consumerGroup {
		v.Close()
	}
	c.client.Close()
}
