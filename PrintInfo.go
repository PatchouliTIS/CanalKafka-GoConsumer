package consumer

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/IBM/sarama"
)

// var earliestES int64 = 0
var earliestTS int64 = 0
var earliestCT int64 = 0 // SQL建立时间

// var earliestUT int64 = 0 // SQL更新时间

// var latestES int64 = 0
var latestTS int64 = 0
var latestCT int64 = 0 // SQL建立时间
// var latestUT int64 = 0 // SQL更新时间

var preCT int64 = 0

var beforeDES int64
var afterDES int64

var cost int64

var current int64

var txCnt int64

var orders []int

var sql2logs float64
var sqlCnt int64
var logs2kafka float64
var logsCnt int64
var kafka2server float64
var kafkaCnt int64
var meanCost float64
var costCnt int64

var index int

func PrintProtoMessage(msg *sarama.ConsumerMessage) error {
	if msg == nil {
		return errors.New("Message is Empty!")
	}

	// beforeDES = time.Now().UnixMilli()

	// fmt.Printf("time - earliestCT = %d - %d = %d\n", beforeDES, earliestCT, beforeDES-earliestCT)

	// 反序列化
	// fmt.Println(">>>ENTERING KAFKA VALUE DES...")
	message := Deserializer(msg.Value, false)
	kafkaTime := msg.Timestamp.UnixMilli()
	// fmt.Printf("FINISHING KAFKA VALUE DES: message RawEntries -> %s \t messages Enties -> %s \n", message.RawEntries, message.Entries)
	// fmt.Println(">>>ENTERING MESSAGE DES...")
	if commonMessages, err := Convert(message); err == nil {
		// fmt.Printf("FINISHING commonMessages VALUE DES: commonMessages size -> %d\n", len(commonMessages))
		// afterDES = time.Now().UnixMilli()

		// fmt.Printf("ENTERING Traversing CMSG>>>\n")

		for _, cmsg := range commonMessages {
			if cmsg == nil {
				continue
			}

			// fmt.Printf("ENTERING CMSG CREATE/UPDATE TIME >>> .   time size: %d . %d\n", len(cmsg.GetCreatedTime()), len(cmsg.GetUpdatedTime()))
			// for ptr := 0; ptr < len(cmsg.GetCreatedTime()); ptr++ {
			// 	fmt.Printf("latestCT: %d \t", cmsg.GetCreatedTime()[ptr].UnixMicro())
			// }
			// fmt.Printf("\nes:%d\n", cmsg.Es)

			current = time.Now().UnixMilli()

			if earliestCT == 0 || current-earliestCT > 30000 {
				fmt.Printf(">>>>>>>>>RESET milestone<<<<<<<<<<<<<\n")
				earliestTS = cmsg.Ts
				if len(cmsg.Data) > 0 && (cmsg.Type == "INSERT") {
					earliestCT = cmsg.GetCreatedTime()[0].UnixMilli()
				} else {
					earliestCT = cmsg.Es
				}

				sql2logs = 0
				sqlCnt = 0
				kafka2server = 0
				kafkaCnt = 0
				meanCost = 0
				costCnt = 0

			}

			size := len(cmsg.GetCreatedTime())
			latestTS = cmsg.Ts
			if len(cmsg.Data) > 0 && (cmsg.Type == "INSERT") {
				latestCT = cmsg.GetCreatedTime()[size-1].UnixMilli()
			} else {
				latestCT = cmsg.Es
			}

			// 先行计算每个语句的值
			sql2logs += float64(latestTS) - float64(latestCT)
			sqlCnt++

			// if msg.Timestamp.UnixMilli() > cmsg.Ts {
			// 	logs2kafka += float64(kafkaTime) - float64(cmsg.Ts)
			// 	logsCnt++
			// }

			kafka2server += float64(current) - float64(latestTS)
			kafkaCnt++

			// 将 ID 号收集起来
			// if len(cmsg.Data) > 0 && cmsg.GetId() != -1 {
			// 	orders = append(orders, cmsg.GetId())
			// }

			// preCT = earliestCT

			// 使用latestCT 与 earliestCT 进行比较，如果不同则说明进入到第二个事务执行阶段
			// if latestCT > preCT {
			// 	txCnt++
			// 	// fmt.Printf("》〉》〉〉》新事务执行时间：%d \n", latestCT)
			// 	preCT = latestCT
			// }

			meanCost += float64(current - latestCT)
			costCnt++

			// fmt.Printf("第 %d 个消息，其创建时间为%s，更新时间为%s, 当前服务器时间为：%s \n  解析消息消耗了：%d 。 ", idx, cmsg.GetCreatedTime()[0], cmsg.GetUpdatedTime()[0], time.Now(), afterDES-beforeDES)
			// fmt.Printf("MySQL数据库到Go服务器总共花费：%d： \n 其中数据库到canal日志花费：%d，  canal日志写入Kafka花费：%d，  Kafka到Go客户端花费: %d \n", cost, cmsg.Ts-latestCT, msg.Timestamp.UnixMilli()-cmsg.Ts, current-msg.Timestamp.UnixMilli())

		}

	} else {
		fmt.Printf("FAILED commonMessages DES: %s \n", err)
	}

	fmt.Printf("本批次内：\t更新条目:%d \n最早数据库时间:\t%d\t最早DB日志时间:\t%d    \n最新数据库时间:\t%d\t最新DB日志时间:\t%d   \nKafkaTime:\t%d\tServerTime:\t%d\n总共从数据库到客户端:\t%d\t从Canal到消费客户端:%d\nsql2logs:\t%f\tlogs2server:\t%f\tsql2server:\t%f\n",
		len(message.Entries), earliestCT, earliestTS, latestCT, latestTS, kafkaTime, current, current-earliestCT, current-earliestTS, sql2logs/float64(sqlCnt), kafka2server/float64(kafkaCnt), meanCost/float64(costCnt)) ///float64(kafkaCnt)

	fmt.Printf("-----------------------------------------------------------------------------------\n")
	// fmt.Print("当前次序：\n")
	// for _, v := range orders {
	// 	fmt.Printf("%d . ", v)
	// }
	// fmt.Print("\n-----------------------------------------------------------------------\n")
	return nil
}

func PrintFlatMessage(msg *sarama.ConsumerMessage) error {
	if msg == nil {
		return errors.New("Message is Empty!")
	}

	current = time.Now().UnixMilli()

	values := string(msg.Value)

	index = strings.LastIndex(values, "ts")

	tsStr := values[index+4 : index+17]

	index = strings.LastIndex(values, "\"es\"")

	esStr := values[index+5 : index+18]

	var err error

	if earliestCT == 0 || current-earliestCT > 60000 {

		fmt.Printf(">>>>>>>>>RESET milestone<<<<<<<<<<<<<\n")

		earliestTS, err = strconv.ParseInt(tsStr, 10, 64)
		earliestCT, err = strconv.ParseInt(esStr, 10, 64)

		sql2logs = 0
		sqlCnt = 0
		kafka2server = 0
		kafkaCnt = 0
		meanCost = 0
		costCnt = 0

	}

	latestTS, err = strconv.ParseInt(tsStr, 10, 64)
	latestCT, err = strconv.ParseInt(esStr, 10, 64)

	// 先行计算每个语句的值
	sql2logs += float64(latestTS) - float64(latestCT)
	sqlCnt++

	kafka2server += float64(current) - float64(latestTS)
	kafkaCnt++

	meanCost += float64(current - latestCT)
	costCnt++

	if err != nil {
		fmt.Println(err)
	}

	// fmt.Printf("Message topic:%q\tKafkaEntityCreatedTime:%d\tClientCurrentTime:%d \nPartition:%d\tOffset:%d\nKey:%s\n",
	// 	msg.Topic, msg.Timestamp.UnixMicro(), time.Now().UnixMicro(), msg.Partition, msg.Offset, string(msg.Key))

	fmt.Printf("本批次内 \n最早数据库时间:\t%d\t最早DB日志时间:\t%d    \n最新数据库时间:\t%d\t最新DB日志时间:\t%d   \nKafkaTime:\t%d\tServerTime:\t%d\n总共从数据库到客户端:\t%d\t从Canal到消费客户端:%d\nsql2logs:\t%f\tlogs2server:\t%f\tsql2server:\t%f\n",
		earliestCT, earliestTS, latestCT, latestTS, msg.Timestamp.UnixMilli(), current, current-earliestCT, current-earliestTS, sql2logs/float64(sqlCnt), kafka2server/float64(kafkaCnt), meanCost/float64(costCnt)) ///float64(kafkaCnt)

	fmt.Printf("-----------------------------------------------------------------------------------\n")
	return nil
}

func PrintPureFlat(msg *sarama.ConsumerMessage) error {
	if msg == nil {
		return errors.New("Message is Empty!")
	}

	cmsg := ReadingFlatMSG(msg.Value)

	kafkaTime := msg.Timestamp.UnixMilli()

	current = time.Now().UnixMilli()

	if earliestCT == 0 || current-earliestCT > 60000 {
		fmt.Printf(">>>>>>>>>RESET milestone<<<<<<<<<<<<<\n")
		earliestTS = cmsg.Ts
		if len(cmsg.Data) > 0 && (cmsg.Type == "INSERT") {
			tmpTimeByte, ok := cmsg.Data[0]["created_time"].([]byte)
			if ok {
				timeStr := string(tmpTimeByte)

				timeStamp, err := time.ParseInLocation("2006-01-02 15:04:05.000", timeStr, time.Local)
				if err != nil {
					panic(err)
				} else {
					earliestCT = timeStamp.UnixMilli()
				}
			} else {
				panic(ok)
			}
		} else {
			earliestCT = cmsg.Es
		}

		sql2logs = 0
		sqlCnt = 0
		kafka2server = 0
		kafkaCnt = 0
		meanCost = 0
		costCnt = 0

	}

	size := len(cmsg.Data)
	latestTS = cmsg.Ts
	if len(cmsg.Data) > 0 && (cmsg.Type == "INSERT") {
		tmpTimeByte, ok := cmsg.Data[size-1]["created_time"].([]byte)
		if ok {
			timeStr := string(tmpTimeByte)
			timeStamp, err := time.ParseInLocation("2006-01-02 15:04:05.000", timeStr, time.Local)
			if err != nil {
				panic(err)
			} else {
				latestCT = timeStamp.UnixMilli()
			}
		} else {
			panic(ok)
		}
	} else {
		latestCT = cmsg.Es
	}

	// 先行计算每个语句的值
	sql2logs += float64(latestTS) - float64(latestCT)
	sqlCnt++

	kafka2server += float64(current) - float64(latestTS)
	kafkaCnt++

	meanCost += float64(current - latestCT)
	costCnt++

	// fmt.Printf("Message topic:%q\tKafkaEntityCreatedTime:%d\tClientCurrentTime:%d \nPartition:%d\tOffset:%d\nKey:%s\nValues:%+v\n\n",
	// 	msg.Topic, msg.Timestamp.UnixMicro(), time.Now().UnixMicro(), msg.Partition, msg.Offset, string(msg.Key), string(msg.Value))
	fmt.Printf("本批次内：\t更新条目:%d \n最早数据库时间:\t%d\t最早DB日志时间:\t%d    \n最新数据库时间:\t%d\t最新DB日志时间:\t%d   \nKafkaTime:\t%d\tServerTime:\t%d\n总共从数据库到客户端:\t%d\t从Canal到消费客户端:%d\nsql2logs:\t%f\tlogs2server:\t%f\tsql2server:\t%f\n",
		len(cmsg.Data), earliestCT, earliestTS, latestCT, latestTS, kafkaTime, current, current-earliestCT, current-earliestTS, sql2logs/float64(sqlCnt), kafka2server/float64(kafkaCnt), meanCost/float64(costCnt)) ///float64(kafkaCnt)

	fmt.Printf("-----------------------------------------------------------------------------------\n")
	return nil
}

func PrintSimpleMSG(msg *sarama.ConsumerMessage) error {
	fmt.Printf("Message topic:%q\tKafkaEntityCreatedTime:%d\tClientCurrentTime:%d \nPartition:%d\tOffset:%d\nKey:%s\nValues:%+v\n\n",

		msg.Topic, msg.Timestamp.UnixMicro(), time.Now().UnixMicro(), msg.Partition, msg.Offset, string(msg.Key), string(msg.Value))

	return nil
}
