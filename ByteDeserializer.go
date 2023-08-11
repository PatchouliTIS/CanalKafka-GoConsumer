package consumer

import (
	protocal_canalpacket "c_k/protocal.canalpacket"
	protocal_message "c_k/protocal.message"
	"errors"
	"fmt"

	"github.com/valyala/fastjson"

	"google.golang.org/protobuf/proto"
)

func Deserializer(data []byte, lazyParseEntry bool) *Message {
	if len(data) > 0 == false {
		fmt.Printf("Args Wrong! Data is empty!!!")
		return nil
	}

	// 分解data ---> packet
	packet := &protocal_canalpacket.Packet{}

	err := proto.Unmarshal(data, packet)
	if err != nil {
		fmt.Printf("Args Wrong! unmarshal data: %s", err)
		return nil
	}

	// 准备数据
	messages := &protocal_canalpacket.Messages{}
	ack := &protocal_canalpacket.Ack{}

	// 根据 packet.Type 决定处理逻辑
	switch packet.Type {
	case protocal_canalpacket.PacketType_MESSAGES:
		if !(packet.GetCompression() == protocal_canalpacket.Compression_NONE) && !(packet.GetCompression() == protocal_canalpacket.Compression_COMPRESSIONCOMPATIBLEPROTO2) { // NONE和兼容pb2的处理方式相同
			panic("compression is not supported in this connector")
		}
		// 分解packet.body --> Messages
		err = proto.Unmarshal(packet.Body, messages)
		if err != nil {
			fmt.Printf("Args Wrong! unmarshal packet: %s", err)
			return nil
		}

		// 分解messages ---> message
		result := &Message{
			ID: messages.BatchId,
		}
		if lazyParseEntry {
			// byteString
			result.RawEntries = messages.Messages
			result.Raw = true
		} else {
			// fmt.Printf("messages' size --> %d \n", len(messages.Messages))

			for _, bytes := range messages.Messages {
				tmp := &protocal_message.Entry{}
				if err = proto.Unmarshal(bytes, tmp); err != nil {
					fmt.Printf("Args Wrong! unmarshal messages' bytes: %s", err)
					return nil
				} else {
					result.AddEntry(tmp)
				}
			}
			result.Raw = false
		}
		return result

	case protocal_canalpacket.PacketType_ACK:
		err := proto.Unmarshal(packet.Body, ack)
		if err != nil {
			return nil
		}
		panic(errors.New(fmt.Sprintf("something goes wrong with reason:%s", ack.GetErrorMessage())))
	default:
		panic(errors.New(fmt.Sprintf("unexpected packet type:%s", packet.Type)))
	}

}

func ReadingFlatMSG(data []byte) *CommonMessage {
	parser := fastjson.Parser{}

	value, err := parser.ParseBytes(data)
	if err != nil {
		panic(err)
	}

	message := &CommonMessage{}

	// 解析JSON数据并填充到CommonMessage结构体
	message.Database = string(value.GetStringBytes("database"))
	message.Table = string(value.GetStringBytes("table"))
	message.PkNames = make([]string, 0)
	for _, pk := range value.GetArray("pkNames") {
		message.PkNames = append(message.PkNames, string(pk.GetStringBytes()))
	}
	message.IsDdl = value.GetBool("isDdl")
	message.Type = string(value.GetStringBytes("type"))
	message.Es = value.GetInt64("es")
	message.Ts = value.GetInt64("ts")
	message.SQL = string(value.GetStringBytes("sql"))

	dataArray := value.GetArray("data")
	message.Data = make([]map[string]interface{}, 0)
	for _, data := range dataArray {
		dataMap := make(map[string]interface{})

		dataMap["id"] = data.GetInt64("id")
		dataMap["comment"] = data.GetStringBytes("comment")
		dataMap["created_time"] = data.GetStringBytes("created_time")
		dataMap["updated_time"] = data.GetStringBytes("updated_time")

		message.Data = append(message.Data, dataMap)
	}

	oldArray := value.GetArray("old")
	message.Old = make([]map[string]interface{}, 0)
	for _, oldData := range oldArray {
		oldDataMap := make(map[string]interface{})

		oldDataMap["id"] = oldData.GetInt64("id")
		oldDataMap["comment"] = oldData.GetStringBytes("comment")
		oldDataMap["created_time"] = oldData.GetStringBytes("created_time")
		oldDataMap["updated_time"] = oldData.GetStringBytes("updated_time")

		message.Old = append(message.Old, oldDataMap)
	}

	return message

}
