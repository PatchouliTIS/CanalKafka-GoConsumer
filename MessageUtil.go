package consumer

import (
	protocal_message "c_k/protocal.message"
	"errors"
	"time"

	"google.golang.org/protobuf/proto"
)

func Convert(msg *Message) ([]*CommonMessage, error) {
	if msg == nil {
		return nil, errors.New("Args Wrong! Message is null\n")
	}

	entries := msg.Entries

	// fmt.Println(len(entries))

	msgs := make([]*CommonMessage, len(entries))

	// fmt.Println(len(msgs))

	for i, entry := range entries {
		if entry.GetEntryType() == protocal_message.EntryType_TRANSACTIONBEGIN || entry.GetEntryType() == protocal_message.EntryType_TRANSACTIONEND {
			continue
		}

		rowChange := &protocal_message.RowChange{}
		err := proto.Unmarshal(entry.GetStoreValue(), rowChange)
		if err != nil {
			return nil, errors.New("protobuf Unmarshal Failed! entry.GetStoreValue()")
		}

		eventType := rowChange.GetEventType()

		cmsg := &CommonMessage{
			IsDdl:    rowChange.GetIsDdl(),
			Database: entry.GetHeader().GetSchemaName(),
			Table:    entry.GetHeader().GetTableName(),
			Type:     eventType.String(),
			Es:       entry.GetHeader().GetExecuteTime(),
			// IsDdl:    rowChange.GetIsDdl(),
			Ts:   time.Now().UnixNano() / int64(time.Millisecond),
			SQL:  rowChange.GetSql(),
			Data: make([]map[string]interface{}, 0),
			Old:  make([]map[string]interface{}, 0),
		}

		msgs[i] = cmsg
		// msgs = append(msgs, cmsg)

		// fmt.Printf("After append: %d", len(msgs))
		// data := make([]map[string]interface{}, 0)
		// old :=  make([]map[string]interface{}, 0)

		if !rowChange.GetIsDdl() {
			updateSet := make(map[string]bool)
			cmsg.PkNames = make([]string, 0)
			for index, rowData := range rowChange.GetRowDatas() {
				if eventType != protocal_message.EventType_INSERT && eventType != protocal_message.EventType_UPDATE && eventType != protocal_message.EventType_DELETE {
					continue
				}

				row := make(map[string]interface{})
				var columns []*protocal_message.Column

				if eventType == protocal_message.EventType_DELETE {
					columns = rowData.GetBeforeColumns()
				} else {
					columns = rowData.GetAfterColumns()
				}

				for _, column := range columns {
					if index == 0 {
						if column.GetIsKey() {
							cmsg.PkNames = append(cmsg.PkNames, column.GetName())
						}
					}

					if column.GetIsNull() {
						row[column.GetName()] = nil
					} else {

						row[column.GetName()] = TypeConvert(cmsg.Table, column.Name, column.Value, column.SqlType, column.MysqlType)
					}

					if column.GetUpdated() {
						updateSet[column.GetName()] = true
					}
				}

				if len(row) > 0 {
					cmsg.Data = append(cmsg.Data, row)
				}

				if eventType == protocal_message.EventType_UPDATE {
					rowOld := make(map[string]interface{}, 0)
					for _, bfrColumn := range rowData.GetBeforeColumns() {
						if _, ok := updateSet[bfrColumn.Name]; ok {
							if bfrColumn.GetIsNull() {
								rowOld[bfrColumn.GetName()] = nil
							} else {
								rowOld[bfrColumn.GetName()] = TypeConvert(cmsg.Table, bfrColumn.Name, bfrColumn.Value, bfrColumn.SqlType, bfrColumn.MysqlType)
							}
						}
					}

					if len(rowOld) > 0 {
						cmsg.Old = append(cmsg.Old, rowOld)
					}
				}
			}

		}

	}
	return msgs, nil
}
