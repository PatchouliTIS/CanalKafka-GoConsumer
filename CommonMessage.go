package consumer

import (
	"fmt"
	"time"
)

const CommonMSGSerialVersionUID int64 = 2611556444074013268

type CommonMessage struct {
	Database string                   // 数据库或schema
	Table    string                   // 表名
	PkNames  []string                 // 主键名列表
	IsDdl    bool                     // 是否是DDL操作
	Type     string                   // 类型：INSERT/UPDATE/DELETE
	Es       int64                    // binlog执行时间，执行耗时
	Ts       int64                    // DML构建时间戳，同步时间
	SQL      string                   // 执行的SQL，DML SQL为空
	Data     []map[string]interface{} // 数据列表
	Old      []map[string]interface{} // 旧数据列表，用于更新，size和data的size一一对应
}

func (c *CommonMessage) Clear() {
	c.Database = ""
	c.Table = ""
	c.Type = ""
	c.Ts = 0
	c.Es = 0
	c.Data = nil
	c.Old = nil
	c.SQL = ""
}

func (c *CommonMessage) String() string {
	return fmt.Sprintf("CommonMessage{Database: %s, Table: %s, PkNames: %v, IsDdl: %t, Type: %s, Es: %d, Ts: %d, SQL: %s, Data: %v, Old: %v}",
		c.Database, c.Table, c.PkNames, c.IsDdl, c.Type, c.Es, c.Ts, c.SQL, c.Data, c.Old)
}

func (c *CommonMessage) GetCreatedTime() []time.Time {
	// fmt.Printf("---->> CommonMessage Data Size: %d  \n", len(c.Data))

	var arr []time.Time
	if len(c.Data) == 0 {
		return nil
	}

	for _, m := range c.Data {
		ctime, ok := m["created_time"].(time.Time)
		if ok {
			arr = append(arr, ctime)
		} else {
			fmt.Println("interface{} 转换为 time.Time 失败！")
			return nil
		}
	}
	return arr
}

func (c *CommonMessage) GetUpdatedTime() []time.Time {
	// fmt.Printf("---->> CommonMessage Data Size: %d  \n", len(c.Data))

	var arr []time.Time

	if len(c.Data) == 0 {
		return nil
	}

	for _, m := range c.Data {
		ctime, ok := m["updated_time"].(time.Time)
		if ok {
			arr = append(arr, ctime)
		} else {
			fmt.Println("interface{} 转换为 time.Time 失败！")
			return nil
		}
	}
	return arr
}

func (c *CommonMessage) GetId() int {
	if len(c.Data) == 0 {
		return -1
	}

	for _, m := range c.Data {
		id, ok := m["id"].(int)
		if ok {
			return id
		} else {
			fmt.Println("interface{} 转换为 time.Time 失败！")
			return -1
		}
	}

	return -1

}
