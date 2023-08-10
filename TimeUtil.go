package consumer

import (
	"fmt"
	"strings"
	"time"
)

func String2Stamp(t *string) *int64 {
	index := strings.LastIndex(*t, "+")

	substr := (*t)[:index]

	substr = strings.Trim(substr, " ")

	timeFormat := "2006-01-02 15:04:05.000"

	timeObject, err := time.Parse(timeFormat, substr)
	if err != nil {
		fmt.Println("解析时间字符串失败：", err)
		return nil
	}

	timeStamp := timeObject.UnixMicro()

	return &timeStamp
}
