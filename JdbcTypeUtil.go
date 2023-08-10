package consumer

/**
JDBC中将每个MySQL中的数据使用一个整型int标识。

查阅java.sql.Types库，获取整型对应的数据后在Golang中进行对应的转换

*/

import (
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"time"
)

func TypeConvert(tableName, columnName, value string, sqlType int32, mysqlType string) interface{} {
	if value == "" && !(isText(mysqlType) || sqlType == 1 || sqlType == 12 || sqlType == -1) {
		return nil
	}

	switch sqlType {
	case 4: // Types.INTEGER
		res, _ := strconv.Atoi(value)
		return res
	case 5: // Types.SMALLINT
		res, _ := strconv.Atoi(value)
		return int16(res)
	case -6, -7: // Types.TINYINT & Types.BIT
		res, _ := strconv.Atoi(value)
		return int8(res)
	case -5: // Types.BIGINT
		if strings.Contains(mysqlType, "bigint") && strings.HasSuffix(mysqlType, "unsigned") {
			res, _ := new(big.Int).SetString(value, 10)
			return res
		}
		res, _ := strconv.ParseInt(value, 10, 64)
		return res
	case 7: // Types.REAL
		res, _ := strconv.ParseFloat(value, 32)
		return float32(res)
	case 8, 6: // Types.FLOAT, Types.DOUBLE
		res, _ := strconv.ParseFloat(value, 64)
		return res
	case 3, 2: // Types.DECIMAL, Types.NUMERIC
		res, _ := new(big.Float).SetString(value)
		return res
	case -2, -3, -4, 2004: // Types.BINARY, Types.VARBINARY, Types.LONGVARBINARY, Types.BLOB
		return []byte(value)
	case 91: // Types.DATE
		if !strings.HasPrefix(value, "0000-00-00") {
			date, err := time.Parse("2006-01-02", value)
			if err == nil {
				return date
			}
		}
		return nil
	case 92: // Types.TIME
		date, err := time.Parse("15:04:05", value)
		if err == nil {
			return date
		}
		return nil
	case 93: // Types.TIMESTAMP
		if !strings.HasPrefix(value, "0000-00-00") {

			// 定义时区标准
			location, err := time.LoadLocation("Asia/Shanghai")
			if err != nil {
				fmt.Printf("JdbcType Convert Failed! Types.TIMESTAMP:%s", err)
			}

			date, err := time.ParseInLocation("2006-01-02 15:04:05.000", value, location)
			if err == nil {
				return date
			}
		}
		return nil
	case 16: // Types.BOOLEAN
		return value != "0"
	case 2005: // Types.CLOB
	default:
		return value
	}

	return nil
}

func isText(columnType string) bool {
	textTypes := []string{"LONGTEXT", "MEDIUMTEXT", "TEXT", "TINYTEXT"}
	columnType = strings.ToUpper(columnType)
	for _, t := range textTypes {
		if columnType == t {
			return true
		}
	}
	return false
}
