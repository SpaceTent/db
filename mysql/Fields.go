package mysql

import (
	"fmt"
	"strconv"
	"time"

	l "log/slog"
)

// The Database returns a map of []Records and each Record is a map of Fields.
// This provides a way to get the Field from the Record.

type Field struct {
	Value any
}

func (F Field) AsString() string {

	if F.Value == nil {
		return ""
	}

	switch v := F.Value.(type) {
	case int32:

	case int64:
		// Convert to a String
		return strconv.FormatInt(int64(F.Value.(int64)), 10)
	case float64:
		// fmt.Printf("Float64: %v\n", val)
		// Arry of Bytes.
	case []uint8:
		b, _ := F.Value.([]byte)
		return string(b)
	case string:
		return F.Value.(string)

	default:
		l.Error("Can not convert type: '" + fmt.Sprintf("%T", v) + "' to a String")
	}

	return F.Value.(string)
}

func (F Field) AsFloat() float64 {

	if F.Value == nil {
		return 0
	}

	switch v := F.Value.(type) {
	case int:
		return float64(F.Value.(int))
	case int8:
		return float64(F.Value.(int8))
	case int16:
		return float64(F.Value.(int16))
	case int32:
		return float64(F.Value.(int32))
	case int64:
		return float64(F.Value.(int64))
	case uint:
		return float64(F.Value.(uint))
	case uint8:
		return float64(F.Value.(uint8))
	case uint16:
		return float64(F.Value.(uint16))
	case uint32:
		return float64(F.Value.(uint32))
	case uint64:
		return float64(F.Value.(uint64))
	case float32:
		return float64(F.Value.(float32))
	case float64:
		return float64(F.Value.(float64))
	case string:
		floatVal, err := strconv.ParseFloat(F.Value.(string), 64)
		if err != nil {
			l.With("err", err.Error()).Error(fmt.Sprintf("Cannot convert %T to float64", v))
			return 0
		}
		return floatVal
	default:
		l.Error("Can not convert type: '" + fmt.Sprintf("%T", v) + "' to a float64")
	}

	return float64(F.Value.(float64))
}

func (F Field) AsDate(d string) time.Time {

	// https://github.com/go-sql-driver/mysql#timetime-support
	// This assumes this is OFF.

	if F.Value == nil {
		if d != "" {
			out, _ := time.Parse("2006-01-02 15:04:05", d)
			return out
		} else {
			return time.Time{}
		}
	}

	switch v := F.Value.(type) {
	case time.Time:
		return F.Value.(time.Time)
	case string:
		t, _ := time.Parse("2006-01-02 15:04:05", F.Value.(string))
		return t
	default:
		l.Error("Can not convert type: '" + fmt.Sprintf("%T", v) + "' to a Date")
		return F.Value.(time.Time)
	}

}

func (F Field) AsDateEpoch() int64 {

	if F.Value == nil {
		return 0
	}
	t, _ := time.Parse("2006-01-02 15:04:05", F.Value.(string))

	return t.Unix()
}

func (F Field) AsInt() int {

	// TODO:  If there is a NULL in the database.  the Interface is nil.

	if F.Value == nil {
		return 0
	}

	// This code is needed on each of the fields for flexiblity.  If you need to gt a Field from the database and have in the
	// code as a different Type.  Most of the time isn't going to be needed. This is (DEFAULT) conversion

	switch v := F.Value.(type) {
	case int:
		// Interface is a Int, so just so the conversion. (DEFAULT)
		return F.Value.(int)

	case int32:
		return int(F.Value.(int32))
	case int64:
		return int(F.Value.(int64))
	case float64:
		return int(F.Value.(float64))
	case float32:
		return int(F.Value.(float32))
	case []uint8:
		// Interface Value is an array of Characters Convert a string, then an Int
		b, _ := F.Value.([]byte)
		i, _ := strconv.Atoi(string(b))
		return i
	case string:
		i, _ := strconv.Atoi(F.Value.(string))
		return i

	default:
		l.Error("Can not convert type: '" + fmt.Sprintf("%T", v) + "' to an int")
	}

	return 0
}

func (F Field) AsInt64() int64 {

	// TODO:  If there is a NULL in the database.  the Interface is nil.

	if F.Value == nil {
		return 0
	}

	// This code is needed on each of the fields for flexiblity.  If you need to gt a Field from the database and have in the
	// code as a different Type.  Most of the time isn't going to be needed. This is (DEFAULT) conversion

	switch v := F.Value.(type) {
	case int:
		// Interface is a Int, so just so the conversion. (DEFAULT)
		return int64(F.Value.(int))
	case int32:
		return int64(F.Value.(int32))
	case int64:
		return F.Value.(int64)
	case float64:
		return int64(F.Value.(float64))
	case float32:
		return int64(F.Value.(float32))
	case string:
		i, _ := strconv.Atoi(F.Value.(string))
		return int64(i)

	default:
		l.Error("Can not convert type %T %v %v", v, v, F.Value)
	}

	return 0
}

func (F Field) AsByte() []byte {

	if F.Value == nil {
		return []byte{}
	}

	switch v := F.Value.(type) {

	case []uint8:
		// Interface Value is an array of Characters Convert a string, then an Int
		b, _ := F.Value.([]byte)
		return b

	case string:
		return []byte(F.Value.(string))

	default:
		l.Error("Can not convert type: '" + fmt.Sprintf("%T", v) + "' to a Bytes")
	}
	return []byte{}
}
