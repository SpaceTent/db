package mysql

import (
	"reflect"

	l "log/slog"
)

// You can't do Method Generic types in Go, so we have to use a function.

func QueryStruct[T any](sql string, parameters ...any) ([]T, error) {

	// First of all, get all the database records, ising the old Record/Field method.
	allRecords, err := DB.Query(sql, parameters...)
	if err != nil {
		return make([]T, 0), err
	}

	results := make([]T, 0)

	for i, record := range allRecords {
		var newStructRecord T

		for k, v := range record {
			// Use Reflection to set the value.

			structFieldName, structFieldType := getStructDetails[T](k)

			// l.INFO("index:%d Key:%s Value:%v structFieldName:%v structFieldType:%v", i, k, "", structFieldName, structFieldType)

			switch structFieldType {
			case "int", "int8", "int16", "int32", "int64":
				// l.INFO("Setting Int64 field: %s to %v type: %T", structFieldName, v.Value, v.Value)
				reflect.ValueOf(&newStructRecord).Elem().FieldByName(structFieldName).SetInt(v.AsInt64())

			case "*int", "*int8", "*int16", "*int32", "*int64":
				// l.INFO("Setting Int64 field: %s to %v type: %T", structFieldName, v.Value, v.Value)
				var valueOf reflect.Value
				switch structFieldType {
				case "*int":
					valueOf = toIntPtr[int](v.AsInt64Ptr())
				case "*int8":
					valueOf = toIntPtr[int8](v.AsInt64Ptr())
				case "*int16":
					valueOf = toIntPtr[int16](v.AsInt64Ptr())
				case "*int32":
					valueOf = toIntPtr[int32](v.AsInt64Ptr())
				case "*int64":
					valueOf = reflect.ValueOf(v.AsInt64Ptr())
				}
				reflect.ValueOf(&newStructRecord).Elem().FieldByName(structFieldName).Set(valueOf)

			case "uint", "uint8", "uint16", "uint32", "uint64":
				reflect.ValueOf(&newStructRecord).Elem().FieldByName(structFieldName).SetUint(v.AsUInt64())

			case "*uint", "*uint8", "*uint16", "*uint32", "*uint64":
				var valueOf reflect.Value
				switch structFieldType {
				case "*uint":
					valueOf = toUIntPtr[uint](v.AsUInt64Ptr())
				case "*uint8":
					valueOf = toUIntPtr[uint8](v.AsUInt64Ptr())
				case "*uint16":
					valueOf = toUIntPtr[uint16](v.AsUInt64Ptr())
				case "*uint32":
					valueOf = toUIntPtr[uint32](v.AsUInt64Ptr())
				case "*uint64":
					valueOf = reflect.ValueOf(v.AsUInt64Ptr())
				}
				reflect.ValueOf(&newStructRecord).Elem().FieldByName(structFieldName).Set(valueOf)

			case "bool":
				reflect.ValueOf(&newStructRecord).Elem().FieldByName(structFieldName).SetBool(v.AsBool())

			case "*bool":
				reflect.ValueOf(&newStructRecord).Elem().FieldByName(structFieldName).Set(reflect.ValueOf(v.AsBoolPtr()))

			case "float32", "float64":
				// l.INFO("Setting flaot64 field: %s to %v", structFieldName, v.Value)
				reflect.ValueOf(&newStructRecord).Elem().FieldByName(structFieldName).SetFloat(v.AsFloat())

			case "*float32", "*float64":
				var valueOf reflect.Value
				switch structFieldType {
				case "*float32":
					valueOf = toFloatPtr[float32](v.AsFloatPtr())
				case "*float64":
					valueOf = reflect.ValueOf(v.AsFloatPtr())
				}
				reflect.ValueOf(&newStructRecord).Elem().FieldByName(structFieldName).Set(valueOf)

			case "string":
				// l.INFO("Setting String field: %s to %v", structFieldName, v.Value)
				reflect.ValueOf(&newStructRecord).Elem().FieldByName(structFieldName).SetString(v.AsString())
			case "*string":
				// l.INFO("Setting String field: %s to %v", structFieldName, v.Value)
				reflect.ValueOf(&newStructRecord).Elem().FieldByName(structFieldName).Set(reflect.ValueOf(v.AsStringPtr()))

			case "Time":
				// l.INFO("Setting Time field: %s to %v", structFieldName, v.Value)
				reflect.ValueOf(&newStructRecord).Elem().FieldByName(structFieldName).Set(reflect.ValueOf(v.AsDate("")))

			case "*Time":
				// l.INFO("Setting String field: %s to %v", structFieldName, v.Value)
				reflect.ValueOf(&newStructRecord).Elem().FieldByName(structFieldName).Set(reflect.ValueOf(v.AsDatePtr("")))

				// Add Blob Support.
			case "[]uint8":
				reflect.ValueOf(&newStructRecord).Elem().FieldByName(structFieldName).Set(reflect.ValueOf(v.AsByte()))
				// l.INFO("Setting Blob field: %s to %v", structFieldName, v.Value)

			default:
				l.With("col", k).With("index", i).With("structFieldName", structFieldName).With("structFieldType", structFieldType).Error("Database column was not found")
			}
		}

		results = append(results, newStructRecord)
	}
	return results, nil
}

// You can't do Method Generic types in Go, so we have to use a function.

func QuerySingleStruct[T any](sql string, parameters ...any) (T, error) {

	var SingleResult T

	results, err := QueryStruct[T](sql, parameters...)
	if err != nil {
		return SingleResult, err
	}
	if len(results) == 0 {
		return SingleResult, nil
	}
	return results[0], nil
}
