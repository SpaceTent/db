package mysql

import "fmt"

func (db *Database) Query(sql string, parameters ...any) ([]Record, error) {

	allRecords := make([]Record, 0)

	DatabaseConnection, err := getConnection()
	if err != nil {
		return allRecords, err
	}
	fmt.Printf("sql: %s params: %+v\n", sql, parameters)
	rows, err := DatabaseConnection.Query(sql, parameters...)

	if err != nil {
		return allRecords, err
	}
	defer rows.Close()

	columns, _ := rows.Columns()
	count := len(columns)
	values := make([]interface{}, count)
	valuePtrs := make([]interface{}, count)

	for rows.Next() {
		fmt.Println("PONG")
		for i := range columns {
			valuePtrs[i] = &values[i]
		}
		if err := rows.Scan(valuePtrs...); err != nil {
			fmt.Printf("ERROR: %s\n", err.Error())
		}

		out := Record{}

		for i, col := range columns {
			val := values[i]

			// TODO: Implement All the Types!

			// nolint:gosimple
			switch val.(type) {
			case uint, uint8, uint16, uint32, uint64, int, int8, int16, int32, int64:
				// fmt.Printf("Int: %v\n", val)
				out[col] = Field{Value: val}
			case float32, float64:
				// fmt.Printf("Float64: %v\n", val)
				out[col] = Field{Value: val}
			case bool:
				out[col] = Field{Value: val}
			case string:
				out[col] = Field{Value: val}

			case []uint8:
				b, _ := val.([]byte)
				// fmt.Printf("String: %s\n", string(b))
				// l.INFO("Type: %T", val)
				out[col] = Field{Value: string(b)}

			case interface{}:
				// l.ERROR("Unknown Type: %T", val)
				// If the Record is NULL
				out[col] = Field{Value: val}

			default:
				// l.ERROR("Unknown Type: %T", val)
				out[col] = Field{Value: val}
			}
		}
		allRecords = append(allRecords, out)
	}

	return allRecords, nil
}
