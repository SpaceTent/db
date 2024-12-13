package mysql

import (
	"errors"
	"fmt"
	l "log/slog"
	"reflect"
	"strings"
	"time"
)

// Insert generates an SQL query based on the db column tags provided in the structure of the argument
func (db *Database) Insert(dbStructure any) (string, error) {
	t := reflect.TypeOf(dbStructure)
	table, buildSql, err := generateBuildSql(dbStructure, t)
	if err != nil {
		return "", err
	}
	if table == "" {
		return "", fmt.Errorf("no table found in structure")
	}
	if buildSql == "" {
		return "", fmt.Errorf("no non-primary key and non-omitted fields found in structure")
	}
	valueSql, err := generateValuesSql(dbStructure, t)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("INSERT INTO %s(%s) VALUES %s;", table, buildSql, valueSql), nil
}

// InsertMany generates an SQL query based on the db column tags provided in the structure of the elements in the argument
func InsertMany[T any](dbStructures []T) (string, error) {
	if len(dbStructures) == 0 {
		return "", nil
	}
	t := reflect.TypeOf(dbStructures[0])
	table, buildSql, err := generateBuildSql(dbStructures[0], t)
	if err != nil {
		return "", err
	}
	if table == "" {
		return "", fmt.Errorf("no table found in structure")
	}
	if buildSql == "" {
		return "", fmt.Errorf("no non-primary key and non-omitted fields found in structure")
	}
	var valuesSql strings.Builder
	entriesLength := len(dbStructures)
	for i, dbStructure := range dbStructures {
		valueSql, err := generateValuesSql(dbStructure, t)
		if err != nil {
			return "", err
		}
		valuesSql.WriteString(valueSql)
		if i < entriesLength-1 {
			valuesSql.WriteString("\n")
		}
	}
	return fmt.Sprintf("INSERT INTO %s(%s) VALUES %s;", table, buildSql, valuesSql.String()), nil
}

// generateBuildSql creates the part of the insert SQL query which specifies which columns are to be inserted
func generateBuildSql(dbStructure any, t reflect.Type) (table string, buildSql string, err error) {
	var sb strings.Builder

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		tag := field.Tag.Get("db")
		dbStructureMap := decodeTag(tag)

		if reflect.ValueOf(dbStructure).Field(i).CanInterface() {

			if dbStructureMap["column"] == "" {
				return "", "", errors.New("no column name specified for field " + field.Name)
			}

			if dbStructureMap["table"] != "" {
				table = dbStructureMap["table"]
			}

			if dbStructureMap["omit"] != "yes" && dbStructureMap["primarykey"] != "yes" {
				sb.WriteString(dbStructureMap["column"] + ",")
			}
		}
	}

	return table, strings.TrimSuffix(sb.String(), ","), err
}

// generateValuesSql creates the part of insert SQL query that adds each entry for each structure
func generateValuesSql(dbStructure any, t reflect.Type) (string, error) {
	var sb strings.Builder
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		tag := field.Tag.Get("db")
		dbStructureMap := decodeTag(tag)

		if reflect.ValueOf(dbStructure).Field(i).CanInterface() {
			value := reflect.ValueOf(dbStructure).Field(i).Interface()

			if dbStructureMap["column"] == "" {
				return "", errors.New("no column name specified for field" + field.Type.Name())
			}

			if dbStructureMap["omit"] != "yes" && dbStructureMap["primarykey"] != "yes" {
				switch field.Type.Name() {
				case "uint", "uint8", "uint16", "uint32", "uint64", "int8", "int16", "int", "int32", "int64":
					sb.WriteString(fmt.Sprintf("%v,", value))
				case "string":
					sb.WriteString(hexRepresentation(value.(string)) + ",")
				case "float32", "float64":
					sb.WriteString(fmt.Sprintf("%v,", value))
				case "Time":
					sb.WriteString(fmt.Sprintf("'%s',", value.(time.Time).Format("2006-01-02 15:04:05")))
				default:
					l.With("type", field.Type.Name()).With("value", value).Error("type error")
					sb.WriteString(fmt.Sprintf(`'%s',`, value.(string)))
				}
			}
		}
	}
	return fmt.Sprintf("(%s)", strings.TrimSuffix(sb.String(), ",")), nil
}
