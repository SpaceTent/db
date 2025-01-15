package mysql

import (
	"fmt"
	"reflect"
	"strings"
	"unicode"

	_ "github.com/go-sql-driver/mysql"
)

// decodeTags Turn a tag string into a map of key/value pairs
func decodeTag(tag string) map[string]string {

	lastQuote := rune(0)
	f := func(c rune) bool {
		switch {
		case c == lastQuote:
			lastQuote = rune(0)
			return false
		case lastQuote != rune(0):
			return false
		case unicode.In(c, unicode.Quotation_Mark):
			lastQuote = c
			return false
		default:
			return unicode.IsSpace(c)

		}
	}

	// splitting string by space but considering quoted section
	items := strings.FieldsFunc(tag, f)

	// create and fill the map
	m := make(map[string]string)
	for _, item := range items {
		x := strings.Split(item, "=")
		m[x[0]] = x[1]
	}

	// print the map
	// for k, v := range m {
	//    fmt.Printf("%s: %s\n", k, v)
	// }
	return m
}

// HexRepresentation Convert a string to a hex representation
func hexRepresentation(in string) string {
	return "X'" + fmt.Sprintf("%x", in) + "'"
	// return "'" + in + "'"
}

// getStructDetails Get the details of a struct
func getStructDetails[T any](dbFieldName string) (string, any) {

	var st T
	t := reflect.TypeOf(st)

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		tag := field.Tag.Get("db")
		dbStructureMap := decodeTag(tag)
		// l.INFO("%d. %v (%v), tag: '%v'\n", i+1, field.Name, field.Type.Name(), tag)
		// l.SPEW(field.Type)

		if dbStructureMap["column"] == dbFieldName {
			if field.Type == reflect.TypeOf([]uint8{}) {
				return field.Name, "[]uint8"
			} else if field.Type.Kind() == reflect.Pointer {
				return field.Name, "*" + field.Type.Elem().Name()
			} else {
				return field.Name, field.Type.Name()
			}
		}
	}
	return "", ""
}
