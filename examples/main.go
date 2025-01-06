package main

import (
	"fmt"
	"os"
	"time"

	"log/slog"

	MySQL "github.com/SpaceTent/db/mysql"
)

type UpdatePerson struct {
	Id      int       `db:"column=id primarykey=yes table=Users"`
	Name    string    `db:"column=name"`
	Dtadded time.Time `db:"column=dtadded"`
	Status  int       `db:"column=status"`
	Ignored int       `db:"column=ignored omit=yes"`
}

type InsertPerson struct {
	Id      int       `db:"column=id primarykey=yes table=Users"`
	Name    string    `db:"column=name"`
	Dtadded time.Time `db:"column=dtadded"`
	Status  int       `db:"column=status"`
	Ignored int       `db:"column=ignored omit=yes"`
}

// InsertOneExample shows how to insert a single row into the table
func InsertOneExample(l *slog.Logger) {
	// First create the structure
	entry := InsertPerson{
		Id:      12,
		Name:    "Test",
		Dtadded: time.Now(),
		Status:  1,
	}
	// Now create the query
	sqlQuery, err := MySQL.DB.Insert(entry)
	if err != nil {
		l.Error(err.Error())
		return
	}
	// Then execute the query
	lastInsertedID, rowsAffected, err := MySQL.DB.Execute(sqlQuery)
	if err != nil {
		l.Error(err.Error())
	}
	l.Info(fmt.Sprintf("Item with ID %d was inserted. %d rows were affected", lastInsertedID, rowsAffected))
}

// UpdateExample shows how to update an entry in the table
func UpdateExample(l *slog.Logger) {
	// First create the structure
	p := UpdatePerson{
		Id:      12,
		Name:    "Test",
		Dtadded: time.Now(),
		Status:  1,
	}
	// Now create the query
	sqlQuery, err := MySQL.DB.Update(p)
	if err != nil {
		l.Error(err.Error())
	}
	// Then execute it
	lastInsertedID, rowsAffected, err := MySQL.DB.Execute(sqlQuery)
	if err != nil {
		l.Error(err.Error())
		return
	}
	l.Info(fmt.Sprintf("Item with ID %d was updated. %d rows were affected", lastInsertedID, rowsAffected))
}

func main() {

	DSN := ""
	textHandler := slog.NewTextHandler(os.Stdout, nil)
	l := slog.New(textHandler)

	MySQL.New(DSN, l)
	InsertOneExample(l)
	UpdateExample(l)
}
