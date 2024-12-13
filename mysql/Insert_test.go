package mysql

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type InsertPerson[StatusType uint | uint8 | uint16 | uint32 | uint64 | int | int8 | int16 | int32 | int64 | float32 | float64 | string | bool] struct {
	Id      int        `db:"column=id primarykey=yes table=Users"`
	Name    string     `db:"column=name"`
	Dtadded time.Time  `db:"column=dtadded omit=yes"`
	Status  StatusType `db:"column=status"`
}

type InsertPersonTime struct {
	Id      int       `db:"column=id primarykey=yes table=Users"`
	Name    string    `db:"column=name"`
	Dtadded time.Time `db:"column=dtadded"`
}

func generateInsertPerson[StatusType uint | uint8 | uint16 | uint32 | uint64 | int | int8 | int16 | int32 | int64 | float32 | float64 | string | bool](value StatusType) InsertPerson[StatusType] {
	return InsertPerson[StatusType]{
		0, "Test", time.Now(), value,
	}
}

func generateInsertPersonTime(id int) InsertPersonTime {
	return InsertPersonTime{id, "Test", time.Date(2024, time.December, 7, 15, 29, 25, 10, time.UTC)}
}

func generateArrayInsertPerson[StatusType uint | uint8 | uint16 | uint32 | uint64 | int | int8 | int16 | int32 | int64 | float32 | float64 | string | bool](value StatusType) []InsertPerson[StatusType] {
	return []InsertPerson[StatusType]{
		{0, "Test", time.Now(), value},
		{1, "Test", time.Now(), value},
		{2, "Test", time.Now(), value},
		{3, "Test", time.Now(), value},
		{4, "Test", time.Now(), value},
	}
}

func generateArrayInsertPersonTime() []InsertPersonTime {
	output := make([]InsertPersonTime, 5)
	for i := 0; i < 5; i++ {
		output[i] = generateInsertPersonTime(i)
	}
	return output
}

func testInsertNumericalErrorValueHelper(t *testing.T, sql string, err error) {
	assert.NoError(t, err)
	assert.Equal(t, "INSERT INTO Users(name,status) VALUES (X'54657374',1);", sql)
}

func testInsertStringErrorValueHelper(t *testing.T, sql string, err error) {
	assert.NoError(t, err)
	assert.Equal(t, `INSERT INTO Users(name,status) VALUES (X'54657374',X'31');`, sql)
}

func testInsertBoolErrorValueHelper(t *testing.T, sql string, err error) {
	assert.NoError(t, err)
	assert.Equal(t, "INSERT INTO Users(name,status) VALUES (X'54657374',true);", sql)
}

func testInsertTimeErrorValueHelper(t *testing.T, sql string, err error) {
	assert.NoError(t, err)
	assert.Equal(t, `INSERT INTO Users(name,dtadded) VALUES (X'54657374','2024-12-07 15:29:25');`, sql)
}

func testInsertManyNumericalErrorValueHelper(t *testing.T, sql string, err error) {
	assert.NoError(t, err)
	assert.Equal(t, `INSERT INTO Users(name,status) VALUES (X'54657374',1)
(X'54657374',1)
(X'54657374',1)
(X'54657374',1)
(X'54657374',1);`, sql)
}

func testInsertManyStringErrorValueHelper(t *testing.T, sql string, err error) {
	assert.NoError(t, err)
	assert.Equal(t, `INSERT INTO Users(name,status) VALUES (X'54657374',X'31')
(X'54657374',X'31')
(X'54657374',X'31')
(X'54657374',X'31')
(X'54657374',X'31');`, sql)
}

func testInsertManyBoolErrorValueHelper(t *testing.T, sql string, err error) {
	assert.NoError(t, err)
	assert.Equal(t, `INSERT INTO Users(name,status) VALUES (X'54657374',true)
(X'54657374',true)
(X'54657374',true)
(X'54657374',true)
(X'54657374',true);`, sql)
}

func testInsertManyTimeErrorValueHelper(t *testing.T, sql string, err error) {
	assert.NoError(t, err)
	assert.Equal(t, `INSERT INTO Users(name,dtadded) VALUES (X'54657374','2024-12-07 15:29:25')
(X'54657374','2024-12-07 15:29:25')
(X'54657374','2024-12-07 15:29:25')
(X'54657374','2024-12-07 15:29:25')
(X'54657374','2024-12-07 15:29:25');`, sql)
}

func TestInsert(t *testing.T) {
	New("", nil)
	sql, err := DB.Insert(generateInsertPerson(uint(1)))
	testInsertNumericalErrorValueHelper(t, sql, err)
	sql, err = DB.Insert(generateInsertPerson(uint8(1)))
	testInsertNumericalErrorValueHelper(t, sql, err)
	sql, err = DB.Insert(generateInsertPerson(uint16(1)))
	testInsertNumericalErrorValueHelper(t, sql, err)
	sql, err = DB.Insert(generateInsertPerson(uint32(1)))
	testInsertNumericalErrorValueHelper(t, sql, err)
	sql, err = DB.Insert(generateInsertPerson(uint64(1)))
	testInsertNumericalErrorValueHelper(t, sql, err)
	sql, err = DB.Insert(generateInsertPerson(int(1)))
	testInsertNumericalErrorValueHelper(t, sql, err)
	sql, err = DB.Insert(generateInsertPerson(int8(1)))
	testInsertNumericalErrorValueHelper(t, sql, err)
	sql, err = DB.Insert(generateInsertPerson(int16(1)))
	testInsertNumericalErrorValueHelper(t, sql, err)
	sql, err = DB.Insert(generateInsertPerson(int32(1)))
	testInsertNumericalErrorValueHelper(t, sql, err)
	sql, err = DB.Insert(generateInsertPerson(int64(1)))
	testInsertNumericalErrorValueHelper(t, sql, err)
	sql, err = DB.Insert(generateInsertPerson(float32(1)))
	testInsertNumericalErrorValueHelper(t, sql, err)
	sql, err = DB.Insert(generateInsertPerson(float64(1)))
	testInsertNumericalErrorValueHelper(t, sql, err)
	sql, err = DB.Insert(generateInsertPerson(true))
	testInsertBoolErrorValueHelper(t, sql, err)

	sql, err = DB.Insert(generateInsertPerson("1"))
	testInsertStringErrorValueHelper(t, sql, err)

	sql, err = DB.Insert(generateInsertPersonTime(0))
	testInsertTimeErrorValueHelper(t, sql, err)
}

func TestInsertMany(t *testing.T) {
	New("", nil)

	sql, err := InsertMany[InsertPerson[uint]](generateArrayInsertPerson(uint(1)))
	testInsertManyNumericalErrorValueHelper(t, sql, err)
	sql, err = InsertMany[InsertPerson[uint8]](generateArrayInsertPerson(uint8(1)))
	testInsertManyNumericalErrorValueHelper(t, sql, err)
	sql, err = InsertMany[InsertPerson[uint16]](generateArrayInsertPerson(uint16(1)))
	testInsertManyNumericalErrorValueHelper(t, sql, err)
	sql, err = InsertMany[InsertPerson[uint32]](generateArrayInsertPerson(uint32(1)))
	testInsertManyNumericalErrorValueHelper(t, sql, err)
	sql, err = InsertMany[InsertPerson[uint64]](generateArrayInsertPerson(uint64(1)))
	testInsertManyNumericalErrorValueHelper(t, sql, err)
	sql, err = InsertMany[InsertPerson[int]](generateArrayInsertPerson(int(1)))
	testInsertManyNumericalErrorValueHelper(t, sql, err)
	sql, err = InsertMany[InsertPerson[int8]](generateArrayInsertPerson(int8(1)))
	testInsertManyNumericalErrorValueHelper(t, sql, err)
	sql, err = InsertMany[InsertPerson[int16]](generateArrayInsertPerson(int16(1)))
	testInsertManyNumericalErrorValueHelper(t, sql, err)
	sql, err = InsertMany[InsertPerson[int32]](generateArrayInsertPerson(int32(1)))
	testInsertManyNumericalErrorValueHelper(t, sql, err)
	sql, err = InsertMany[InsertPerson[int64]](generateArrayInsertPerson(int64(1)))
	testInsertManyNumericalErrorValueHelper(t, sql, err)
	sql, err = InsertMany[InsertPerson[float32]](generateArrayInsertPerson(float32(1)))
	testInsertManyNumericalErrorValueHelper(t, sql, err)
	sql, err = InsertMany[InsertPerson[float64]](generateArrayInsertPerson(float64(1)))
	testInsertManyNumericalErrorValueHelper(t, sql, err)
	sql, err = InsertMany[InsertPerson[bool]](generateArrayInsertPerson(true))
	testInsertManyBoolErrorValueHelper(t, sql, err)

	sql, err = InsertMany[InsertPerson[string]](generateArrayInsertPerson("1"))
	testInsertManyStringErrorValueHelper(t, sql, err)

	sql, err = InsertMany[InsertPersonTime](generateArrayInsertPersonTime())
	testInsertManyTimeErrorValueHelper(t, sql, err)
}

func TestInsertManyNoEntries(t *testing.T) {
	sql, err := InsertMany[InsertPerson[uint]]([]InsertPerson[uint]{})
	assert.NoError(t, err)
	assert.Equal(t, sql, "")
}

func TestNoColumnNameInsert(t *testing.T) {
	type testType1 struct {
		Id      int       `db:"column=id primarykey=yes table=Users"`
		Name    string    `db:"column="`
		Dtadded time.Time `db:"column=dtadded"`
	}
	type testType2 struct {
		Id      int       `db:"column=id primarykey=yes table=Users"`
		Name    string    `db:""`
		Dtadded time.Time `db:"column=dtadded"`
	}
	type testType3 struct {
		Id      int `db:"column=id primarykey=yes table=Users"`
		Name    string
		Dtadded time.Time `db:"column=dtadded"`
	}

	testTypeEntry1 := testType1{0, "Test", time.Now()}
	testTypeEntry2 := testType2{0, "Test", time.Now()}
	testTypeEntry3 := testType3{0, "Test", time.Now()}

	sql, err := DB.Insert(testTypeEntry1)
	assert.EqualError(t, err, "no column name specified for field Name")
	assert.Empty(t, sql)

	sql, err = DB.Insert(testTypeEntry2)
	assert.EqualError(t, err, "no column name specified for field Name")
	assert.Empty(t, sql)

	sql, err = DB.Insert(testTypeEntry3)
	assert.EqualError(t, err, "no column name specified for field Name")
	assert.Empty(t, sql)

	testTypeEntry1Array := []testType1{{0, "Test", time.Now()}, {1, "Test", time.Now()}}
	testTypeEntry2Array := []testType2{{0, "Test", time.Now()}, {1, "Test", time.Now()}}
	testTypeEntry3Array := []testType3{{0, "Test", time.Now()}, {1, "Test", time.Now()}}

	sql, err = InsertMany[testType1](testTypeEntry1Array)
	assert.EqualError(t, err, "no column name specified for field Name")
	assert.Empty(t, sql)

	sql, err = InsertMany[testType2](testTypeEntry2Array)
	assert.EqualError(t, err, "no column name specified for field Name")
	assert.Empty(t, sql)

	sql, err = InsertMany[testType3](testTypeEntry3Array)
	assert.EqualError(t, err, "no column name specified for field Name")
	assert.Empty(t, sql)
}

func TestNoTableNameInsert(t *testing.T) {
	type testType1 struct {
		Id      int       `db:"column=id primarykey=yes"`
		Name    string    `db:"column=name"`
		Dtadded time.Time `db:"column=dtadded"`
	}

	testTypeEntry1 := testType1{0, "Test", time.Now()}

	sql, err := DB.Insert(testTypeEntry1)
	assert.EqualError(t, err, "no table found in structure")
	assert.Empty(t, sql)

	testTypeEntry1Array := []testType1{{0, "Test", time.Now()}, {1, "Test", time.Now()}}

	sql, err = InsertMany[testType1](testTypeEntry1Array)
	assert.EqualError(t, err, "no table found in structure")
	assert.Empty(t, sql)
}

func TestNoFieldsInsert(t *testing.T) {
	type testType1 struct {
	}
	type testType2 struct {
		Id      int       `db:"column=id primarykey=yes table=Users"`
		Name    string    `db:"column=name omit=yes"`
		Dtadded time.Time `db:"column=dtadded omit=yes"`
	}

	testTypeEntry1 := testType1{}
	testTypeEntry2 := testType2{0, "Test", time.Now()}

	testTypeEntry1Array := []testType1{{}, {}}
	testTypeEntry2Array := []testType2{{0, "Test", time.Now()}, {1, "Test", time.Now()}}

	sql, err := DB.Insert(testTypeEntry1)
	assert.EqualError(t, err, "no table found in structure")
	assert.Empty(t, sql)

	sql, err = DB.Insert(testTypeEntry2)
	assert.EqualError(t, err, "no non-primary key and non-omitted fields found in structure")
	assert.Empty(t, sql)

	sql, err = InsertMany[testType1](testTypeEntry1Array)
	assert.EqualError(t, err, "no table found in structure")
	assert.Empty(t, sql)

	sql, err = InsertMany[testType2](testTypeEntry2Array)
	assert.EqualError(t, err, "no non-primary key and non-omitted fields found in structure")
	assert.Empty(t, sql)
}
