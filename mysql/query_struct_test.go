package mysql

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestDataTypes(t *testing.T) {
	fname := setUpSaveIntegrationTestConnection(t)

	// Test integers
	t.Run("Integer Types", func(t *testing.T) {
		type IntTypes struct {
			Id        *int    `db:"column=id"`
			IntVal    *int    `db:"column=intval"`
			Int8Val   *int8   `db:"column=int8val"`
			Int16Val  *int16  `db:"column=int16val"`
			Int32Val  *int32  `db:"column=int32val"`
			Int64Val  *int64  `db:"column=int64val"`
			UintVal   *uint   `db:"column=uintval"`
			Uint8Val  *uint8  `db:"column=uint8val"`
			Uint16Val *uint16 `db:"column=uint16val"`
			Uint32Val *uint32 `db:"column=uint32val"`
			Uint64Val *uint64 `db:"column=uint64val"`
		}
		_, err := DB.dbConnection.Exec(`
        CREATE TABLE Users (
            id INT PRIMARY KEY,
			intval INT,
            int8val TINYINT,
            int16val SMALLINT,
            int32val INT,
			int64val BIGINT,
			uintval INT UNSIGNED,
			uint8val TINYINT UNSIGNED,
			uint16val SMALLINT UNSIGNED,
			uint32val INT UNSIGNED,
			uint64val BIGINT UNSIGNED
        )`)
		assert.NoError(t, err)

		_, rowsAffected, err := DB.Execute(`
            INSERT INTO Users(id,intval, int8val,int16val,int32val,int64val,uintval,uint8val,uint16val,uint32val,uint64val) 
            VALUES (?,?,?,?,?,?,?,?,?,?,?)`,
			1, 2147483647, 127, 32767, 2147483647, 9223372036854775807,
			4294967295, 255, 65535, 4294967295, "18446744073709551615")
		assert.NoError(t, err)
		assert.Greater(t, rowsAffected, int64(0))

		resp, err := QuerySingleStruct[IntTypes]("SELECT * FROM Users WHERE id=?", 1)
		assert.NoError(t, err)
		assert.Equal(t, int8(127), *resp.Int8Val)
		assert.Equal(t, int16(32767), *resp.Int16Val)
		assert.Equal(t, int32(2147483647), *resp.Int32Val)
		assert.Equal(t, int64(9223372036854775807), *resp.Int64Val)
		assert.Equal(t, uint(4294967295), *resp.UintVal)
		assert.Equal(t, uint8(255), *resp.Uint8Val)
		assert.Equal(t, uint16(65535), *resp.Uint16Val)
		assert.Equal(t, uint32(4294967295), *resp.Uint32Val)
		assert.Equal(t, uint64(18446744073709551615), *resp.Uint64Val)

		_, rowsAffected, err = DB.Execute(`
            INSERT INTO Users(id,intval, int8val,int16val,int32val,int64val,uintval,uint8val,uint16val,uint32val,uint64val) 
            VALUES (?,?,?,?,?,?,?,?,?,?,?)`,
			2, nil, nil, nil, nil, nil,
			nil, nil, nil, nil, nil)
		assert.NoError(t, err)
		assert.Greater(t, rowsAffected, int64(0))

		resp, err = QuerySingleStruct[IntTypes]("SELECT * FROM Users WHERE id=?", 2)
		assert.NoError(t, err)
		assert.Equal(t, 2, *resp.Id)
		assert.Equal(t, (*int8)(nil), resp.Int8Val)
		assert.Equal(t, (*int16)(nil), resp.Int16Val)
		assert.Equal(t, (*int32)(nil), resp.Int32Val)
		assert.Equal(t, (*int64)(nil), resp.Int64Val)
		assert.Equal(t, (*uint)(nil), resp.UintVal)
		assert.Equal(t, (*uint8)(nil), resp.Uint8Val)
		assert.Equal(t, (*uint16)(nil), resp.Uint16Val)
		assert.Equal(t, (*uint32)(nil), resp.Uint32Val)
		assert.Equal(t, (*uint64)(nil), resp.Uint64Val)

		tearDownIntegrationSaveTable(t)
	})

	// Test floats
	t.Run("Float Types", func(t *testing.T) {
		_, err := DB.dbConnection.Exec(`
        CREATE TABLE Users (
            id INT PRIMARY KEY,
			float32val FLOAT,
			float64val DOUBLE
        )`)
		assert.NoError(t, err)
		type FloatTypes struct {
			Id         int      `db:"column=id primarykey=yes table=Users"`
			Float32Val *float32 `db:"column=float32val"`
			Float64Val *float64 `db:"column=float64val"`
		}

		_, rowsAffected, err := DB.Execute(`
            INSERT INTO Users(id,float32val,float64val) 
            VALUES (?,?,?)`,
			1, 3.14159, 2.7182818284590452)
		assert.NoError(t, err)
		assert.Greater(t, rowsAffected, int64(0))

		resp, err := QuerySingleStruct[FloatTypes]("SELECT * FROM Users WHERE id=?", 1)
		assert.NoError(t, err)
		assert.InDelta(t, float32(3.14159), *resp.Float32Val, 0.0001)
		assert.InDelta(t, 2.7182818284590452, *resp.Float64Val, 0.0000000000000001)

		_, rowsAffected, err = DB.Execute(`
            INSERT INTO Users(id,float32val,float64val) 
            VALUES (?,?,?)`,
			2, nil, nil)
		assert.NoError(t, err)
		assert.Greater(t, rowsAffected, int64(0))

		resp, err = QuerySingleStruct[FloatTypes]("SELECT * FROM Users WHERE id=?", 2)
		assert.NoError(t, err)
		assert.Equal(t, 2, resp.Id)
		assert.Equal(t, (*float32)(nil), resp.Float32Val)
		assert.Equal(t, (*float64)(nil), resp.Float64Val)

		tearDownIntegrationSaveTable(t)
	})

	// Test boolean
	t.Run("Boolean Type", func(t *testing.T) {
		_, err := DB.dbConnection.Exec(`
        CREATE TABLE Users (
            id INT PRIMARY KEY,
			boolval BOOLEAN
        )`)
		assert.NoError(t, err)
		type BoolType struct {
			Id      int   `db:"column=id primarykey=yes table=Users"`
			BoolVal *bool `db:"column=boolval"`
		}

		_, rowsAffected, err := DB.Execute(`
            INSERT INTO Users(id,boolval) VALUES (?,?)`,
			1, true)
		assert.NoError(t, err)
		assert.Greater(t, rowsAffected, int64(0))

		resp, err := QuerySingleStruct[BoolType]("SELECT * FROM Users WHERE id=?", 1)
		assert.NoError(t, err)
		assert.True(t, *resp.BoolVal)

		_, rowsAffected, err = DB.Execute(`
            INSERT INTO Users(id,boolval) VALUES (?,?)`,
			2, nil)
		assert.NoError(t, err)
		assert.Greater(t, rowsAffected, int64(0))

		resp, err = QuerySingleStruct[BoolType]("SELECT * FROM Users WHERE id=?", 2)
		assert.NoError(t, err)
		assert.Equal(t, 2, resp.Id)
		assert.Equal(t, (*bool)(nil), resp.BoolVal)

		tearDownIntegrationSaveTable(t)
	})

	// Test boolean
	t.Run("String Type", func(t *testing.T) {
		_, err := DB.dbConnection.Exec(`
        CREATE TABLE Users (
            id INT PRIMARY KEY,
			stringval VARCHAR(255)
        )`)
		assert.NoError(t, err)
		type IntegrationGenericStruct struct {
			Id        int     `db:"column=id primarykey=yes table=Users"`
			StringVal *string `db:"column=stringval"`
		}
		_, rowsAffected, err := DB.Execute("INSERT INTO Users(id,stringval) VALUES (?,?)", 1, "Test")
		assert.NoError(t, err)
		assert.Greater(t, rowsAffected, int64(0))
		resp, err := QuerySingleStruct[IntegrationGenericStruct]("SELECT * FROM Users where id=?", 1)
		assert.NoError(t, err)
		assert.Equal(t, "Test", *resp.StringVal)

		_, rowsAffected, err = DB.Execute("INSERT INTO Users(id,stringval) VALUES (?,?)", 2, nil)
		assert.NoError(t, err)
		assert.Greater(t, rowsAffected, int64(0))
		resp, err = QuerySingleStruct[IntegrationGenericStruct]("SELECT * FROM Users where id=?", 2)
		assert.NoError(t, err)
		assert.Equal(t, 2, resp.Id)
		assert.Equal(t, (*string)(nil), resp.StringVal)

		tearDownIntegrationSaveTable(t)
	})

	// Test time.Time
	t.Run("Time Type", func(t *testing.T) {
		_, err := DB.dbConnection.Exec(`
        CREATE TABLE Users (
            id INT PRIMARY KEY,
			timeval DATETIME
        )`)
		assert.NoError(t, err)
		type TimeType struct {
			Id      int        `db:"column=id primarykey=yes table=Users"`
			TimeVal *time.Time `db:"column=timeval"`
		}

		testTime := time.Date(2024, 1, 8, 12, 30, 45, 0, time.UTC)
		_, rowsAffected, err := DB.Execute(`
            INSERT INTO Users(id,timeval) VALUES (?,?)`,
			1, testTime)
		assert.NoError(t, err)
		assert.Greater(t, rowsAffected, int64(0))

		resp, err := QuerySingleStruct[TimeType]("SELECT * FROM Users WHERE id=?", 1)
		assert.NoError(t, err)
		assert.Equal(t, testTime.UTC(), (*resp.TimeVal).UTC())

		_, rowsAffected, err = DB.Execute(`
            INSERT INTO Users(id,timeval) VALUES (?,?)`,
			2, nil)
		assert.NoError(t, err)
		assert.Greater(t, rowsAffected, int64(0))

		resp, err = QuerySingleStruct[TimeType]("SELECT * FROM Users WHERE id=?", 2)
		assert.NoError(t, err)
		assert.Equal(t, 2, resp.Id)
		assert.Equal(t, (*time.Time)(nil), resp.TimeVal)

		tearDownIntegrationSaveTable(t)
	})

	tearDownIntegrationSaveTestConnection(t, fname)
}
