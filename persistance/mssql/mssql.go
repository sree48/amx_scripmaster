package mssql

import (
	_ "github.com/denisenkom/go-mssqldb"
	log "github.com/sirupsen/logrus"
	"main.go/constants"
	configs "main.go/utils/config"

	"database/sql"

	"context"
	"fmt"
	"strconv"
)

type MSSQL struct {
	Server, Database, User, Password string
	Port                             int
}

func (mssql MSSQL) GetDBConnection() (*sql.DB, error) {

	connString := fmt.Sprintf("server=%s;user id=%s;password=%s;port=%d;database=%s;",
		mssql.Server, mssql.User, mssql.Password, mssql.Port, mssql.Database)

	db, connErr := sql.Open(constants.SQL, connString)
	if connErr != nil {
		log.Error("Error creating connection pool: ", connErr)
	}
	return db, connErr
}

func (mssql MSSQL) Reconnect() (*sql.DB, bool) {

	appConfig := configs.Get(constants.ApplicationConfig)
	val, _ := strconv.Atoi(appConfig.GetString(constants.Retry))
	attempt, n := val, val

	for attempt > 0 {

		log.Warnf("Reconnect.. : Attempt - %d of %d \n", n-(attempt-1), n)
		db, _ := mssql.GetDBConnection()

		if IsConnected(db) {
			return db, true
		}
		attempt--
	}

	return nil, false
}

func (mssql MSSQL) MssqlConnCheck(db *sql.DB) bool {
	var flag bool
	if !IsConnected(db) {
		db, flag = mssql.Reconnect()
		if !flag {

			log.Errorf("Unable to connect database %s \n", mssql.Server)
			return false
		}
	}
	return true
}

func IsConnected(dbConn *sql.DB) bool {

	c := context.Background()
	err := dbConn.PingContext(c)

	if err != nil {
		log.Error("error while creating mssql connection :", err)
		return false
	}

	return true
}

func CloseDBConnection(dbConn *sql.DB) error {
	return dbConn.Close()
}
