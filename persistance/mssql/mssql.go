package mssql

import (
	log "github.com/sirupsen/logrus"
	"main.go/constants"
	"main.go/entities"
	configs "main.go/utils/config"

	"database/sql"

	"context"
	"fmt"
	"strconv"
)

type mssqlConn *entities.MssqlConnection

func InitMssql() *entities.MssqlConnection {

	appConfig := configs.Get(constants.ApplicationConfig)
	port, _ := strconv.Atoi(appConfig.GetString(constants.Port))

	return &entities.MssqlConnection{
		Server:   appConfig.GetString(constants.Server),
		Database: appConfig.GetString(constants.Database),
		Port:     port,
		User:     appConfig.GetString(constants.User),
		Password: appConfig.GetString(constants.Password),
	}
}

var db *sql.DB

func BuildSqlConnection(connection *entities.MssqlConnection) error {

	connString := fmt.Sprintf("server=%s;user id=%s;password=%s;port=%d;database=%s;",
		connection.Server, connection.User, connection.Password, connection.Port, connection.Database)
	var connErr error

	db, connErr = sql.Open(constants.SQL, connString)
	if connErr != nil {
		log.Error("Error creating connection pool: ", connErr)
	}
	return connErr
}

func GetDBConnection(connection *entities.MssqlConnection) (*sql.DB, error) {

	BuildSqlConnection(connection)
	return db, nil
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

func Reconnect(connection *entities.MssqlConnection) (*sql.DB, bool) {

	appConfig := configs.Get(constants.ApplicationConfig)
	val, _ := strconv.Atoi(appConfig.GetString(constants.Retry))
	attempt, n := val, val

	for attempt > 0 {

		log.Warnf("Reconnect.. : Attempt - %d of %d \n", n-(attempt-1), n)
		db, _ := GetDBConnection(connection)

		if IsConnected(db) {
			return db, true
		}
		attempt--
	}

	return nil, false
}

func MssqlConnCheck(db *sql.DB, MsSqlConn *entities.MssqlConnection) bool {
	var flag bool
	if !IsConnected(db) {
		db, flag = Reconnect(MsSqlConn)
		if !flag {

			log.Errorf("Unable to connect database %s \n", MsSqlConn.Server)
			return false
		}
	}
	return true
}

func CloseDBConnection(dbConn *sql.DB) error {
	return dbConn.Close()
}

func SetServer(connection mssqlConn, server string) {
	connection.Server = server
}

func SetDatabase(connection mssqlConn, database string) {
	connection.Database = database
}
