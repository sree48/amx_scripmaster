package services

import (
	"database/sql"

	log "github.com/sirupsen/logrus"
	"main.go/constants"
	"main.go/entities"
	"main.go/persistance/mssql"
	configs "main.go/utils/config"
)

func Build_MarketCap() {

	log.Info("Updating Market Cap Details...")

	var MsSqlConn *entities.MssqlConnection
	var db *sql.DB
	var err error

	MsSqlConn = mssql.InitMssql()
	db, err = mssql.GetDBConnection(MsSqlConn)
	if err != nil {
		log.Errorln("Error in mssql connection creation")
		return
	}

	if !mssql.MssqlConnCheck(db, MsSqlConn) {
		log.Errorln("MSSQL Connection  Failed")
		return
	}

	log.Infof("Connected to : %s !\n", MsSqlConn.Server)

	defer mssql.CloseDBConnection(db)
	sQuery := configs.Get(constants.DatabaseConfig).GetString(constants.MarketCapQuery)
	_, qErr := db.Query(sQuery)
	if qErr != nil {
		log.Error("error in Updating Market Cap Details : ", sQuery, qErr)
		return
	}

	log.Info("Market Cap Details Updated")
}
