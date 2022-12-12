package services

import (
	"database/sql"

	log "github.com/sirupsen/logrus"
	"main.go/constants"
	"main.go/persistance/mssql"
)

func (amx *AMXConfig) Build_MarketCap() {

	log.Info("Updating Market Cap Details...")

	var db *sql.DB
	var err error

	db, err = amx.MSSQLEntities.GetDBConnection()
	if err != nil {
		log.Errorln("Error in mssql connection creation")
		return
	}

	if !amx.MSSQLEntities.MssqlConnCheck(db) {
		log.Errorln("MSSQL Connection  Failed")
		return
	}

	defer mssql.CloseDBConnection(db)
	sQuery := amx.DBConfig.GetString(constants.MarketCapQuery)
	_, qErr := db.Query(sQuery)
	if qErr != nil {
		log.Error("error in Updating Market Cap Details : ", sQuery, qErr)
		return
	}

	log.Info("Market Cap Details Updated")
}
