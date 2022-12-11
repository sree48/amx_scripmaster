package services

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	log "github.com/sirupsen/logrus"
	"main.go/constants"
	"main.go/persistance/mssql"
	configs "main.go/utils/config"
)

func (amx AMXConfig) UpdateStockID() {

	log.Info("Updating Stock ID Details...")

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

	url := configs.Get(constants.APIConfig).GetString(configs.Get(constants.ApplicationConfig).GetString(constants.Env) + "." + constants.StockMasterUrl)
	client := http.Client{}
	req, _ := http.NewRequest("GET", url, nil)
	response, httpErr := client.Do(req)
	if httpErr != nil {
		log.Error("HTTP Error Occurred on Stock Master: ", httpErr)
		return
	}

	res, _ := io.ReadAll(response.Body)
	var apiRes map[string]interface{}
	json.Unmarshal(res, &apiRes)

	if apiRes["message"] != "Success" {
		log.Error("Stock Master API Failed", apiRes["message"])
		return
	}

	var data []map[string]interface{}
	data = apiRes["data"].(map[string]interface{})["stock_master"].([]map[string]interface{})
	for index := 0; index < len(data); index++ {
		sid := data[index]["sid"].(string)
		if sid == "null" {
			sid = ""
		}
		isin := data[index]["isin"].(string)
		if isin == "null" {
			isin = ""
		}
		if len(isin) == 0 {
			// Skipping
			continue
		}

		sQuery := configs.Get(constants.DatabaseConfig).GetString(constants.StockIDQuery)
		tsql := fmt.Sprintf(
			sQuery,
			sid,
			isin)

		_, qErr := db.Query(tsql)
		if qErr != nil {
			log.Error("Error in stock id updation : ", tsql, qErr)
		}
	}

	log.Info("Stock ID Updated")
}
