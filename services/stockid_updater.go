package services

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/rs/zerolog/log"
	"main.go/constants"
	"main.go/persistance/mssql"
)

func (amx *AMXConfig) UpdateStockID() {

	log.Info().Msg("Updating Stock ID Details...")

	var db *sql.DB
	var err error

	db, err = amx.MSSQLEntities.GetDBConnection()
	if err != nil {
		amx.Log.IsDBFailed = true
		amx.Log.FailureMessage = err.Error()
		amx.Log.Details = "MSSQL - Failed to create connection"
		amx.LogStatus()
	}

	if !amx.MSSQLEntities.MssqlConnCheck(db) {
		amx.Log.IsDBFailed = true
		amx.Log.FailureMessage = "MSSQL Reconnect attepmts has been failed"
		amx.Log.Details = "MSSQL - Connection Inactive"
		amx.LogStatus()
	}

	defer mssql.CloseDBConnection(db)

	url := amx.UrlConfig.GetString(amx.AppConfig.GetString(constants.Env) + "." + constants.StockMasterUrl)
	client := http.Client{}
	req, _ := http.NewRequest("GET", url, nil)
	response, httpErr := client.Do(req)
	if httpErr != nil {

		log.Error().Stringer("Requesting Url : ", req.URL).Err(httpErr).Msg("Mojo API Has Been Failed")
		amx.Log.IsAPIFailed = true
		amx.Log.FailureMessage = httpErr.Error()
		amx.Log.Details = "Mojo api has been failed"
		amx.Log.Url = url
		amx.LogStatus()

	}

	res, _ := io.ReadAll(response.Body)
	log.Info().Stringer("Requesting Url", req.URL).RawJSON("Response", res).Msg("")
	var apiRes map[string]interface{}
	json.Unmarshal(res, &apiRes)

	if apiRes["message"] != "Success" {

		amx.Log.IsAPIFailed = true
		amx.Log.FailureMessage = apiRes["message"].(string)
		amx.Log.Details = "Mojo api has been failed"
		amx.Log.Url = url
		amx.LogStatus()
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

		sQuery := amx.DBConfig.GetString(constants.StockIDQuery)
		tsql := fmt.Sprintf(
			sQuery,
			sid,
			isin)

		ctx := context.Background()
		_, qErr := db.ExecContext(ctx, tsql)

		if qErr != nil {
			log.Error().Str("Query", tsql).Err(qErr).Msg("Error In Stock Id Updation")
		}
	}

	log.Info().Msg("Stock ID Updated")
}
