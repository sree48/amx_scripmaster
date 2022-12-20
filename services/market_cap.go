package services

import (
	"context"
	"database/sql"

	"github.com/rs/zerolog/log"
	"main.go/constants"
	"main.go/persistance/mssql"
)

func (amx *AMXConfig) Build_MarketCap() {

	log.Info().Msg("Updating Market Cap Details...")

	var db *sql.DB
	var err error

	db, err = amx.MSSQLEntities.GetDBConnection()
	if err != nil {
		log.Error().Stack().Err(err).Msg("Error in mssql connection creation")
		return
	}

	if !amx.MSSQLEntities.MssqlConnCheck(db) {
		log.Error().Stack().Msg("MSSQL Connection  Failed")
		return
	}

	defer mssql.CloseDBConnection(db)
	sQuery := amx.DBConfig.GetString(constants.MarketCapQuery)

	ctx := context.Background()
	_, qErr := db.ExecContext(ctx, sQuery)

	if qErr != nil {
		log.Error().Str("Query", sQuery).Err(qErr).Msg("Error In Updating Market Cap Details")
		return
	}

	log.Info().Msg("Market Cap Details Updated")
}
