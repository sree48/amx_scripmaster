package services

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/spf13/viper"
	"main.go/constants"
	helper "main.go/helper"
	"main.go/persistance/mssql"
)

type Logger struct {
	IsDBFailed     bool
	IsAPIFailed    bool
	FailureMessage string
	Details        string
	Url            string
}

type AMXConfig struct {
	AppConfig, UrlConfig, DBConfig                          *viper.Viper
	MSSQLEntities                                           mssql.MSSQL
	ISBackupDone                                            bool
	vSegments, vNse_Series, vBse_Series, vIndex_Instruments []string
	Log                                                     Logger
}

var wg sync.WaitGroup

func (amx *AMXConfig) Init() {

	segments := amx.AppConfig.GetString(constants.SegmentsAllowed)
	nse_series := amx.AppConfig.GetString(constants.NseSeries)
	bse_series := amx.AppConfig.GetString(constants.BseSeries)
	index_instruments := amx.AppConfig.GetString(constants.IndexInstruments)
	amx.vSegments = strings.Split(segments, ",")
	amx.vNse_Series = strings.Split(nse_series, ",")
	amx.vBse_Series = strings.Split(bse_series, ",")
	amx.vIndex_Instruments = strings.Split(index_instruments, ",")
	amx.MSSQLEntities = mssql.MSSQL{Server: amx.AppConfig.GetString(constants.Server), Database: amx.AppConfig.GetString(constants.Database), Port: amx.AppConfig.GetInt(constants.Port), User: amx.AppConfig.GetString(constants.User), Password: amx.AppConfig.GetString(constants.Password)}

}

func (amx *AMXConfig) Login() string {

	url := amx.UrlConfig.GetString(amx.AppConfig.GetString(constants.Env) + "." + constants.GetLoginUrl)
	client := http.Client{}
	body := map[string]string{}
	body["userid"] = amx.AppConfig.GetString(amx.AppConfig.GetString(constants.Env) + "." + constants.UserID)
	body["passorpin"] = amx.AppConfig.GetString(amx.AppConfig.GetString(constants.Env) + "." + constants.UserPassword)
	json_req, _ := json.Marshal(body)

	req, _ := http.NewRequest("POST", url, bytes.NewBuffer(json_req))
	req.Header.Set("X-SourceID", "2")
	req.Header.Set("X-Platform", "MSIL")
	req.Header.Set("X-DeviceID", "MSIL-MW")
	req.Header.Set("X-UserType", "1")
	req.Header.Set("X-OperatingSystem", "Linux")
	req.Header.Set("Content-Type", "application/json")

	response, httpErr := client.Do(req)
	if httpErr != nil {
		log.Error().Stringer("Requesting Url : ", req.URL).RawJSON("Request : ", json_req).Interface("Headers : ", req.Header).Err(httpErr).Msg("AMX Login Failed")
		amx.Log.IsAPIFailed = true
		amx.Log.FailureMessage = httpErr.Error()
		amx.Log.Details = "AMX login api has been failed"
		amx.Log.Url = url
		amx.LogStatus()
	}

	res, _ := io.ReadAll(response.Body)
	log.Info().Stringer("Requesting Url", req.URL).RawJSON("Request", json_req).Interface("Headers", req.Header).RawJSON("Response", res).Msg("")

	var apiRes map[string]interface{}
	json.Unmarshal(res, &apiRes)

	if strings.EqualFold(apiRes[constants.Message].(string), constants.Success) {

		data := apiRes[constants.Data].(map[string]interface{})
		return data["accesstoken"].(string)

	} else {

		amx.Log.IsAPIFailed = true
		amx.Log.FailureMessage = apiRes[constants.ErrCode].(string) + " " + apiRes[constants.Message].(string)
		amx.Log.Details = "AMX login api has been failed"
		amx.Log.Url = url
		amx.LogStatus()
	}

	return ""
}

func (amx *AMXConfig) Build(accToken string) {

	url := amx.UrlConfig.GetString(amx.AppConfig.GetString(constants.Env) + "." + constants.GetSecinfoUrl)
	segmentData := make(map[string][]interface{})

	amx.Delete_Records(amx.DBConfig.GetString(constants.DeleteEquity), "Equity")
	amx.Delete_Records(amx.DBConfig.GetString(constants.DeleteDerivative), "Derivative")

	wg.Add(len(amx.vSegments))

	for _, segments := range amx.vSegments {

		isLastPage := false
		page := "1"

		for isLastPage == false {

			finalUrl := url + "exchange=" + segments + "&page=" + page

			client := http.Client{}
			req, _ := http.NewRequest("GET", finalUrl, nil)
			req.Header.Set("Authorization", "Bearer "+accToken)

			response, httpErr := client.Do(req)
			if httpErr != nil {
				log.Error().Str("Segment", segments).Str("Page", page).Err(httpErr).Stringer("Requesting Url", req.URL).Interface("Headers", req.Header).Msg("AMX Scripmaster api failed")
				amx.Log.IsAPIFailed = true
				amx.Log.FailureMessage = httpErr.Error()
				amx.Log.Details = "AMX ScripMaster api has been failed"
				amx.Log.Url = url
				amx.LogStatus()
			}

			res, _ := io.ReadAll(response.Body)
			log.Info().Stringer("Requesting Url", req.URL).Interface("Headers", req.Header).RawJSON("Response", res).Msg("")

			var apiRes map[string]interface{}
			json.Unmarshal(res, &apiRes)
			if strings.EqualFold(apiRes[constants.Message].(string), constants.Success) {

				data := apiRes[constants.Data].(map[string]interface{})
				isLastPage = data[constants.LastPage].(bool)
				page = strconv.Itoa(int(math.Round(data[constants.NextPage].(float64))))
				vData := data[constants.Data].([]interface{})

				segmentData[segments] = append(segmentData[segments], vData)

			} else {

				amx.Log.IsAPIFailed = true
				amx.Log.FailureMessage = apiRes[constants.Message].(string)
				amx.Log.Details = "AMX ScripMaster api has been failed"
				amx.Log.Url = url
				amx.LogStatus()

			}
		}
		log.Info().Str("Segment", segments).Msg("API call completed for segment " + segments)

		if segments == "nse_cm" || segments == "bse_cm" {

			go amx.Parse_EQ(segmentData[segments], segments)
			delete(segmentData, segments)

		} else {

			go amx.Parse_Derv(segmentData[segments], segments)
			delete(segmentData, segments)
		}
	}
	wg.Wait()
}

func (amx *AMXConfig) Parse_EQ(segData []interface{}, segment string) {

	defer wg.Done()

	var count, skip_count int
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

	for outer_index := 0; outer_index < len(segData); outer_index++ {
		for inner_index := 0; inner_index < len(segData[outer_index].([]interface{})); inner_index++ {
			count++
			data := segData[outer_index].([]interface{})[inner_index].(map[string]interface{})

			if data["remarksText"].(string) == "SP" ||
				data["symbol"].(string) == "" {

				skip_count++
				log.Debug().Interface("Data", data).Str("Segment", segment).Msg("Skipped empty symbol / Invalid remarks")
				continue //Skipping
			}

			if segment == "nse_cm" && !amx.Check_Series(segment, data["series"].(string)) {

				skip_count++
				log.Debug().Interface("Data", data).Str("Segment", segment).Msg("Skipped invalid series")
				continue //Skipping
			}

			token := data["symbol"].(string)
			if segment == "bse_cm" && !strings.HasPrefix(token, "7") &&
				!strings.HasPrefix(token, "5") && (!strings.HasPrefix(token, "8") && !amx.Check_Series(segment, data["series"].(string))) {

				skip_count++
				log.Debug().Interface("Data", data).Str("Segment", segment).Msg("Skipped invalid series / token")
				continue //Skipping
			}

			divider, precision := helper.GetDividerAndPrecision(data["marketSegmentId"].(string))
			assetClass := "cash"
			expDate := "01 Jan 1980"

			var details = "-"
			if data["securityDesc"].(string) != "" {
				details = data["securityDesc"].(string)
			}

			sQuery := amx.DBConfig.GetString(constants.EQInsertQuery)
			tsql := fmt.Sprintf(
				sQuery,
				data["symbol"].(string)+"_"+helper.GetSegmentId(data["marketSegmentId"].(string)),
				data["symbol"].(string),
				data["symbolName"].(string),
				data["series"].(string),
				data["instrumentType"].(string),
				divider, precision, assetClass,
				helper.GetMaturityDate(data["issueMaturityDate"].(string)),
				details,
				helper.SetPrecision(strconv.Itoa(int(data["priceTick"].(float64))), divider, precision),
				strconv.Itoa(int(data["minimumLot"].(float64))),
				strconv.Itoa(int(data["lowPriceRange"].(float64))),
				strconv.Itoa(int(data["highPriceRange"].(float64))),
				data["assetToken"].(string),
				data["instrumentType"].(string),
				data["expiryDate"].(string),
				expDate,
				strconv.Itoa(int(data["strikePrice"].(float64))),
				data["optionType"].(string),
				helper.GetSegmentId(data["marketSegmentId"].(string)),
				data["faceValue"].(string),
				data["isinCode"].(string),
				strconv.Itoa(int(data["priceQuotUnit"].(float64))),
				strconv.Itoa(int(data["maxSingleTransQty"].(float64))),
				strconv.Itoa(int(data["maxSingleTransValue"].(float64))),
				data["qtyUnits"].(string),
				"1",
				"1",
				data["marketType"].(string),
				strconv.Itoa(int(data["openInterest"].(float64))),
				strconv.Itoa(int(data["totalValueTraded"].(float64))),
				details,
				helper.GetFreezepercentage(strconv.Itoa(int(data["freezePercent"].(float64))), divider, precision),
				data["deliveryUnit"].(string),
				strconv.Itoa(int(data["basePrice"].(float64))),
				strconv.Itoa(int(data["issueCapital"].(float64))),
				strconv.Itoa(int(data["regularLot"].(float64))),
				data["priceQuotFactor"].(string),
				data["issueStartDate"].(string),
				data["trdSymbol"].(string))

			ctx := context.Background()
			_, qErr := db.ExecContext(ctx, tsql)

			if qErr != nil {
				log.Error().Stack().Str("Query", tsql).Err(qErr).Msg("Error in updating AMX ScripMaster")
				amx.Log.IsDBFailed = true
				amx.Log.FailureMessage = qErr.Error()
				amx.Log.Details = "Query execution failed"
				amx.LogStatus()
			}
		}
	}

	log.Info().Str("Segment", segment).Int("Processed Count", count).Int("Skipped Count", skip_count).Msg(segment + " has been processed")
}

func (amx *AMXConfig) Parse_Derv(segData []interface{}, segment string) {

	defer wg.Done()

	var count, skip_count int
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

	for outer_index := 0; outer_index < len(segData); outer_index++ {
		for inner_index := 0; inner_index < len(segData[outer_index].([]interface{})); inner_index++ {
			count++
			data := segData[outer_index].([]interface{})[inner_index].(map[string]interface{})

			var assetClass, details, strikePrice, optionType string

			instName := data["instrumentType"].(string)
			expDate := data["expiryDate"].(string)
			divider, precision := helper.GetDividerAndPrecision(data["marketSegmentId"].(string))
			strikePrice = strconv.Itoa(int(data["strikePrice"].(float64)))
			optionType = data["optionType"].(string)
			assetClass = "derivative"
			var priceNum string
			priceNum = "1"
			if segment == "mcx_fo" || segment == "ncx_fo" {

				if int(data["genDen"].(float64)) == 0 || int(data["priceDen"].(float64)) == 0 {
					priceNum = "1"
				} else {
					priceNum = strconv.Itoa(int(data["genNum"].(float64)) / int(data["genDen"].(float64)) * int(data["priceNum"].(float64)) / int(data["priceDen"].(float64)))
				}
			}

			if strings.HasPrefix(instName, "FUT") || strings.HasPrefix(instName, "OPT") {
				if expDate == "" {

					skip_count++
					log.Debug().Interface("Data", data).Str("Segment", segment).Msg("Skipped Empty Expiry")
					continue //Skipping
				}

				if Expiry_Validate(expDate) {

					skip_count++
					log.Debug().Interface("Data", data).Str("Segment", segment).Msg("Skipped Expired Contract")
					continue //Skipping
				}

				expDate = helper.GetTimeInFormat(expDate, constants.ExpFormat)

				details += expDate
				if strings.HasPrefix(instName, "OPT") {
					details += " " + optionType + " " + helper.FormatStrikePrice(strikePrice, divider, precision)
				}

			} else if amx.Check_Index(instName) {

				details += data["securityDesc"].(string)

			} else {

				skip_count++
				log.Debug().Interface("Data", data).Str("Segment", segment).Msg("Skipped Invalid Derivative Contract")
				continue //Skipping
			}

			sQuery := amx.DBConfig.GetString(constants.DERInsertQuery)
			tsql := fmt.Sprintf(
				sQuery,
				data["symbol"].(string)+"_"+helper.GetSegmentId(data["marketSegmentId"].(string)),
				data["symbol"].(string),
				data["symbolName"].(string),
				data["series"].(string),
				instName,
				strconv.Itoa(int(data["normalMarketAllowed"].(float64))),
				divider, precision, assetClass,
				helper.GetMaturityDate(data["issueMaturityDate"].(string)),
				data["securityDesc"].(string),
				helper.SetPrecision(strconv.Itoa(int(data["priceTick"].(float64))), divider, precision),
				strconv.Itoa(int(data["minimumLot"].(float64))),
				strconv.Itoa(int(data["lowPriceRange"].(float64))),
				strconv.Itoa(int(data["highPriceRange"].(float64))),
				data["assetToken"].(string),
				instName,
				data["expiryDate"].(string),
				expDate, strikePrice, optionType,
				helper.GetSegmentId(data["marketSegmentId"].(string)),
				data["faceValue"].(string),
				data["isinCode"].(string),
				strconv.Itoa(int(data["priceQuotUnit"].(float64))),
				strconv.Itoa(int(data["maxSingleTransQty"].(float64))),
				strconv.Itoa(int(data["maxSingleTransValue"].(float64))),
				data["qtyUnits"].(string),
				priceNum,
				"1",
				data["marketType"].(string),
				strconv.Itoa(int(data["openInterest"].(float64))),
				strconv.Itoa(int(data["totalValueTraded"].(float64))),
				details,
				helper.GetFreezepercentage(strconv.Itoa(int(data["freezePercent"].(float64))), divider, precision),
				data["deliveryUnit"].(string),
				strconv.Itoa(int(data["basePrice"].(float64))),
				strconv.Itoa(int(data["issueCapital"].(float64))),
				strconv.Itoa(int(data["regularLot"].(float64))),
				data["priceQuotFactor"].(string),
				data["issueStartDate"].(string),
				data["trdSymbol"].(string))

			ctx := context.Background()
			_, qErr := db.ExecContext(ctx, tsql)

			if qErr != nil {
				log.Error().Stack().Str("Query", tsql).Err(qErr).Msg("Error in updating AMX ScripMaster")
				amx.Log.IsDBFailed = true
				amx.Log.FailureMessage = qErr.Error()
				amx.Log.Details = "Query execution failed"
				amx.LogStatus()
			}
		}
	}

	log.Info().Str("Segment", segment).Int("Processed Count", count).Int("Skipped Count", skip_count).Msg(segment + " has been processed")
}

func (amx *AMXConfig) BackUp_AMXScripMaster() {

	log.Info().Msg("Backing Up Data")
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

	log.Info().Str("Server", amx.MSSQLEntities.Server).Msg("Connected")

	defer mssql.CloseDBConnection(db)

	sQuery := amx.DBConfig.GetString(constants.BackUpProcedure)
	ctx := context.Background()
	_, qErr := db.ExecContext(ctx, sQuery)

	if qErr != nil {
		log.Error().Str("Query", sQuery).Err(qErr).Msg("Error in backup AMXScripmaster")
		amx.Log.IsDBFailed = true
		amx.Log.FailureMessage = qErr.Error()
		amx.Log.Details = "Query execution failed"
		amx.LogStatus()
	}

	log.Info().Msg("Back Up Completed...")

	amx.ISBackupDone = true
}

func (amx *AMXConfig) Delete_Records(sQuery, segment string) {

	if !amx.ISBackupDone {
		return
	}

	log.Info().Str("Segment", segment).Msg("Started deleting the records for " + segment)

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

	ctx := context.Background()
	_, qErr := db.ExecContext(ctx, sQuery)

	if qErr != nil {
		amx.Log.IsDBFailed = true
		amx.Log.FailureMessage = qErr.Error()
		amx.Log.Details = "Query execution failed"
		amx.LogStatus()
	}

	log.Info().Str("Segment", segment).Msg(segment + " records cleaned...")
}

func (amx *AMXConfig) Check_Series(segment string, series string) bool {

	switch {
	case segment == "nse_cm":
		for _, s := range amx.vNse_Series {
			if s == series {
				return true
			}
		}
		return false
	case segment == "bse_cm":
		for _, s := range amx.vBse_Series {
			if s == series {
				return true
			}
		}
		return false
	default:
		return false
	}
}

func (amx *AMXConfig) Check_Index(instName string) bool {

	for _, s := range amx.vIndex_Instruments {
		if s == instName {
			return true
		}
	}

	return false
}

func Expiry_Validate(expDate string) bool {

	if expDate != "" {

		iTime, _ := strconv.ParseInt(expDate, 10, 64)
		now := time.Now()

		if iTime == now.Unix() {
			return false
		}

		if iTime < now.Unix() {
			return true
		}
	}
	return false
}

func (amx *AMXConfig) LogStatus() {

	if amx.Log.IsAPIFailed == true {

		log.Error().Stack().Str("Details", amx.Log.Details).Str("Contact", "API Team").Str("Url", amx.Log.Url).Msg(amx.Log.FailureMessage)
		os.Exit(1)

	} else if amx.Log.IsDBFailed == true {

		log.Error().Stack().Str("Details", amx.Log.Details).Str("Contact", "MSIL Team").Msg(amx.Log.FailureMessage)
		os.Exit(1)

	} else {

		log.Info().Msg("Records updated into amx scripmaster successfully")

	}
}
