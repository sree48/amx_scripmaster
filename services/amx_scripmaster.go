package services

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"main.go/constants"
	"main.go/entities"
	helper "main.go/helper"
	"main.go/persistance/mssql"
	configs "main.go/utils/config"
)

var appConfig, urlConfig, dbConfig *viper.Viper
var vSegments, vNse_Series, vBse_Series, vIndex_Instruments []string
var isBackupDone bool

func Init() {

	appConfig = configs.Get(constants.ApplicationConfig)
	urlConfig = configs.Get(constants.APIConfig)
	dbConfig = configs.Get(constants.DatabaseConfig)
	segments := appConfig.GetString(constants.SegmentsAllowed)
	nse_series := appConfig.GetString(constants.NseSeries)
	bse_series := appConfig.GetString(constants.BseSeries)
	index_instruments := appConfig.GetString(constants.IndexInstruments)
	vSegments = strings.Split(segments, ",")
	vNse_Series = strings.Split(nse_series, ",")
	vBse_Series = strings.Split(bse_series, ",")
	vIndex_Instruments = strings.Split(index_instruments, ",")
	isBackupDone = BackUp_AMXScripMaster()
}

func Login() string {

	url := urlConfig.GetString(appConfig.GetString(constants.Env) + "." + constants.GetLoginUrl)
	client := http.Client{}
	body := map[string]string{}
	body["userid"] = appConfig.GetString(appConfig.GetString(constants.Env) + "." + constants.UserID)
	body["passorpin"] = appConfig.GetString(appConfig.GetString(constants.Env) + "." + constants.UserPassword)
	json_req, _ := json.Marshal(body)

	req, _ := http.NewRequest("POST", url, bytes.NewBuffer(json_req))
	req.Header.Set("X-SourceID:", "2")
	req.Header.Set("X-Platform:", "MSIL")
	req.Header.Set("X-DeviceID:", "MSIL-MW")
	req.Header.Set("X-UserType:", "1")
	req.Header.Set("X-OperatingSystem:", "Linux")
	req.Header.Set("Content-Type:", "application/json")

	response, httpErr := client.Do(req)
	if httpErr != nil {
		log.Fatal("HTTP Error Occurred on login : ", httpErr)
	}

	res, _ := io.ReadAll(response.Body)
	var apiRes map[string]interface{}
	json.Unmarshal(res, &apiRes)

	if strings.EqualFold(apiRes[constants.Message].(string), constants.Success) {

		data := apiRes[constants.Data].(map[string]interface{})
		return data["accesstoken"].(string)
	}

	log.Fatal("Login Failed : ", apiRes[constants.ErrCode].(string), apiRes[constants.Message].(string))
	return ""
}

func Build(accToken string) {

	url := urlConfig.GetString(appConfig.GetString(constants.Env) + "." + constants.GetSecinfoUrl)
	segmentData := make(map[string][]interface{})

	for _, segments := range vSegments {

		isLastPage := false
		page := "1"

		for isLastPage == false {
			finalUrl := url + "exchange=" + segments + "&page=" + page

			client := http.Client{}
			req, _ := http.NewRequest("GET", finalUrl, nil)
			req.Header.Set("Authorization", "Bearer "+accToken)

			response, httpErr := client.Do(req)

			if httpErr != nil {
				log.Error("HTTP Error Occurred on segment : ", segments, httpErr)
				return
			}

			res, _ := io.ReadAll(response.Body)
			var apiRes map[string]interface{}
			json.Unmarshal(res, &apiRes)
			if strings.EqualFold(apiRes[constants.Message].(string), constants.Success) {

				data := apiRes[constants.Data].(map[string]interface{})
				isLastPage = data[constants.LastPage].(bool)
				page = strconv.Itoa(int(math.Round(data[constants.NextPage].(float64))))
				vData := data[constants.Data].([]interface{})

				segmentData[segments] = append(segmentData[segments], vData)

			} else {

				log.Error("GetSecInfo API Failed : ", apiRes[constants.Message])
				return
			}
		}
		log.Info("API call completed for segment : ", segments)
		if segments == "nse_cm" || segments == "bse_cm" {
			go Parse_EQ(segmentData[segments], segments)
			delete(segmentData, segments)
		} else {
			go Parse_Derv(segmentData[segments], segments)
			delete(segmentData, segments)
		}
	}

	time.Sleep(5 * time.Second)
}

func Parse_EQ(segData []interface{}, segment string) {

	if len(segData) > 0 {
		Delete_Records(dbConfig.GetString(constants.DeleteEquity), "Equity")
	}

	var count, skip_count int
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
		log.Errorln("MSSQL Connection Check Failed")
		return
	}
	log.Infof("Connected to : %s !\n", MsSqlConn.Server)

	defer mssql.CloseDBConnection(db)

	for outer_index := 0; outer_index < len(segData); outer_index++ {
		for inner_index := 0; inner_index < len(segData[outer_index].([]interface{})); inner_index++ {
			count++
			data := segData[outer_index].([]interface{})[inner_index].(map[string]interface{})

			// Skipping
			if data["remarksText"].(string) == "SP" ||
				data["symbol"].(string) == "" ||
				!Check_Series(segment, data["series"].(string)) {

				skip_count++
				continue
			}

			divider, precision := helper.GetDividerAndPrecision(data["marketSegmentId"].(string))
			assetClass := "cash"
			expDate := "01 Jan 1980"

			sQuery := dbConfig.GetString(constants.EQInsertQuery)
			tsql := fmt.Sprintf(
				sQuery,
				data["symbol"].(string)+"_"+helper.GetSegmentId(data["marketSegmentId"].(string)),
				data["symbol"].(string),
				data["symbolName"].(string),
				data["series"].(string),
				data["instrumentType"].(string),
				strconv.Itoa(int(data["normalMarketAllowed"].(float64))),
				divider, precision, assetClass,
				helper.GetMaturityDate(data["issueMaturityDate"].(string)),
				data["securityDesc"].(string),
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
				data["securityDesc"].(string),
				helper.GetFreezepercentage(strconv.Itoa(int(data["freezePercent"].(float64))), divider, precision),
				data["deliveryUnit"].(string),
				strconv.Itoa(int(data["basePrice"].(float64))),
				strconv.Itoa(int(data["issueCapital"].(float64))),
				strconv.Itoa(int(data["regularLot"].(float64))),
				data["priceQuotFactor"].(string),
				data["issueStartDate"].(string),
				data["trdSymbol"].(string))

			_, qErr := db.Query(tsql)
			if qErr != nil {
				log.Error("error in updating AMXScripmaster : ", tsql, qErr)
			}
		}
	}
	log.Info("Processed - Segment : ", segment, " - Count : ", count, " - Skipped : ", skip_count)
}

func Parse_Derv(segData []interface{}, segment string) {

	if len(segData) > 0 {
		Delete_Records(dbConfig.GetString(constants.DeleteDerivative), "Derivative")
	}

	var count, skip_count int
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
		log.Errorln("MSSQL Connection Check Failed")
		return
	}
	log.Infof("Connected to : %s !\n", MsSqlConn.Server)

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
				priceNum = strconv.Itoa(int(data["genNum"].(float64)) / int(data["genDen"].(float64)) * int(data["priceNum"].(float64)) / int(data["priceDen"].(float64)))
			}

			if strings.HasPrefix(instName, "FUT") || strings.HasPrefix(instName, "OPT") {
				if expDate == "" {

					skip_count++
					continue //Skipping
				}

				if Expiry_Validate(expDate) {

					skip_count++
					continue //Skipping
				}

				expDate = helper.GetTimeInFormat(expDate, constants.ExpFormat)

				details += expDate
				if strings.HasPrefix(instName, "OPT") {
					details += " " + optionType + " " + helper.FormatStrikePrice(strikePrice, divider, precision)
				}

			} else if Check_Index(instName) {

				details += data["securityDesc"].(string)

			} else {

				skip_count++
				continue
			}

			sQuery := dbConfig.GetString(constants.DERInsertQuery)
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

			_, qErr := db.Query(tsql)
			if qErr != nil {
				log.Error("error in updating AMXScripmaster : ", tsql, qErr)
			}
		}
	}
	log.Info("Processed - Segment : ", segment, " - Count : ", count, " - Skipped : ", skip_count)
}

func BackUp_AMXScripMaster() bool {

	log.Info("Backing Up...")

	var MsSqlConn *entities.MssqlConnection
	var db *sql.DB
	var err error

	MsSqlConn = mssql.InitMssql()
	db, err = mssql.GetDBConnection(MsSqlConn)
	if err != nil {
		log.Errorln("Error in mssql connection creation")
		return false
	}

	if !mssql.MssqlConnCheck(db, MsSqlConn) {
		log.Errorln("MSSQL Connection  Failed")
		return false
	}

	log.Infof("Connected to : %s !\n", MsSqlConn.Server)

	defer mssql.CloseDBConnection(db)

	sQuery := dbConfig.GetString(constants.BackUpProcedure)
	_, qErr := db.Query(sQuery)
	if qErr != nil {
		log.Error("error in backup AMXScripmaster : ", sQuery, qErr)
		return false
	}

	log.Info("Back Up Completed...")

	return true
}

func Delete_Records(sQuery, segment string) {

	if !isBackupDone {
		return
	}

	log.Info("Deleting " + segment + " ...")

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

	_, qErr := db.Query(sQuery)
	if qErr != nil {
		log.Error("error in backup AMXScripmaster : ", sQuery, qErr)
		return
	}

	log.Info(segment + "Cleaned...")
}

func Check_Series(segment string, series string) bool {

	switch {
	case segment == "nse_cm":
		for _, s := range vNse_Series {
			if s == series {
				return true
			}
		}
		return false
	case segment == "bse_cm":
		for _, s := range vBse_Series {
			if s == series {
				return true
			}
		}
		return false
	default:
		return false
	}
}

func Check_Index(instName string) bool {

	for _, s := range vIndex_Instruments {
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
