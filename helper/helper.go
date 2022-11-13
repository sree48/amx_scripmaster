package helper

import (
	"strconv"
	"strings"
	"time"

	"main.go/constants"
)

func GetSegmentId(segment string) string {

	switch {
	case segment == "nse_cm":
		return "1"
	case segment == "bse_cm":
		return "3"
	case segment == "nse_fo":
		return "2"
	case segment == "mcx_fo":
		return "5"
	case segment == "ncx_fo":
		return "7"
	case segment == "cde_fo":
		return "13"
	default:
		return ""
	}
}

func GetDividerAndPrecision(segmentID string) (string, string) {

	switch segmentID {
	case "11", "12":
		return "10000", "4"
	case "13", "14":
		return "10000000", "4"
	default:
		return "100", "2"
	}
}

func GetFreezepercentage(value, divider, precision string) string {

	if len(value) < 1 {
		return ""
	} else {
		if !strings.Contains(value, ".") {
			value += ".0000"
		}
		fValue, _ := strconv.ParseFloat(value, 32)
		fDivider, _ := strconv.ParseInt(divider, 10, 32)
		fPrecision, _ := strconv.ParseInt(precision, 10, 32)
		fValue = fValue / float64(fDivider)
		return strconv.FormatFloat(fValue, 'f', int(fPrecision), 64)
	}
}

func GetMaturityDate(value string) string {

	if len(value) == 0 {
		return "-"
	}

	tmp_val, _ := strconv.ParseFloat(value, 32)
	if len(value) < 1 || tmp_val == 0.0 {
		return "-"
	} else {
		fValue, _ := strconv.ParseInt(value, 10, 64)
		tm := time.Unix(fValue, 0)
		return tm.Format(constants.MatDateTimeFomat)
	}
}

func GetTimeInSeconds(date, format string) string {

	tm, _ := time.Parse(format, date)
	return strconv.Itoa(int(tm.Unix()))
}

func GetTimeInFormat(date, format string) string {

	iTime, _ := strconv.ParseInt(date, 10, 64)
	tm := time.Unix(iTime, 0)
	return tm.Format(format)
}

func FormatStrikePrice(price, divider, precision string) string {

	if !strings.Contains(price, ".") {
		price += ".0000"
	}

	fValue, _ := strconv.ParseFloat(price, 32)
	fDivider, _ := strconv.ParseInt(divider, 10, 32)
	fPrecision, _ := strconv.ParseInt(precision, 10, 32)
	fValue = fValue / float64(fDivider)
	return strconv.FormatFloat(fValue, 'f', int(fPrecision), 64)
}

func SetPrecision(value, divider, precision string) string {

	fValue, _ := strconv.ParseFloat(value, 32)
	fDivider, _ := strconv.ParseInt(divider, 10, 32)
	fPrecision, _ := strconv.ParseInt(precision, 10, 32)
	fValue = fValue / float64(fDivider)
	return strconv.FormatFloat(fValue, 'f', int(fPrecision), 64)
}
