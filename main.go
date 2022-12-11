package main

import (
	"main.go/constants"
	"main.go/services"
	service "main.go/services"
	configs "main.go/utils/config"
	flag "main.go/utils/flags"
)

func init() {
	initconfig()
}

func main() {

	amx_config := service.AMXConfig{AppConfig: configs.Get(constants.ApplicationConfig), UrlConfig: configs.Get(constants.APIConfig), DBConfig: configs.Get(constants.DatabaseConfig)}
	service.AMXScripmaster.Init(amx_config)
	amx_config.ISBackupDone = services.AMXScripmaster.BackUp_AMXScripMaster(amx_config)
	accToken := service.AMXScripmaster.Login(amx_config)
	service.AMXScripmaster.Build(amx_config, accToken)
	service.AMXScripmaster.Build_MarketCap(amx_config)
	service.AMXScripmaster.UpdateStockID(amx_config)
}

func initconfig() {
	configs.Init(flag.BaseConfigPath())
}
