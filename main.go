package main

import (
	service "main.go/services"
	configs "main.go/utils/config"
	flag "main.go/utils/flags"
)

func init() {
	initconfig()
}

func main() {

	service.Init()
	accToken := service.Login()
	service.Build(accToken)
	service.Build_MarketCap()
	service.UpdateStockID()
}

func initconfig() {
	configs.Init(flag.BaseConfigPath())
}
