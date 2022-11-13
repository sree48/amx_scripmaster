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
	service.Build()
}

func initconfig() {
	configs.Init(flag.BaseConfigPath())
}
