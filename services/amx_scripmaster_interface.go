package services

type AMXScripmaster interface {
	Init()
	Login() string
	Build(accToken string)
	BackUp_AMXScripMaster() bool
	Delete_Records(sQuery, segment string)
	Build_MarketCap()
	UpdateStockID()
}
