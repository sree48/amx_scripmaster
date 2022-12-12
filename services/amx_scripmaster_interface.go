package services

type AMXScripmaster interface {
	Init()
	Login() string
	Build(accToken string)
	BackUp_AMXScripMaster()
	Delete_Records(sQuery, segment string)
	Build_MarketCap()
	UpdateStockID()
	Parse_EQ(segData []interface{}, segment string)
	Parse_Derv(segData []interface{}, segment string)
}
