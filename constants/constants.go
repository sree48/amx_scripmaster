package constants

const (
	ApplicationConfig = "application.yaml"
	DatabaseConfig    = "database.yaml"
	APIConfig         = "config.json"
)

// default values
const (
	Status  = "status"
	Message = "message"
	Success = "success"
	Failure = "failure"
	Data    = "data"
)

// db constants
const (
	Server           = "server"
	User             = "user"
	Password         = "password"
	Port             = "port"
	Retry            = "retry"
	Database         = "database"
	TimeFormat       = "Jan 02 2006 03:04PM"
	MatDateTimeFomat = "2006/01/02 15:04"
	ExpFormat        = "02 Jan 2006"
	LogTimeFormat    = "2006-01-02-15:04"
	SQL              = "sqlserver"
	EQInsertQuery    = "eqDataInsertion"
	DERInsertQuery   = "dervDataInsertion"
	BackUpProcedure  = "backUpProc"
	DeleteDerivative = "deleteDervProc"
	DeleteEquity     = "deleteEQProc"
	MarketCapQuery   = "marketCapProc"
	StockIDQuery     = "stockIDUpdate"
)

// log constants
const (
	Path = "log_path"
	File = "log_file"
)

// api constants
const (
	SegmentsAllowed  = "segments_allowed"
	Env              = "env"
	ContentType      = "application/json"
	LastPage         = "hasLastPage"
	NextPage         = "nextPage"
	NseSeries        = "nse_series"
	BseSeries        = "bse_series"
	IndexInstruments = "index_instruments"
)

// config file path
const (
	BaseConfigPathKey          = "base-config-path"
	BaseConfigPathDefaultValue = "resources/configs"
	BaseConfigPathUsage        = "path to folder that stores your configurations"
	GetSecinfoUrl              = "getSecInfo"
	StockMasterUrl             = "stockMaster"
)
