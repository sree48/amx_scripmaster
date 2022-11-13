package entities

type MssqlConnection struct {
	Server   string
	Port     int
	User     string
	Password string
	Database string
}
