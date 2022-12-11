package persistance

import (
	"database/sql"
)

type Database interface {
	GetDBConnection() *sql.DB
	Reconnect() (*sql.DB, bool)
	MssqlConnCheck() bool
}
