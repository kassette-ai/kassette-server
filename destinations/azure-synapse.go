package destinations

import (
	"database/sql"
	"fmt"
	"io"
	"kassette.ai/kassette-server/destinations/model"
	"kassette.ai/kassette-server/utils/logger"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const (
	host     = "host"
	dbName   = "database"
	user     = "user"
	password = "password"
	port     = "port"
	sslMode  = "sslMode"
)

const (
	mssqlStringLengthLimit = 512
	provider               = AZURE_SYNAPSE
	tableNameLimit         = 127
)

var kassetteDataTypesMapToMssql = map[string]string{
	"int":      "bigint",
	"float":    "decimal(28,10)",
	"string":   "varchar(512)",
	"datetime": "datetimeoffset",
	"boolean":  "bit",
	"json":     "jsonb",
}

var mssqlDataTypesMapToKassette = map[string]string{
	"integer":                  "int",
	"smallint":                 "int",
	"bigint":                   "int",
	"tinyint":                  "int",
	"double precision":         "float",
	"numeric":                  "float",
	"decimal":                  "float",
	"real":                     "float",
	"float":                    "float",
	"text":                     "string",
	"varchar":                  "string",
	"nvarchar":                 "string",
	"ntext":                    "string",
	"nchar":                    "string",
	"char":                     "string",
	"datetimeoffset":           "datetime",
	"date":                     "datetime",
	"datetime2":                "datetime",
	"timestamp with time zone": "datetime",
	"timestamp":                "datetime",
	"jsonb":                    "json",
	"bit":                      "boolean",
}

type AzureSynapse struct {
	DB                          *sql.DB
	Namespace                   string
	ObjectStorage               string
	NumWorkersDownloadLoadFiles int
	ConnectTimeout              time.Duration
	Warehouse                   Warehouse
}

type credentials struct {
	host     string
	dbName   string
	user     string
	password string
	port     string
	sslMode  string
	timeout  time.Duration
}

func connect(cred credentials) (*sql.DB, error) {
	// Create connection string
	// url := fmt.Sprintf("server=%s;user id=%s;password=%s;port=%s;database=%s;encrypt=%s;TrustServerCertificate=true", cred.host, cred.user, cred.password, cred.port, cred.dbName, cred.sslMode)

	query := url.Values{}
	query.Add("database", cred.dbName)
	query.Add("encrypt", cred.sslMode)

	if cred.timeout > 0 {
		query.Add("dial timeout", fmt.Sprintf("%d", cred.timeout/time.Second))
	}
	query.Add("TrustServerCertificate", "true")
	port, err := strconv.Atoi(cred.port)
	if err != nil {
		return nil, fmt.Errorf("invalid port: %w", err)
	}
	connUrl := &url.URL{
		Scheme:   "sqlserver",
		User:     url.UserPassword(cred.user, cred.password),
		Host:     net.JoinHostPort(cred.host, strconv.Itoa(port)),
		RawQuery: query.Encode(),
	}

	var db *sql.DB
	if db, err = sql.Open("sqlserver", connUrl.String()); err != nil {
		return nil, fmt.Errorf("synapse connection error : (%v)", err)
	}
	return db, nil
}

func (as *AzureSynapse) getConnectionCredentials() credentials {
	return credentials{
		host:     GetConfigValue(host, as.Warehouse),
		dbName:   GetConfigValue(dbName, as.Warehouse),
		user:     GetConfigValue(user, as.Warehouse),
		password: GetConfigValue(password, as.Warehouse),
		port:     GetConfigValue(port, as.Warehouse),
		sslMode:  GetConfigValue(sslMode, as.Warehouse),
		timeout:  as.ConnectTimeout,
	}
}

func (as *AzureSynapse) createTable(name string, columns model.TableSchema) (err error) {
	sqlStatement := fmt.Sprintf(`IF  NOT EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'%[1]s') AND type = N'U')
	CREATE TABLE %[1]s ( %v )`, name, columnsWithDataTypes(columns, ""))

	logger.Info(fmt.Sprintf("AZ: Creating table in synapse for AZ:%s : %v", as.Warehouse.Destination.ID, sqlStatement))
	_, err = as.DB.Exec(sqlStatement)
	return
}

func columnsWithDataTypes(columns model.TableSchema, prefix string) string {
	var arr []string
	for name, dataType := range columns {
		arr = append(arr, fmt.Sprintf(`%s%s %s`, prefix, name, kassetteDataTypesMapToMssql[dataType]))
	}
	return strings.Join(arr, ",")
}

func (as *AzureSynapse) CreateSchema() (err error) {
	sqlStatement := fmt.Sprintf(`IF NOT EXISTS ( SELECT  * FROM  sys.schemas WHERE   name = N'%s' )
    EXEC('CREATE SCHEMA [%s]');
`, as.Namespace, as.Namespace)
	logger.Info(fmt.Sprintf("SYNAPSE: Creating schema name in synapse for AZ:%s : %v", as.Warehouse.Destination.ID, sqlStatement))
	_, err = as.DB.Exec(sqlStatement)
	if err == io.EOF {
		return nil
	}
	return
}
