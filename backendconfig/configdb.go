package backendconfig

import (
	"database/sql"
	"fmt"
	"github.com/spf13/viper"
	"log"
	"time"
)

func GetConnectionString() string {
	return fmt.Sprintf("host=%s port=%s user=%s "+
		"password=%s dbname=%s sslmode=disable",
		viper.GetString("database.host"),
		viper.GetString("database.port"),
		viper.GetString("database.user"),
		viper.GetString("database.password"),
		viper.GetString("database.name"))
}

type HandleT struct {
	dbHandle        *sql.DB
	destinationList []DestinationT
	sourceList      []SourceT
}

func (cd *HandleT) Setup(clearAll bool, tablePrefix string, retentionPeriod time.Duration, toBackup bool) {

	var err error

	psqlInfo := GetConnectionString()

	cd.dbHandle, err = sql.Open("postgres", psqlInfo)

	if err != nil {
		log.Fatal("Failed to open DB connection", err)
	}

	cd.createConfigTable()

}

func (cd *HandleT) getConfig() {
	sqlStatement := fmt.Sprintf(`SELECT * FROM source_config`)
	result, _ := cd.dbHandle.Prepare(sqlStatement)

	defer result.Close()

	rows, _ := result.Query()
	defer rows.Close()

	//var sourceConfig SourcesT

	for rows.Next() {
		var id int
		var source string
		var writeKey string

		err := rows.Scan(&id, &source, &writeKey)
		if err != nil {
			log.Fatal(err)
		}

		//cd.sourceList = append(sourceConfig, SourcesT{Source: source, WriteKey: writeKey})
	}

	//return sourceConfig

}

func (cd *HandleT) createConfigTable() {

	sqlStatement := fmt.Sprintf(
		`CREATE TABLE IF NOT EXISTS source_config (
		id BIGSERIAL PRIMARY KEY,
		source JSONB NOT NULL,
		write_key VARCHAR(255) NOT NULL);`)

	_, _ = cd.dbHandle.Exec(sqlStatement)

	return
}
