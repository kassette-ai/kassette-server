package connections

//Put in your worker here

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"time"

	_ "github.com/lib/pq"
)

type Record struct {
	ID      int    `json:"id"`
	Name    string `json:"name"`
	Email   string `json:"email"`
	Address string `json:"address"`
}

func main() {

	host := os.Getenv("POSTGRES_HOST")
	port, err := strconv.Atoi(os.Getenv("POSTGRES_PORT"))
	if err != nil {
		fmt.Println("Error, invalid port value:", err)
		return
	}

	user := os.Getenv("POSTGRES_USER")
	password := os.Getenv("POSTGRES_PASSWORD")
	dbname := os.Getenv("POSTGRES_DB")

	tickerSecs, err := strconv.Atoi(os.Getenv("TICKER"))

	if err != nil {
		fmt.Println("Error, invalid ticker value:", err)
		return
	}

	connectionString := fmt.Sprintf("postgres",
		"host=%s port=%s"+
			"user=%s password=%s "+
			"dbname=%s sslmode=disable", host, port, user, password, dbname)

	// Connect to the database
	db, err := sql.Open("postgres", connectionString)
	if err != nil {
		fmt.Println("Error connecting to database:", err)
		return
	}
	defer db.Close()

	// Create a ticker that polls the database every 10 seconds
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// Query the database for new records
			rows, err := db.Query("SELECT * FROM mytable WHERE processed = false")
			if err != nil {
				fmt.Println("Error querying database:", err)
				continue
			}
			defer rows.Close()

			// Process the new records
			for rows.Next() {
				var record Record
				err := rows.Scan(&record.ID, &record.Name, &record.Email, &record.Address)
				if err != nil {
					fmt.Println("Error reading row:", err)
					continue
				}

				// Send the record to the REST endpoint
				url := "https://myapi.com/records"
				payload, err := json.Marshal(record)
				if err != nil {
					fmt.Println("Error marshaling record:", err)
					continue
				}

				resp, err := http.Post(url, "application/json", bytes.NewBuffer(payload))
				if err != nil {
					fmt.Println("Error sending record to API:", err)
					continue
				}
				defer resp.Body.Close()

				// Mark the record as processed
				_, err = db.Exec("UPDATE mytable SET processed = true WHERE id = ?", record.ID)
				if err != nil {
					fmt.Println("Error updating row:", err)
					continue
				}

				// Print a message indicating that the record was processed
				fmt.Printf("Processed record: id=%d, name=%s, email=%s, address=%s\n", record.ID, record.Name, record.Email, record.Address)
			}
		}
	}
}
