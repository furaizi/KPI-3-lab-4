package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/roman-mazur/architecture-practice-4-template/httptools"
	"github.com/roman-mazur/architecture-practice-4-template/signal"
)

var port = flag.Int("port", 8080, "server port")
var dbAddress = flag.String("db-addr", "db:8070", "DB service address")

const confResponseDelaySec = "CONF_RESPONSE_DELAY_SEC"
const confHealthFailure = "CONF_HEALTH_FAILURE"

const teamName = "sophisticated_operations_on_software_architecture_labs"

type DbResponse struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func main() {
	flag.Parse()

	currentDate := time.Now().Format("2006-01-02")
	storeInitialData(teamName, currentDate)

	h := new(http.ServeMux)

	h.HandleFunc("/health", func(rw http.ResponseWriter, r *http.Request) {
		rw.Header().Set("content-type", "text/plain")
		if failConfig := os.Getenv(confHealthFailure); failConfig == "true" {
			rw.WriteHeader(http.StatusInternalServerError)
			_, _ = rw.Write([]byte("FAILURE"))
		} else {
			rw.WriteHeader(http.StatusOK)
			_, _ = rw.Write([]byte("OK"))
		}
	})

	report := make(Report)

	h.HandleFunc("/api/v1/some-data", func(rw http.ResponseWriter, r *http.Request) {
		respDelayString := os.Getenv(confResponseDelaySec)
		if delaySec, parseErr := strconv.Atoi(respDelayString); parseErr == nil && delaySec > 0 && delaySec < 300 {
			time.Sleep(time.Duration(delaySec) * time.Second)
		}

		report.Process(r)

		key := r.URL.Query().Get("key")
		if key == "" {
			rw.WriteHeader(http.StatusBadRequest)
			return
		}

		value, err := fetchFromDb(key)
		if err != nil {
			rw.WriteHeader(http.StatusNotFound)
			return
		}

		rw.Header().Set("content-type", "application/json")
		rw.WriteHeader(http.StatusOK)
		_, _ = rw.Write([]byte(value))
	})

	h.Handle("/report", report)

	server := httptools.CreateServer(*port, h)
	server.Start()
	signal.WaitForTerminationSignal()
}

func storeInitialData(key, value string) {
	url := fmt.Sprintf("http://%s/db/%s", *dbAddress, key)
	
	payload := fmt.Sprintf(`{"value": "%s"}`, value)
	
	req, err := http.NewRequest("POST", url, strings.NewReader(payload))
	if err != nil {
		log.Printf("Error creating request to DB: %s", err)
		return
	}
	
	req.Header.Set("Content-Type", "application/json")
	
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Printf("Failed to store initial data in DB: %s", err)
		return
	}
	defer resp.Body.Close()
	
	log.Printf("Stored initial data in DB, status: %d", resp.StatusCode)
}

func fetchFromDb(key string) (string, error) {
	url := fmt.Sprintf("http://%s/db/%s", *dbAddress, key)
	
	resp, err := http.Get(url)
	if err != nil {
		log.Printf("Error fetching from DB: %s", err)
		return "", err
	}
	defer resp.Body.Close()
	
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("DB service returned status %d", resp.StatusCode)
	}
	
	var dbResp DbResponse
	if err := json.NewDecoder(resp.Body).Decode(&dbResp); err != nil {
		log.Printf("Error decoding DB response: %s", err)
		return "", err
	}
	
	return dbResp.Value, nil
}