package main

import (
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"os"
	"path/filepath"

	"github.com/roman-mazur/architecture-practice-4-template/datastore"
	"github.com/roman-mazur/architecture-practice-4-template/httptools"
	"github.com/roman-mazur/architecture-practice-4-template/signal"
)

var port = flag.Int("port", 8070, "database server port")

type Response struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func main() {
	flag.Parse()
	log.Printf("Starting db server at port %d", *port)

	dbDir := "db_data"
	if err := os.MkdirAll(dbDir, 0755); err != nil {
		log.Fatalf("Failed to create DB directory: %s", err)
	}

	db, err := datastore.Open(dbDir)
	if err != nil {
		log.Fatalf("Failed to open database: %s", err)
	}
	defer db.Close()

	http.HandleFunc("/db/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			key := filepath.Base(r.URL.Path)
			
			value, err := db.Get(key)
			if err != nil {
				log.Printf("Error fetching key %s: %s", key, err)
				http.Error(w, "Not found", http.StatusNotFound)
				return
			}

			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			response := Response{
				Key:   key,
				Value: value,
			}
			if err := json.NewEncoder(w).Encode(response); err != nil {
				log.Printf("Error encoding response: %s", err)
			}
		} else if r.Method == http.MethodPost {
			key := filepath.Base(r.URL.Path)
			
			var reqBody struct {
				Value string `json:"value"`
			}
			
			if err := json.NewDecoder(r.Body).Decode(&reqBody); err != nil {
				http.Error(w, "Invalid request body", http.StatusBadRequest)
				return
			}
			
			if err := db.Put(key, reqBody.Value); err != nil {
				log.Printf("Error storing key %s: %s", key, err)
				http.Error(w, "Internal server error", http.StatusInternalServerError)
				return
			}
			
			w.WriteHeader(http.StatusOK)
		} else {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
	})

	server := httptools.CreateServer(*port, nil)
	server.Start()
	signal.WaitForTerminationSignal()
}
