package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/roman-mazur/architecture-practice-4-template/httptools"
	"github.com/roman-mazur/architecture-practice-4-template/signal"
)

var (
	port        = flag.Int("port", 8090, "load balancer port")
	timeoutSec  = flag.Int("timeout-sec", 3, "request timeout time in seconds")
	https       = flag.Bool("https", false, "whether backends support HTTPs")
	traceEnabled = flag.Bool("trace", false, "whether to include tracing information into responses")
)

var (
	timeout     time.Duration
	serversPool = []string{
		"server1:8080",
		"server2:8080",
		"server3:8080",
	}
)

type BackendServer struct {
	Address string
	Traffic int64
	Healthy bool
}

var (
	backendStats = make(map[string]*BackendServer)
	mu           sync.Mutex
)

func scheme() string {
	if *https {
		return "https"
	}
	return "http"
}

func health(dst string) bool {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	req, _ := http.NewRequestWithContext(ctx, "GET",
		fmt.Sprintf("%s://%s/health", scheme(), dst), nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()

	return resp.StatusCode == http.StatusOK
}

func forward(dst string, rw http.ResponseWriter, r *http.Request) error {
	ctx, cancel := context.WithTimeout(r.Context(), timeout)
	defer cancel()

	fwdRequest := r.Clone(ctx)
	fwdRequest.RequestURI = ""
	fwdRequest.URL.Host = dst
	fwdRequest.URL.Scheme = scheme()
	fwdRequest.Host = dst

	resp, err := http.DefaultClient.Do(fwdRequest)
	if err == nil {
		defer resp.Body.Close()
		for k, values := range resp.Header {
			for _, value := range values {
				rw.Header().Add(k, value)
			}
		}
		if *traceEnabled {
			rw.Header().Set("lb-from", dst)
		}
		log.Println("fwd", resp.StatusCode, resp.Request.URL)
		rw.WriteHeader(resp.StatusCode)
		_, err := io.Copy(rw, resp.Body)
		if err != nil {
			log.Printf("Failed to write response: %s", err)
		}
		return nil
	} else {
		log.Printf("Failed to get response from %s: %s", dst, err)
		rw.WriteHeader(http.StatusServiceUnavailable)
		return err
	}
}

func getLeastTrafficServer() *BackendServer {
	mu.Lock()
	defer mu.Unlock()

	var selected *BackendServer
	for _, server := range backendStats {
		if server.Healthy {
			if selected == nil || server.Traffic < selected.Traffic {
				selected = server
			}
		}
	}
	return selected
}

func main() {
	flag.Parse()
	timeout = time.Duration(*timeoutSec) * time.Second

	for _, addr := range serversPool {
		backendStats[addr] = &BackendServer{
			Address: addr,
			Traffic: 0,
			Healthy: false,
		}
	}

	for _, addr := range serversPool {
		addr := addr
		go func() {
			for range time.Tick(5 * time.Second) {
				isHealthy := health(addr)
				mu.Lock()
				backendStats[addr].Healthy = isHealthy
				mu.Unlock()
				log.Println(addr, "healthy:", isHealthy)
			}
		}()
	}

	frontend := httptools.CreateServer(*port, http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		server := getLeastTrafficServer()
		if server == nil {
			http.Error(rw, "No healthy servers available", http.StatusServiceUnavailable)
			return
		}

		err := forward(server.Address, rw, r)
		if err == nil {
			mu.Lock()
			server.Traffic++
			mu.Unlock()
		}
	}))

	log.Println("Starting load balancer...")
	log.Printf("Tracing support enabled: %t", *traceEnabled)
	frontend.Start()
	signal.WaitForTerminationSignal()
}
