package integration

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"testing"
	"time"
)

const (
	baseAddress = "http://balancer:8090"
	teamName    = "sophisticated_operations_on_software_architecture_labs"
)

var client = http.Client{
	Timeout: 3 * time.Second,
}

func TestBalancer(t *testing.T) {
	if _, exists := os.LookupEnv("INTEGRATION_TEST"); !exists {
		t.Skip("Integration test is not enabled")
	}

	serverHits := make(map[string]int)
	const requests = 20

	time.Sleep(6 * time.Second)

	for i := 0; i < requests; i++ {
		resp, err := client.Get(fmt.Sprintf("%s/api/v1/some-data?key=%s", baseAddress, teamName))
		if err != nil {
			t.Fatalf("Request failed: %v", err)
		}

		if resp.StatusCode != http.StatusOK {
			t.Errorf("Expected status code 200, got %d", resp.StatusCode)
		}

		from := resp.Header.Get("lb-from")
		
		body, err := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		
		if err != nil {
			t.Fatalf("Failed to read response body: %v", err)
		}
		
		if len(body) == 0 {
			t.Errorf("Expected non-empty response, got empty")
		}
		
		t.Logf("Response from [%s]: %s", from, string(body))

		if from == "" {
			t.Errorf("Missing 'lb-from' header in response")
			continue
		}

		serverHits[from]++
	}

	t.Logf("Server hits distribution: %v", serverHits)
	if len(serverHits) < 2 {
		t.Errorf("Expected requests to hit at least 2 different servers, got %d", len(serverHits))
	}
}

func BenchmarkBalancer(b *testing.B) {
	if _, exists := os.LookupEnv("INTEGRATION_TEST"); !exists {
		b.Skip("Integration test is not enabled")
	}

	for i := 0; i < b.N; i++ {
		resp, err := client.Get(fmt.Sprintf("%s/api/v1/some-data?key=%s", baseAddress, teamName))
		if err != nil {
			b.Fatalf("Request failed: %v", err)
		}
		
		if resp.StatusCode != http.StatusOK {
			b.Fatalf("Expected status code 200, got %d", resp.StatusCode)
		}
		
		_, err = ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		
		if err != nil {
			b.Fatalf("Failed to read response body: %v", err)
		}
	}
}