package integration

import (
  "fmt"
  "net/http"
  "testing"
  "time"
  "os"
)

const baseAddress = "http://balancer:8090"

var client = http.Client{
  Timeout: 3 * time.Second,
}

func TestBalancer(t *testing.T) {
  if _, exists := os.LookupEnv("INTEGRATION_TEST"); !exists {
		t.Skip("Integration test is not enabled")
  }

  serverHits := make(map[string]int)
  const requests = 20

  // чекаємо, щоб сервери запустились
  time.Sleep(6 * time.Second)

  for i := 0; i < requests; i++ {
    resp, err := client.Get(fmt.Sprintf("%s/api/v1/some-data", baseAddress))
    if err != nil {
      t.Fatalf("Request failed: %v", err)
    }
    from := resp.Header.Get("lb-from")
    resp.Body.Close() // закриваємо відповідь
    if from == "" {
      t.Errorf("Missing 'lb-from' header in response")
      continue
    }
    t.Logf("Response from [%s]", from)
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
    resp, err := client.Get(fmt.Sprintf("%s/api/v1/some-data", baseAddress))
    if err != nil {
      b.Fatalf("Request failed: %v", err)
    }
    resp.Body.Close()
  }
}