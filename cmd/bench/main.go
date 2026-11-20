// SafePath Live — High-Throughput In-Memory Load Benchmark Generator.
//
// Zero external dependencies needed! Runs directly via `go run ./cmd/bench`.
// Generates up to 100,000 requests/second against the local API instance,
// measuring p50, p95, and p99 latencies in real time.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type TripResponse struct {
	ShareCode  string `json:"share_code"`
	OwnerToken string `json:"owner_token"`
}

func main() {
	targetURL := flag.String("url", "http://localhost:8081", "Target API server base URL")
	targetRPS := flag.Int("rps", 50000, "Target requests per second (e.g. 10000, 50000, 100000)")
	duration := flag.Duration("duration", 30*time.Second, "Benchmark duration")
	workers := flag.Int("workers", 2000, "Number of concurrent worker goroutines")
	flag.Parse()

	fmt.Println("────────────────────────────────────────────────────────────")
	fmt.Println("🚀 SafePath Live — 100k RPS High-Throughput Load Generator")
	fmt.Printf("   Target URL:  %s\n", *targetURL)
	fmt.Printf("   Target RPS:  %d requests/sec\n", *targetRPS)
	fmt.Printf("   Duration:    %s\n", *duration)
	fmt.Printf("   Workers:     %d concurrent goroutines\n", *workers)
	fmt.Println("────────────────────────────────────────────────────────────")

	// High-performance HTTP client tuned for 100k concurrent connections
	transport := &http.Transport{
		MaxIdleConns:        50000,
		MaxIdleConnsPerHost: 50000,
		MaxConnsPerHost:     50000,
		IdleConnTimeout:     90 * time.Second,
		DisableCompression:  true,
		ForceAttemptHTTP2:   false,
	}
	client := &http.Client{
		Transport: transport,
		Timeout:   3 * time.Second,
	}

	// Step 1: Create a benchmark test trip
	fmt.Print("[1/3] Initializing benchmark test trip... ")
	trip, err := createBenchmarkTrip(client, *targetURL)
	if err != nil {
		fmt.Printf("\n❌ Failed to create test trip: %v\n", err)
		fmt.Println("👉 Make sure the API server is running on " + *targetURL)
		os.Exit(1)
	}
	fmt.Printf("DONE! (Share Code: %s)\n", trip.ShareCode)

	// Step 2: Prepare worker channels and metrics
	ctx, cancel := context.WithTimeout(context.Background(), *duration)
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigCh
		fmt.Println("\n[!] Aborting benchmark early...")
		cancel()
	}()

	var (
		totalSent    atomic.Uint64
		totalSuccess atomic.Uint64
		totalErrors  atomic.Uint64
	)

	// Sampled latency tracker (recording every 10th request to prevent benchmark memory overhead)
	var latMu sync.Mutex
	latencies := make([]int64, 0, 500000)

	pingEndpoint := fmt.Sprintf("%s/v1/trips/%s/ping", *targetURL, trip.ShareCode)
	authHeader := "Bearer " + trip.OwnerToken

	// Work queue
	workQueue := make(chan struct{}, *targetRPS*2)

	// Rate pacemaker goroutine
	go func() {
		defer close(workQueue)
		interval := time.Second / time.Duration(*targetRPS)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				select {
				case workQueue <- struct{}{}:
				default:
					// queue full, drop tick to avoid memory buildup
				}
			}
		}
	}()

	fmt.Printf("[2/3] Warming up %d worker goroutines...\n", *workers)
	var wg sync.WaitGroup
	startBench := time.Now()

	for i := 0; i < *workers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			// Pre-allocated reusable ping payload
			payloadBuf := bytes.NewBuffer(make([]byte, 0, 128))

			for range workQueue {
				lat := 17.3850 + (rand.Float64()-0.5)*0.05
				lng := 78.4867 + (rand.Float64()-0.5)*0.05
				spd := 10.0 + rand.Float64()*15.0

				payloadBuf.Reset()
				fmt.Fprintf(payloadBuf, `{"lat":%.5f,"lng":%.5f,"speed_mps":%.1f}`, lat, lng, spd)

				req, err := http.NewRequestWithContext(ctx, http.MethodPost, pingEndpoint, bytes.NewReader(payloadBuf.Bytes()))
				if err != nil {
					totalErrors.Add(1)
					continue
				}
				req.Header.Set("Content-Type", "application/json")
				req.Header.Set("Authorization", authHeader)

				t0 := time.Now()
				resp, err := client.Do(req)
				durUs := time.Since(t0).Microseconds()

				totalSent.Add(1)

				if err != nil {
					totalErrors.Add(1)
					continue
				}

				// Drain body to allow TCP connection reuse
				_, _ = io.Copy(io.Discard, resp.Body)
				resp.Body.Close()

				if resp.StatusCode == http.StatusOK {
					totalSuccess.Add(1)
				} else {
					totalErrors.Add(1)
				}

				// Sample 5% of latencies for percentile calculations
				if rand.Float32() < 0.05 {
					latMu.Lock()
					if len(latencies) < cap(latencies) {
						latencies = append(latencies, durUs)
					}
					latMu.Unlock()
				}
			}
		}(i)
	}

	// Live Reporter Loop (prints stats every 1 second)
	fmt.Println("[3/3] Benchmark in progress... (press Ctrl+C to stop)")
	fmt.Println("────────────────────────────────────────────────────────────")

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	var lastSent uint64
	lastTime := time.Now()

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case now := <-ticker.C:
				currentSent := totalSent.Load()
				deltaSent := currentSent - lastSent
				deltaSec := now.Sub(lastTime).Seconds()
				currentRPS := float64(deltaSent) / deltaSec

				lastSent = currentSent
				lastTime = now

				fmt.Printf("⚡ Live RPS: %-9.0f | Total Sent: %-10d | Success: %-10d | Errors: %d\n",
					currentRPS, currentSent, totalSuccess.Load(), totalErrors.Load())
			}
		}
	}()

	wg.Wait()
	totalDuration := time.Since(startBench)

	// Final Summary Report
	fmt.Println("────────────────────────────────────────────────────────────")
	fmt.Println("📊 BENCHMARK COMPLETED — FINAL RESULTS")
	fmt.Println("────────────────────────────────────────────────────────────")

	sent := totalSent.Load()
	success := totalSuccess.Load()
	errs := totalErrors.Load()
	achievedRPS := float64(sent) / totalDuration.Seconds()

	fmt.Printf("Total Duration:     %.2f seconds\n", totalDuration.Seconds())
	fmt.Printf("Total Pings Sent:   %d\n", sent)
	fmt.Printf("Successful Pings:   %d (%.2f%%)\n", success, float64(success)/float64(max(1, sent))*100)
	fmt.Printf("Failed Pings:       %d\n", errs)
	fmt.Printf("Avg Throughput:     %.0f Requests / Second\n", achievedRPS)

	// Calculate Percentiles
	latMu.Lock()
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	n := len(latencies)
	if n > 0 {
		p50 := float64(latencies[n*50/100]) / 1000.0
		p90 := float64(latencies[n*90/100]) / 1000.0
		p95 := float64(latencies[n*95/100]) / 1000.0
		p99 := float64(latencies[n*99/100]) / 1000.0

		fmt.Println("────────────────────────────────────────────────────────────")
		fmt.Printf("Latency p50:        %.2f ms\n", p50)
		fmt.Printf("Latency p90:        %.2f ms\n", p90)
		fmt.Printf("Latency p95:        %.2f ms\n", p95)
		fmt.Printf("Latency p99:        %.2f ms\n", p99)
	}
	latMu.Unlock()
	fmt.Println("────────────────────────────────────────────────────────────")
}

func createBenchmarkTrip(client *http.Client, baseURL string) (*TripResponse, error) {
	payload := `{"dest_lat":17.4401,"dest_lng":78.3489,"arrival_radius_m":50,"expires_in_minutes":180}`
	req, err := http.NewRequest(http.MethodPost, baseURL+"/v1/trips", bytes.NewBufferString(payload))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
	}

	var trip TripResponse
	if err := json.NewDecoder(resp.Body).Decode(&trip); err != nil {
		return nil, err
	}
	return &trip, nil
}

func max(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}
