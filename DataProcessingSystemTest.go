package main

import (
	"bufio"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"time"
)

// -----------------------
// Task Structure
// -----------------------

type Task struct {
	ID      string
	Payload string
}

// -----------------------
// Worker Function
// -----------------------

func worker(workerID int, tasks <-chan Task, results chan<- string, wg *sync.WaitGroup) {
	logger := log.Default()
	logger.Printf("Worker-%d started at %s", workerID, time.Now().Format(time.RFC3339))

	defer func() {
		logger.Printf("Worker-%d terminated at %s", workerID, time.Now().Format(time.RFC3339))
		wg.Done()
	}()

	for task := range tasks {
		// Simulate processing delay
		sleepMs := time.Duration(100+rand.Intn(500)) * time.Millisecond
		time.Sleep(sleepMs)

		// Simulated 5% processing failure
		if rand.Intn(100) < 5 {
			err := errors.New("simulated processing error")
			logger.Printf("Worker-%d ERROR processing task %s: %v", workerID, task.ID, err)
			results <- fmt.Sprintf("Task %s FAILED by Worker-%d: %v", task.ID, workerID, err)
			continue
		}

		// Successful processing
		output := fmt.Sprintf(
			"TaskID=%s processedBy=Worker-%d durationMs=%d payload=%s",
			task.ID, workerID, sleepMs.Milliseconds(), task.Payload,
		)
		results <- output
		logger.Printf("Worker-%d completed %s", workerID, task.ID)
	}
}

// -----------------------
// Safe File Writing
// -----------------------

func writeResults(filename string, results []string) error {
	f, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer f.Close()

	w := bufio.NewWriter(f)
	for _, line := range results {
		_, err := w.WriteString(line + "\n")
		if err != nil {
			return err
		}
	}
	return w.Flush()
}

// -----------------------
// Main Program
// -----------------------

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	const NUM_WORKERS = 4
	const NUM_TASKS = 30
	const OUTPUT_FILE = "results.txt"

	rand.Seed(time.Now().UnixNano())

	log.Println("Main: Starting DataProcessingSystem")

	// Channels as concurrency-safe queue
	taskQueue := make(chan Task, 10) // buffered
	resultQueue := make(chan string, NUM_TASKS)

	// Worker goroutines
	var wg sync.WaitGroup
	for i := 1; i <= NUM_WORKERS; i++ {
		wg.Add(1)
		go worker(i, taskQueue, resultQueue, &wg)
	}

	// Producer: Add tasks
	go func() {
		for i := 1; i <= NUM_TASKS; i++ {
			task := Task{
				ID:      fmt.Sprintf("T-%d", i),
				Payload: fmt.Sprintf("payload-%d", i),
			}
			taskQueue <- task
			time.Sleep(20 * time.Millisecond)
		}
		close(taskQueue) // Signals no more tasks
	}()

	// Collect results in another goroutine
	var results []string
	var rmu sync.Mutex
	var collector sync.WaitGroup

	collector.Add(1)
	go func() {
		defer collector.Done()
		for r := range resultQueue {
			rmu.Lock()
			results = append(results, r)
			rmu.Unlock()
		}
	}()

	// Wait for workers to finish, then close results channel
	wg.Wait()
	close(resultQueue)
	collector.Wait()

	log.Printf("Main: All workers finished. Total results collected: %d", len(results))

	// Write results to file
	if err := writeResults(OUTPUT_FILE, results); err != nil {
		log.Fatalf("Main: I/O error writing results: %v", err)
	}

	log.Printf("Main: Results written to %s", OUTPUT_FILE)
	log.Println("Main: DataProcessingSystem finished.")
}
