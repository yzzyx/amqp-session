package session

import (
	"bytes"
	"github.com/streadway/amqp"
	"os"
	"sync"
	"testing"
	"time"
)

var testServerAddress = "amqp://guest:guest@localhost:5672/"

func declareQueues(conn *amqp.Connection, ch *amqp.Channel) error {
	// Declare the default exchange
	err := ch.ExchangeDeclare(
		"test-exchange",
		"direct", // type
		true,     // durable
		false,    // auto-deleted
		false,    // exclusive
		false,    // no-wait
		nil,      // arguments
	)
	if err != nil {
		return err
	}

	q, err := ch.QueueDeclare("test-queue",
		true,  // durable
		false, // autodelete
		false, // exclusive
		false, // noWait
		nil)   // args
	if err != nil {
		return err
	}

	// Make sure queue is empty
	_, err = ch.QueuePurge("test-queue", false)
	if err != nil {
		return err
	}

	err = ch.QueueBind(q.Name,
		"test-key",      // route key
		"test-exchange", // exchange
		false,           // nowait
		nil)             // args

	return err
}

func consumerTest(t *testing.T, expectedCount int, maxTime time.Duration) {
	s := New()
	s.Address = testServerAddress
	s.OnInit = declareQueues
	s.Start()
	defer func() {
		err := s.Close()
		if err != nil {
			t.Error("Close returned error", err)
		}
	}()

	ch, closech, err := s.StreamQueue("test-queue")
	if err != nil {
		t.Fatalf("StreamQueue returned error: %s", err)
		return
	}

	start := time.Now()
	timeout := time.After(maxTime)
	count := 0

	for {
		select {
		case msg := <-ch:
			// Check contents of message
			if bytes.Compare(msg.Body, []byte("test-message")) != 0 {
				t.Errorf("Expected body to be 'test-message', got '%s'", string(msg.Body))
				return
			}
			err := msg.Ack(false)
			if err != nil {
				t.Errorf("Could not ACK message: %s", err)
			}
			count++
			if count == expectedCount {
				t.Logf("Recieved %d messages after %s", count, time.Since(start))
				return
			}
		case <-closech:
			t.Errorf("Received unexpected close-signal from streamqueue")
			return
		case <-timeout:
			t.Errorf("Timeout reached before expected number of messages was seen - expected %d, got %d (after %s)", expectedCount, count, maxTime)
			return
		}
	}
}

func publisherTest(t *testing.T, expectedCount int, maxTime time.Duration) {
	s := New()
	s.Address = testServerAddress
	s.OnInit = declareQueues
	s.Publisher()
	s.Start()
	defer func() {
		// Wait for buffers to be flushed
		time.Sleep(5 * time.Second)
		err := s.Close()
		if err != nil {
			t.Error("Close returned error", err)
		}
	}()

	for !s.IsReady() {
		time.Sleep(100 * time.Millisecond)
	}

	start := time.Now()
	timeout := time.After(maxTime)

	msg := amqp.Publishing{
		Body: []byte("test-message"),
	}

	for count := 0; count < expectedCount; count++ {
		select {
		case <-timeout:
			t.Errorf("Timeout reached before expected number of messages was sent - expected %d, got %d (after %s)", expectedCount, count, maxTime)
			return
		default:
			err := s.Push("test-exchange", "test-key", msg)
			if err != nil {
				t.Errorf("Push returned error: %s", err)
				return
			}
		}
	}
	t.Logf("Sent %d messages in %s", expectedCount, time.Since(start))
}

func TestSession_PublishConsume(t *testing.T) {
	env := os.Getenv("AMQP_SESSION_SERVER")
	if env != "" {
		testServerAddress = env
	}

	wg := sync.WaitGroup{}

	wg.Add(1)
	go func() {
		consumerTest(t, 100000, 1*time.Minute)
		wg.Done()
	}()

	wg.Add(1)
	go func() {
		publisherTest(t, 100000, 1*time.Minute)
		wg.Done()
	}()

	// Wait for tests to finish
	wg.Wait()
}
