package session

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/streadway/amqp"
)

func TestSession_PushSpeed(t *testing.T) {
	unconfirmedMessages := 0
	sentMessages := 0

	s := New()
	s.Address = testServerAddress
	s.OnInit = declareQueues
	s.OnShutdown = func(exchangeName string, routeKey string, msg amqp.Publishing) error {
		unconfirmedMessages++
		return nil
	}
	s.Publisher()
	s.Start()

	// Wait until we're connected
	for !s.IsReady() {
		time.Sleep(100 * time.Millisecond)
	}

	timeout := time.After(5 * time.Second)

	msg := amqp.Publishing{
		Body: []byte("test-message"),
	}

	// Push as many messages as possible
loop:
	for {
		select {
		case <-timeout:
			break loop
		default:
			err := s.Push("test-exchange", "test-key", msg)
			if err != nil {
				t.Errorf("Push returned error: %s", err)
				break loop
			}
			sentMessages++
		}
	}

	// Close publishing session immediately, which should call 'OnShutdown'
	ctx, _ := context.WithTimeout(context.Background(), 0)
	err := s.CloseWithContext(ctx)
	if err != nil {
		t.Error("Close returned error", err)
	}

	// Even on a very slow system, we should be able to push atleast 10000 messages
	if sentMessages < 10000 {
		t.Error("Only sent", sentMessages, "messages, expected 10000")
	}

	// We will probably have a bunch of unconfirmed messages, but at least half should be confirmed
	if unconfirmedMessages == 0 {
		t.Error("Expected at least 1 unconfirmed message")
	}

	if unconfirmedMessages > sentMessages/2 {
		t.Error("Expected at least half of sent messages to be confirmed")
	}
}

func testUnconfirmed(t *testing.T, shutdownTimeout time.Duration, unconfirmedExpected bool) {
	unconfirmedMessages := 0
	sentMessages := 0

	s := New()
	s.Address = testServerAddress
	s.OnInit = declareQueues
	s.OnShutdown = func(exchangeName string, routeKey string, msg amqp.Publishing) error {
		unconfirmedMessages++
		return nil
	}
	s.Publisher()
	s.Start()

	// Wait until we're connected
	for !s.IsReady() {
		time.Sleep(100 * time.Millisecond)
	}

	timeout := time.After(5 * time.Second)

	msg := amqp.Publishing{
		Body: []byte("test-message"),
	}

	numberOfMessages := 10000
loop:
	for i := 0; i < numberOfMessages; i++ {
		select {
		case <-timeout:
			t.Errorf("Timeout occurred before all messages was sent")
		default:
			err := s.Push("test-exchange", "test-key", msg)
			if err != nil {
				t.Errorf("Push returned error: %s", err)
				break loop
			}
			sentMessages++
		}
	}

	fmt.Println("sent messages:", sentMessages)
	ctx, _ := context.WithTimeout(context.Background(), shutdownTimeout)
	err := s.CloseWithContext(ctx)
	if err != nil {
		t.Errorf("Close returned error: %s", err)
	}

	if sentMessages < numberOfMessages {
		t.Errorf("Not all messages were sent (%d of %d sent)", sentMessages, numberOfMessages)
	}

	if unconfirmedExpected && unconfirmedMessages == 0 {
		t.Errorf("Expected number of unconfirmed messages to be larger than 0, got 0")
		return
	} else if !unconfirmedExpected && unconfirmedMessages > 0 {
		t.Errorf("Expected number of unconfirmed messages to be 0, got %d", unconfirmedMessages)
		return
	}
}

func TestSession_PushUnconfirmed(t *testing.T) {
	// WHEN timeout is 5 seconds,
	// THEN all queues should be flushed, and no unconfirmed messages be left
	testUnconfirmed(t, 5*time.Second, false)

	// WHEN timeout is 0,
	// THEN unconfirmed messages should be left when we shut down
	testUnconfirmed(t, 0, true)

}
