package session

import (
	"time"

	"github.com/streadway/amqp"
)

// StreamQueue will continuously put queue items on the channel.
// It is required to call delivery.Ack when it has been
// successfully processed, or delivery.Nack when it fails.
// Ignoring this will cause data to build up on the server.
//
// StreamQueue returns a channel to read from, a cancellation channel
// and possibly an error.
// The cancellation channel will be closed when the session is terminated,
// and can be used to terminate any waiting listeners.
func (session *Session) StreamQueue(queueName string) (<-chan amqp.Delivery, <-chan struct{}, error) {

	initialise := func() (<-chan amqp.Delivery, error) {
		// Wait until we're connected
		for {
			if session.IsReady() {
				break
			}
			time.Sleep(1 * time.Second)
		}

		// Make sure that channel is not being replaced
		session.channelMutex.RLock()
		defer session.channelMutex.RUnlock()

		return session.channel.Consume(queueName,
			"",    // consumer name (left blank for a unique consumer name)
			false, // auto-ack
			false, // exclusive
			false, // no local
			false, // no wait
			nil)   // args
	}

	consumerChannel := make(chan amqp.Delivery)

	ch, err := initialise()
	if err != nil {
		return nil, nil, err
	}

	doneSignal := make(chan struct{})

	// Wait for session to become ready
	go func() {
		reconnectChan := session.reconnectChan
		for {
			if !session.IsReady() {
				time.Sleep(1 * time.Second)
				continue
			}

			select {
			case <-reconnectChan:
				// When reconnectChan is closed, a reconnect attempt has started.
				// This means that we'll have to reconnect our listeners
				for {
					ch, err = initialise()
					if err == nil {
						break
					}
					time.Sleep(session.ReconnectDelay)
				}
				reconnectChan = session.reconnectChan
			case msg := <-ch:
				// pass message along
				consumerChannel <- msg
			case <-session.done:
				// we're done
				close(doneSignal)
				return
			}

		}
	}()

	return consumerChannel, doneSignal, nil
}
