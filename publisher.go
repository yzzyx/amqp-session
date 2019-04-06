package session

import (
	"context"
	"time"

	"github.com/streadway/amqp"
)

func (session *Session) intConfirmReceived(confirm amqp.Confirmation) {
	session.unconfirmedMutex.Lock()
	defer session.unconfirmedMutex.Unlock()

	for e := session.unconfirmedMessages.Front(); e != nil; {
		if msg, ok := e.Value.(*message); ok && msg.deliveryTag > 0 {
			if msg.deliveryTag <= confirm.DeliveryTag {
				session.unconfirmedMessages.Remove(e)
				e = session.unconfirmedMessages.Front()
				session.unconfirmedCount--
				if session.unconfirmedCount < 0 {
					session.unconfirmedCount = 0
				}
				continue
			} else {
				// messages are placed in the queue in order,
				// which means that if we see a deliverytag with higher value than
				// our confirmed value, we can exit the loop
				break
			}
		}
		// Walk to next message
		e = e.Next()
	}
}

func (session *Session) intPublishMessage(msg *message) error {
	// Add unconfirmed message to queue without a deliverytag
	// - these will be picked up by 'resendTimer'
	session.unconfirmedMutex.Lock()
	session.unconfirmedMessages.PushBack(msg)
	session.unconfirmedCount++
	session.unconfirmedMutex.Unlock()

	err := session.unsafePush(msg)
	if err != nil {
		// Pick message up on next iteration
		return err
	}

	// All updates to unconfirmed messages should be wrapped in this mutex
	session.unconfirmedMutex.Lock()
	msg.deliveryTag = session.incDeliveryTag()
	session.unconfirmedMutex.Unlock()
	return nil
}

// incDeliveryTag increments deliverytag by one and returns the new value
func (session *Session) incDeliveryTag() uint64 {
	session.deliveryTagMutex.Lock()
	defer session.deliveryTagMutex.Unlock()
	session.expectedDeliveryTag++
	return session.expectedDeliveryTag
}

// resetDeliveryTag sets deliverytag to 0
func (session *Session) resetDeliveryTag() uint64 {
	session.deliveryTagMutex.Lock()
	defer session.deliveryTagMutex.Unlock()
	session.expectedDeliveryTag = 0
	return session.expectedDeliveryTag
}

// handlePublish receives messages via a publishing channel, and
// makes sure that we receive confirmations for them
func (session *Session) handlePublish() {

	resendTimer := time.After(session.ReSendDelay)
	session.unconfirmedCount = 0
	reconnected := false

	resendMessages := func() {
		session.unconfirmedMutex.Lock()
		defer session.unconfirmedMutex.Unlock()

		for e := session.unconfirmedMessages.Front(); e != nil; e = e.Next() {
			if msg, ok := e.Value.(*message); ok && msg.deliveryTag == 0 {
				// Messages with deliverytag 0 has not been sent yet
				err := session.unsafePush(msg)
				if err != nil {
					// On error, wait again
					continue
				}
				msg.deliveryTag = session.incDeliveryTag()
			}
		}
	}

	defer func() {
		// On exit, make sure that no goroutine is stuck in 'Push' because of a full queue
		session.publishQueueIsFullCond.L.Lock()
		if session.publishQueueIsFull {
			session.publishQueueIsFull = false
			session.publishQueueIsFullCond.Broadcast()
		}
		session.publishQueueIsFullCond.L.Unlock()
	}()

	for {

		// Wait until we're ready
		if !session.IsReady() {
			time.Sleep(100 * time.Millisecond)
			reconnected = true
			continue
		}

		// Reconnected is set when we've been disconnected and we're now connected again.
		// This can happen in two ways:
		//   - session was marked as not ready, but now it's ready again
		//   - session.reconnectChan was closed (reInit was called)
		if reconnected {
			reconnected = false // clear flag

			// This means that we need to update our existing list of messages
			// and mark all messages as unsent
			session.resetDeliveryTag()
			cnt := 0

			session.unconfirmedMutex.Lock()
			for e := session.unconfirmedMessages.Front(); e != nil; e = e.Next() {
				if msg, ok := e.Value.(*message); ok {
					cnt++
					msg.deliveryTag = 0
				}
			}
			session.unconfirmedMutex.Unlock()

			// Resend all messages
			resendMessages()
		}

		select {
		case msg := <-session.publishChan:
			// A new message was added to the end of our list
			err := session.intPublishMessage(msg)
			if err != nil {
				session.log.Println("Error received when publishing, will retry. Error:", err)
			}

			// Update 'isFull' flag if neccessary
			session.publishQueueIsFullCond.L.Lock()
			session.unconfirmedMutex.RLock()
			if session.unconfirmedCount >= session.MaxInTransit && !session.publishQueueIsFull {
				session.publishQueueIsFull = true
			}
			session.unconfirmedMutex.RUnlock()
			session.publishQueueIsFullCond.L.Unlock()

		case confirm := <-session.notifyConfirm:
			session.intConfirmReceived(confirm)

			// Update 'isFull' flag if neccessary
			session.publishQueueIsFullCond.L.Lock()
			session.unconfirmedMutex.RLock()
			if session.unconfirmedCount < session.MaxInTransit-100 && session.publishQueueIsFull {
				session.publishQueueIsFull = false
				session.publishQueueIsFullCond.Broadcast() // Inform all waiting publishers that we're no longer full
			}
			session.unconfirmedMutex.RUnlock()
			session.publishQueueIsFullCond.L.Unlock()

		case <-resendTimer:
			resendTimer = time.After(session.ReSendDelay)
			resendMessages()

		case <-session.reconnectChan:
			// When reconnectChan is closed, a reconnect attempt has started.
			// This means that we'll have to reset all deliverytags that we haven't
			// gotten confirmation for yet
			reconnected = true

		case <-session.done:
			// session is closed
			return
		}
	}
}

// drainPublish calls 'OnShutdown' once for each message in unconfirmedList
func (session *Session) drainPublish(ctx context.Context) {
	hasQueuedMessages := func() bool {
		session.unconfirmedMutex.RLock()
		defer session.unconfirmedMutex.RUnlock()

		// Note that we check both the length of unconfirmed messages, and the length of the
		// publishing channel, which can contain a lot of messages.
		// Since we're not going to read from publishChan, it does not matter that it's not valid on return
		return session.unconfirmedMessages.Len() > 0 || len(session.publishChan) > 0
	}

	// Drain all outgoing messages
	for {
		select {
		case <-time.After(50 * time.Millisecond):
			if !hasQueuedMessages() {
				return
			}
		case <-ctx.Done():
			return
		}
	}
}

// Push will push data onto the queue, and wait for a confirm.
// If no confirms are received until within the resendTimeout,
// it continuously re-sends messages until a confirm is received.
// This will block until the server sends a confirm. Errors are
// only returned if the push action itself fails, see UnsafePush.
//
// Note that in case of reconnecting while sending many messages,
// the messages might be sent twice.
// Make sure that all messages are idempotent.
func (session *Session) Push(exchangeName, routingKey string, publishing amqp.Publishing) error {

	msg := message{
		exchangeName: exchangeName,
		routingKey:   routingKey,
		data:         publishing,
	}

	if session.isInShutdown() {
		return ErrShutdown
	}

	for {
		// Wait until queue is not full
		session.publishQueueIsFullCond.L.Lock()
		if session.publishQueueIsFull {
			session.publishQueueIsFullCond.Wait()
		}
		session.publishQueueIsFullCond.L.Unlock()

		select {
		case session.publishChan <- &msg:
			return nil
		case <-time.After(500 * time.Millisecond):
			continue
		}
	}
}

// unsafePush will push to the queue without checking for
// confirmation. It returns an error if it fails to connect.
// No guarantees are provided for whether the server will
// receive the message.
func (session *Session) unsafePush(msg *message) error {
	if !session.IsReady() {
		return ErrNotConnected
	}

	// Make sure that channel is not being replaced
	session.channelMutex.RLock()
	defer session.channelMutex.RUnlock()

	return session.channel.Publish(
		msg.exchangeName, // Exchange
		msg.routingKey,   // Routing key
		false,            // Mandatory
		false,            // Immediate
		msg.data)
}
