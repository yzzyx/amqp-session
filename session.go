package session

import (
	"container/list"
	"errors"
	"log"
	"os"
	"sync"
	"time"

	"github.com/streadway/amqp"
)

type message struct {
	deliveryTag  uint64
	exchangeName string
	routingKey   string
	data         amqp.Publishing
}

// Session defines the base session type
type Session struct {
	Address string // Address of AMQP server

	ReconnectDelay time.Duration // Delay between reconnection attempts
	ReInitDelay    time.Duration // Delay between reinitialisation attempts
	ReSendDelay    time.Duration // Delay between attempts to redeliver messages
	MaxInTransit   int           // Maximum number of unacknowledged messages to publish

	// OnInit is called on successful connection, and can be used to
	// configure exchanges, queues, etc.
	OnInit func(conn *amqp.Connection, ch *amqp.Channel) error

	// OnShutdown is called when session is closed, once for each message still
	// in the publishing queue. If OnShutdown returns error, no further messages will be processed.
	// exchangeName and routeKey corresponds to the values used in the call to 'Push'
	OnShutdown func(exchangeName string, routeKey string, msg amqp.Publishing) error

	connection      *amqp.Connection
	connectionMutex sync.RWMutex
	channel         *amqp.Channel
	channelMutex    sync.RWMutex

	done            chan bool
	notifyConnClose chan *amqp.Error
	notifyChanClose chan *amqp.Error
	notifyConfirm   chan amqp.Confirmation
	reconnectChan   chan bool
	log             *log.Logger

	isReady      bool
	isReadyMutex sync.RWMutex

	isPublisher            bool
	publishChan            chan *message // channel messages are read from
	unconfirmedMessages    *list.List    // messages awaiting confirm
	unconfirmedCount       int           // number of messages currently awaiting confirm
	unconfirmedMutex       sync.RWMutex
	expectedDeliveryTag    uint64 // next expected delivery tag (incremented by one for each publishing)
	deliveryTagMutex       sync.Mutex
	publishQueueIsFull     bool
	publishQueueIsFullCond *sync.Cond
}

// Default settings used when initalising a new Session
const (
	DefaultReconnectDelay = 5 * time.Second
	DefaultReInitDelay    = 2 * time.Second
	DefaultResendDelay    = 5 * time.Second
	DefaultMaxInTransit   = 10000
)

// Errors used in package
var (
	ErrNotConnected  = errors.New("not connected to a server")
	ErrAlreadyClosed = errors.New("already closed: not connected to the server")
	ErrQueueTimeout  = errors.New("timeout while enqueuing message")
	ErrShutdown      = errors.New("session is shutting down")
)

// New creates a new consumer state instance, and automatically
// attempts to connect to the server.
func New() *Session {
	session := Session{
		done:           make(chan bool),
		reconnectChan:  make(chan bool),
		MaxInTransit:   DefaultMaxInTransit,
		ReconnectDelay: DefaultReconnectDelay,
		ReInitDelay:    DefaultReInitDelay,
		ReSendDelay:    DefaultResendDelay,
		log:            log.New(os.Stderr, "amqp-session", log.LstdFlags),
	}
	return &session
}

// SetLogger sets the logger interface for the current session
func (session *Session) SetLogger(logger *log.Logger) {
	session.log = logger
}

// Publisher marks this session as a publisher
func (session *Session) Publisher() {
	session.publishChan = make(chan *message, session.MaxInTransit)
	session.unconfirmedMessages = list.New()
	session.isPublisher = true
	session.publishQueueIsFullCond = sync.NewCond(&sync.Mutex{})
}

// ChannelAction allows an action to be performed on a channel like declaring a queue.
// After locks have been acquired, actionFunc will be called.
func (session *Session) ChannelAction(actionFunc func(channel *amqp.Channel) error) error {
	// Wait until session is ready
	for !session.IsReady() {
		time.Sleep(1 * time.Second)
	}

	session.channelMutex.Lock()
	defer session.channelMutex.Unlock()
	return actionFunc(session.channel)
}

// Start the session background handlers
// This must be called before consuming or publishing messages to the session.
// If the session should be used as a publisher, call session.Publisher() before Start().
func (session *Session) Start() {
	go session.handleReconnect()
	if session.isPublisher {
		go session.handlePublish()
	}
}

// handleReconnect will wait for a connection error on
// notifyConnClose, and then continuously attempt to reconnect.
func (session *Session) handleReconnect() {
	for {
		session.isReadyMutex.Lock()
		session.isReady = false
		session.isReadyMutex.Unlock()

		session.log.Println("Attempting to connect")

		conn, err := session.connect(session.Address)

		if err != nil {
			session.log.Println("Failed to connect. Retrying...")

			select {
			case <-session.done:
				return
			case <-time.After(session.ReconnectDelay):
			}
			continue
		}

		if done := session.handleReInit(conn); done {
			break
		}
	}
}

// connect will create a new AMQP connection
func (session *Session) connect(addr string) (*amqp.Connection, error) {
	conn, err := amqp.Dial(addr)

	if err != nil {
		return nil, err
	}

	session.changeConnection(conn)
	return conn, nil
}

// handleReconnect will wait for a channel error
// and then continuously attempt to re-initialize both channels
func (session *Session) handleReInit(conn *amqp.Connection) bool {
	for {
		session.isReadyMutex.Lock()
		session.isReady = false
		session.isReadyMutex.Unlock()

		prevChannel := session.reconnectChan
		session.reconnectChan = make(chan bool)
		close(prevChannel)

		err := session.init(conn)

		if err != nil {
			select {
			case <-session.done:
				return true
			case <-time.After(session.ReInitDelay):
			}
			continue
		}

		select {
		case <-session.done:
			return true
		case <-session.notifyConnClose:
			return false
		case <-session.notifyChanClose:
			// Run handleReInit() again
		}
	}
}

// init will initialize channel & declare queue
func (session *Session) init(conn *amqp.Connection) error {
	ch, err := conn.Channel()

	if err != nil {
		return err
	}

	err = ch.Confirm(false)
	if err != nil {
		return err
	}

	err = session.OnInit(conn, ch)
	if err != nil {
		return err
	}

	session.changeChannel(ch)

	session.isReadyMutex.Lock()
	session.isReady = true
	session.isReadyMutex.Unlock()

	return nil
}

// changeConnection takes a new connection to the queue,
// and updates the close listener to reflect this.
func (session *Session) changeConnection(connection *amqp.Connection) {
	session.connectionMutex.Lock()
	defer session.connectionMutex.Unlock()

	session.connection = connection
	session.notifyConnClose = make(chan *amqp.Error)
	session.connection.NotifyClose(session.notifyConnClose)
}

// changeChannel takes a new channel to the queue,
// and updates the channel listeners to reflect this.
func (session *Session) changeChannel(channel *amqp.Channel) {
	session.channelMutex.Lock()
	defer session.channelMutex.Unlock()

	session.channel = channel
	session.notifyChanClose = make(chan *amqp.Error)
	session.notifyConfirm = make(chan amqp.Confirmation, session.MaxInTransit)
	session.channel.NotifyClose(session.notifyChanClose)
	session.channel.NotifyPublish(session.notifyConfirm)
}

// IsReady returns true if the session is ready
func (session *Session) IsReady() bool {
	session.isReadyMutex.RLock()
	defer session.isReadyMutex.RUnlock()
	return session.isReady
}

// Close will cleanly shutdown the channel and connection.
func (session *Session) Close() error {
	if !session.isReady {
		return ErrAlreadyClosed
	}

	session.log.Println("Shutting down")

	// Cancel handlePublish
	close(session.done)

	if session.isPublisher && session.OnShutdown != nil {
		session.drainPublish()
	}

	session.channelMutex.Lock()
	defer session.channelMutex.Unlock()

	err := session.channel.Close()
	if err != nil {
		return err
	}

	session.connectionMutex.Lock()
	defer session.connectionMutex.Unlock()

	err = session.connection.Close()
	if err != nil {
		return err
	}

	session.isReadyMutex.Lock()
	defer session.isReadyMutex.Unlock()
	session.isReady = false
	return nil
}
