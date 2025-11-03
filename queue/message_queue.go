package queue

import (
	"sync"
	"time"

	"log/slog"
)

// EmailMessage represents an email message
type EmailMessage struct {
	To      string
	Subject string
	Body    string
}

// TelegramMessage represents a Telegram message
type TelegramMessage struct {
	ChatID string
	Text   string
}

// MessageQueue manages message queues with QPS limits
type MessageQueue struct {
	emailQueue    chan *EmailMessage
	telegramQueue chan *TelegramMessage

	emailQPS    int // Emails per second
	telegramQPS int // Telegram messages per second

	emailTicker    *time.Ticker
	telegramTicker *time.Ticker

	stopChan chan struct{}
	wg       sync.WaitGroup
	logger   *slog.Logger

	// Sender functions (should be set by the caller)
	EmailSender    func(*EmailMessage) error
	TelegramSender func(*TelegramMessage) error
}

// NewMessageQueue creates a new message queue
func NewMessageQueue(emailQPS, telegramQPS int) *MessageQueue {
	return &MessageQueue{
		emailQueue:    make(chan *EmailMessage, 10000),    // Buffer up to 10k emails
		telegramQueue: make(chan *TelegramMessage, 10000), // Buffer up to 10k telegram messages
		emailQPS:      emailQPS,
		telegramQPS:   telegramQPS,
		stopChan:      make(chan struct{}),
		logger:        slog.Default(),
	}
}

// SetLogger sets a custom logger
func (q *MessageQueue) SetLogger(logger *slog.Logger) {
	q.logger = logger
}

// SetEmailSender sets the email sender function
func (q *MessageQueue) SetEmailSender(sender func(*EmailMessage) error) {
	q.EmailSender = sender
}

// SetTelegramSender sets the Telegram sender function
func (q *MessageQueue) SetTelegramSender(sender func(*TelegramMessage) error) {
	q.TelegramSender = sender
}

// Start starts the message queue workers
func (q *MessageQueue) Start() {
	q.logger.Info("Starting message queue",
		"email_qps", q.emailQPS,
		"telegram_qps", q.telegramQPS)

	// Calculate ticker intervals based on QPS
	emailInterval := time.Second / time.Duration(q.emailQPS)
	telegramInterval := time.Second / time.Duration(q.telegramQPS)

	q.emailTicker = time.NewTicker(emailInterval)
	q.telegramTicker = time.NewTicker(telegramInterval)

	// Start email worker
	q.wg.Add(1)
	go q.emailWorker()

	// Start telegram worker
	q.wg.Add(1)
	go q.telegramWorker()
}

// Stop stops the message queue workers
func (q *MessageQueue) Stop() {
	q.logger.Info("Stopping message queue")
	close(q.stopChan)

	if q.emailTicker != nil {
		q.emailTicker.Stop()
	}
	if q.telegramTicker != nil {
		q.telegramTicker.Stop()
	}

	q.wg.Wait()
}

// PushEmail pushes an email message to the queue
func (q *MessageQueue) PushEmail(msg *EmailMessage) error {
	select {
	case q.emailQueue <- msg:
		q.logger.Debug("Email message queued", "to", msg.To)
		return nil
	default:
		q.logger.Warn("Email queue full, dropping message", "to", msg.To)
		return nil // Don't block if queue is full
	}
}

// PushTelegram pushes a Telegram message to the queue
func (q *MessageQueue) PushTelegram(msg *TelegramMessage) error {
	select {
	case q.telegramQueue <- msg:
		q.logger.Debug("Telegram message queued", "chat_id", msg.ChatID)
		return nil
	default:
		q.logger.Warn("Telegram queue full, dropping message", "chat_id", msg.ChatID)
		return nil // Don't block if queue is full
	}
}

// emailWorker processes email messages from the queue
func (q *MessageQueue) emailWorker() {
	defer q.wg.Done()

	for {
		select {
		case <-q.stopChan:
			return
		case <-q.emailTicker.C:
			// Try to send one email per tick
			select {
			case msg := <-q.emailQueue:
				if q.EmailSender != nil {
					if err := q.EmailSender(msg); err != nil {
						q.logger.Error("Failed to send email", "error", err, "to", msg.To)
						// Optionally: retry logic here
					} else {
						q.logger.Debug("Email sent successfully", "to", msg.To)
					}
				} else {
					q.logger.Warn("Email sender not set, dropping message", "to", msg.To)
				}
			default:
				// No message in queue, skip
			}
		}
	}
}

// telegramWorker processes Telegram messages from the queue
func (q *MessageQueue) telegramWorker() {
	defer q.wg.Done()

	for {
		select {
		case <-q.stopChan:
			return
		case <-q.telegramTicker.C:
			// Try to send one telegram message per tick
			select {
			case msg := <-q.telegramQueue:
				if q.TelegramSender != nil {
					if err := q.TelegramSender(msg); err != nil {
						q.logger.Error("Failed to send Telegram message", "error", err, "chat_id", msg.ChatID)
						// Optionally: retry logic here
					} else {
						q.logger.Debug("Telegram message sent successfully", "chat_id", msg.ChatID)
					}
				} else {
					q.logger.Warn("Telegram sender not set, dropping message", "chat_id", msg.ChatID)
				}
			default:
				// No message in queue, skip
			}
		}
	}
}

// GetQueueStats returns current queue statistics
func (q *MessageQueue) GetQueueStats() (emailQueueLen, telegramQueueLen int) {
	return len(q.emailQueue), len(q.telegramQueue)
}
