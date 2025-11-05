package email

import (
	"fmt"
	"log/slog"
	"net/smtp"
	"strings"
)

// SMTPConfig holds SMTP configuration
type SMTPConfig struct {
	Host     string
	Port     int
	Username string
	Password string
	From     string
	FromName string
	Logger   *slog.Logger
}

// SMTPClient handles SMTP email sending
type SMTPClient struct {
	config *SMTPConfig
}

// NewSMTPClient creates a new SMTP client
func NewSMTPClient(config *SMTPConfig) *SMTPClient {
	if config.Logger == nil {
		config.Logger = slog.Default()
	}
	return &SMTPClient{
		config: config,
	}
}

// SendEmail sends an email using SMTP (deprecated, use SendEmailWithTLS instead)
func (c *SMTPClient) SendEmail(to, subject, body string) error {
	return c.SendEmailWithTLS(to, subject, body)
}

// SendEmailWithTLS sends an email using SMTP with TLS
// Uses smtp.SendMail which handles TLS automatically for ports 587 (STARTTLS) and 465 (SSL)
func (c *SMTPClient) SendEmailWithTLS(to, subject, body string) error {
	// Validate configuration
	if c.config.Host == "" {
		return fmt.Errorf("SMTP host is not configured")
	}
	if c.config.From == "" {
		return fmt.Errorf("SMTP from address is not configured")
	}
	if c.config.Port <= 0 {
		return fmt.Errorf("SMTP port is not configured")
	}

	// Build email message with proper headers
	fromHeader := c.config.From
	if c.config.FromName != "" {
		fromHeader = fmt.Sprintf("%s <%s>", c.config.FromName, c.config.From)
	}

	// Build message headers and body
	msg := []byte(fmt.Sprintf("From: %s\r\n", fromHeader) +
		fmt.Sprintf("To: %s\r\n", to) +
		fmt.Sprintf("Subject: %s\r\n", subject) +
		"MIME-Version: 1.0\r\n" +
		"Content-Type: text/html; charset=UTF-8\r\n" +
		"\r\n" +
		body + "\r\n")

	// Set up authentication (use empty string if no username/password)
	var auth smtp.Auth
	if c.config.Username != "" && c.config.Password != "" {
		auth = smtp.PlainAuth("", c.config.Username, c.config.Password, c.config.Host)
	}

	// Parse recipients (handle multiple recipients separated by comma)
	recipients := []string{}
	for _, recipient := range strings.Split(to, ",") {
		recipient = strings.TrimSpace(recipient)
		if recipient != "" {
			recipients = append(recipients, recipient)
		}
	}

	if len(recipients) == 0 {
		return fmt.Errorf("no valid recipients")
	}

	// Connect to SMTP server
	addr := fmt.Sprintf("%s:%d", c.config.Host, c.config.Port)

	// Use smtp.SendMail which handles TLS automatically:
	// - Port 587: Uses STARTTLS
	// - Port 465: Uses SSL/TLS directly
	// - Other ports: Plain connection (not recommended)
	err := smtp.SendMail(addr, auth, c.config.From, recipients, msg)
	if err != nil {
		return fmt.Errorf("failed to send email: %w", err)
	}

	c.config.Logger.Debug("Email sent successfully", "to", to, "subject", subject)
	return nil
}
