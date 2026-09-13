// Package queue provides durable RabbitMQ publishing and consumption helpers.
package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"webhooknotifier/internal/model"

	"github.com/rabbitmq/amqp091-go"
)

type Client struct {
	Connection *amqp091.Connection
	Channel    *amqp091.Channel
	QueueName  string
	DLQName    string
}

// New connects to RabbitMQ, declares the event queues, and returns a client.
// It retries transient startup failures until the context is canceled.
func New(ctx context.Context, url, queueName, dlqName string) (*Client, error) {
	const retryInterval = time.Second * 5
	var lastError error
	for {
		client, err := connect(ctx, url, queueName, dlqName)
		if err == nil {
			return client, nil
		}
		lastError = err
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("connect to RabbitMQ: %w (last error: %v)", ctx.Err(), lastError)
		case <-time.After(retryInterval):
		}
	}
}

func connect(ctx context.Context, url, queueName, dlqName string) (*Client, error) {
	connection, err := amqp091.Dial(url)
	if err != nil {
		return nil, err
	}
	channel, err := connection.Channel()
	if err != nil {
		connection.Close()
		return nil, err
	}
	client := &Client{Connection: connection, Channel: channel, QueueName: queueName, DLQName: dlqName}
	if err := client.Declare(); err != nil {
		client.Close()
		return nil, err
	}
	return client, nil
}

// Declare creates the durable event and dead-letter queues when they do not exist.
func (client *Client) Declare() error {
	if _, err := client.Channel.QueueDeclare(client.QueueName, true, false, false, false, nil); err != nil {
		return err
	}
	_, err := client.Channel.QueueDeclare(client.DLQName, true, false, false, false, nil)
	return err
}

// Publish serializes an event and publishes it durably to the main queue.
func (client *Client) Publish(ctx context.Context, event model.Event) error {
	body, err := json.Marshal(event)
	if err != nil {
		return err
	}
	return client.Channel.PublishWithContext(ctx, "", client.QueueName, false, false, amqp091.Publishing{ContentType: "application/json", DeliveryMode: amqp091.Persistent, MessageId: event.ID, Body: body, Timestamp: time.Now()})
}

// PublishDeadLetter serializes an event and its failure reason to the dead-letter queue.
func (client *Client) PublishDeadLetter(ctx context.Context, event model.Event, reason string) error {
	body, err := json.Marshal(map[string]any{"event": event, "reason": reason})
	if err != nil {
		return err
	}
	return client.Channel.PublishWithContext(ctx, "", client.DLQName, false, false, amqp091.Publishing{ContentType: "application/json", DeliveryMode: amqp091.Persistent, MessageId: event.ID, Body: body})
}

// Depth returns the number of queued messages and active consumers.
func (client *Client) Depth() (int, int, error) {
	state, err := client.Channel.QueueInspect(client.QueueName)
	if err != nil {
		return 0, 0, fmt.Errorf("inspect queue: %w", err)
	}
	return state.Messages, state.Consumers, nil
}

// Consume starts consuming messages with the requested prefetch concurrency.
func (client *Client) Consume(ctx context.Context, concurrency int) (<-chan amqp091.Delivery, error) {
	if err := client.Channel.Qos(concurrency, 0, false); err != nil {
		return nil, err
	}
	return client.Channel.ConsumeWithContext(ctx, client.QueueName, "", false, false, false, false, nil)
}

// Close closes the RabbitMQ channel and connection.
func (client *Client) Close() { client.Channel.Close(); client.Connection.Close() }
