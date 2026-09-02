package reactivity

import (
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

type Client struct {
	mu   sync.RWMutex
	ID   string
	Conn *websocket.Conn
	Send chan []byte
	Auth *AuthCtx
}

type AuthCtx struct {
	UserID    string
	ExpiresAt time.Time
}

func NewClient(conn *websocket.Conn) *Client {
	return &Client{ID: uuid.NewString(), Conn: conn, Send: make(chan []byte, 256), Auth: &AuthCtx{UserID: "", ExpiresAt: time.Time{}}}
}

func (c *Client) SetAuth(userID string, expiresAt time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.Auth.UserID = userID
	c.Auth.ExpiresAt = expiresAt
}

func (c *Client) GetAuth() AuthCtx {
	c.mu.RLock()
	defer c.mu.RUnlock()
	copy := *c.Auth
	return copy
}

func (c *Client) WritePump() {
	defer c.Conn.Close()

	for message := range c.Send {
		err := c.Conn.WriteMessage(websocket.TextMessage, message)
		if err != nil {
			slog.Error("Client write pump failed", "error", err, "client", c.ID)
			return
		}
	}
}
