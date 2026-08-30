package reactivity

import (
	"encoding/json"
	"log/slog"
	"net/http"

	"github.com/gorilla/websocket"
)

// MessageReceiver receives decoded WebSocket payloads. Implemented by tether.Engine.
type EngineHandler interface {
	OnConnect(clientID string) error
	OnDisconnect(clientID string) error
	OnReceiveMessage(clientID string, msg map[string]interface{}) error
}

type WebsocketHelper struct {
	CheckOrigin func(r *http.Request) bool
}

var upgrader = websocket.Upgrader{}

func Handle(w http.ResponseWriter, r *http.Request, e EngineHandler, tracker *Tracker, helper *WebsocketHelper) {
	ws, err := upgrader.Upgrade(w, r, nil)
	upgrader.CheckOrigin = helper.CheckOrigin
	if err != nil {
		slog.Error("WS: Failed to upgrade to websocket", "error", err)
		return
	}

	client := NewClient(ws)
	err = e.OnConnect(client.ID)
	if err != nil {
		slog.Error("WS: Failed to call onConnect handler", "error", err)
		return
	}
	defer e.OnDisconnect(client.ID)
	tracker.Track(client)
	defer func() {
		tracker.Untrack(client)
		close(client.Send)
	}()
	go client.WritePump()
	for {
		_, message, err := ws.ReadMessage()
		if err != nil {
			slog.Error("WS: Failed to read message", "error", err)
			return
		}
		slog.Debug("WS: Received message", "message", string(message))

		var msg map[string]interface{}
		err = json.Unmarshal(message, &msg)
		if err != nil {
			slog.Error("WS: Failed to unmarshal message", "error", err)
			return
		}
		slog.Debug("WS: Unmarshalled message", "message", msg)
		err = e.OnReceiveMessage(client.ID, msg)
		if err != nil {
			slog.Error("WS: Failed to on receive message", "error", err)
			continue
		}
	}
}
