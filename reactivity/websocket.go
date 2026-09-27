package reactivity

import (
	"encoding/json"
	"log/slog"
	"net/http"

	"github.com/gorilla/websocket"
	"github.com/recodeorg/tether/utilities"
)

// MessageReceiver receives decoded WebSocket payloads. Implemented by tether.Engine.

type WebsocketHelper struct {
	CheckOrigin func(r *http.Request) bool
}

func Handle(w http.ResponseWriter, r *http.Request, onReceiveMessage func(clientID string, msg map[string]interface{}) error, tracker *Tracker, helper *WebsocketHelper) {
	upgrader := websocket.Upgrader{
		CheckOrigin: helper.CheckOrigin,
	}
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		slog.Error("WS: Failed to upgrade to websocket", "error", err)
		return
	}

	client := NewClient(ws)
	tracker.Track(client)
	defer func() {
		tracker.Untrack(client)
		close(client.Send)
	}()
	go client.WritePump()
	ws.SetReadLimit(1024 * 8) // 8KB
	for {
		_, message, err := ws.ReadMessage()
		if err != nil {
			slog.Error("WS: Failed to read message", "error", err)
			return
		}
		var msg map[string]interface{}
		err = json.Unmarshal(message, &msg)
		if err != nil {
			slog.Error("WS: Failed to unmarshal message", "error", err)
			return
		}
		logged := utilities.RedactSensitiveFrame(msg)
		if raw, mErr := json.Marshal(logged); mErr == nil {
			slog.Debug("WS: Received message", "message", string(raw))
		}
		slog.Debug("WS: Unmarshalled message", "message", logged)
		err = onReceiveMessage(client.ID, msg)
		if err != nil {
			slog.Error("WS: Failed to on receive message", "error", err)
			continue
		}
	}
}
