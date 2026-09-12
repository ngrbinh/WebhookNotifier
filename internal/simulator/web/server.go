// Package web serves the embedded simulator dashboard.
package web

import (
	"embed"
	"encoding/json"
	"net/http"
)

//go:embed index.html
var assets embed.FS

type SimulationStarter func(profile string, events, rate int) (string, error)

// Handler returns an HTTP handler for the simulator dashboard and API.
func Handler(start SimulationStarter) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/", http.FileServer(http.FS(assets)))
	mux.HandleFunc("/api/simulate", func(response http.ResponseWriter, request *http.Request) {
		var input struct {
			Profile string `json:"profile"`
			Events  int    `json:"events"`
			Rate    int    `json:"rate"`
		}
		if err := json.NewDecoder(request.Body).Decode(&input); err != nil {
			http.Error(response, err.Error(), 400)
			return
		}
		output, err := start(input.Profile, input.Events, input.Rate)
		if err != nil {
			http.Error(response, err.Error(), 500)
			return
		}
		response.Write([]byte(output))
	})
	return mux
}
