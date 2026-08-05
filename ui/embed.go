// Package dashboardui embeds the production HOR-396 Dashboard in the API
// binary so the customer surface and durable work APIs share one origin.
package dashboardui

import (
	"embed"
	"io/fs"
	"net/http"
	"strings"
)

// dist is populated by npm run build. placeholder.txt keeps ordinary Go
// tooling compilable before the frontend build runs.
//
//go:embed dist/*
var dist embed.FS

// Handler serves immutable assets and the Dashboard document with restrictive
// browser policy. API authorization remains separate and bearer-key protected.
func Handler() http.Handler {
	root, err := fs.Sub(dist, "dist")
	if err != nil {
		panic(err)
	}
	files := http.FileServer(http.FS(root))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Referrer-Policy", "no-referrer")
		w.Header().Set("Content-Security-Policy", "default-src 'self'; connect-src 'self'; font-src 'self'; img-src 'self' data:; style-src 'self'; script-src 'self'; base-uri 'none'; frame-ancestors 'none'; form-action 'self'")
		if strings.HasPrefix(r.URL.Path, "/assets/") {
			w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
		} else {
			w.Header().Set("Cache-Control", "no-cache")
		}
		files.ServeHTTP(w, r)
	})
}
