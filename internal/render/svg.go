// Package render produces a standalone SVG with no scripts or remote assets.
package render

import (
	"fmt"
	"html"
	"io"
	"strings"

	"github.com/andrii2g/chandy-lamport-snapshot/internal/sim"
	"github.com/andrii2g/chandy-lamport-snapshot/internal/snapshot"
	"github.com/andrii2g/chandy-lamport-snapshot/internal/verify"
)

// SVG uses event-order rows so equal-tick sends and receives remain legible.
// Tick labels retain the actual simulated times; vertical spacing is not duration.
func SVG(w io.Writer, run sim.Run, report verify.Report) error {
	var b strings.Builder
	panelWidth := 240 + 180*(len(run.Config.Processes)-1)
	width := 60 + len(run.Snapshots)*(panelWidth+40)
	height := 240 + len(run.Trace)*32
	f := func(format string, args ...any) { fmt.Fprintf(&b, format, args...) }
	f(`<svg xmlns="http://www.w3.org/2000/svg" width="%d" height="%d" viewBox="0 0 %d %d" role="img" aria-labelledby="title desc">`, width, height, width, height)
	f(`<title id="title">%s — distributed snapshot comparison</title><desc id="desc">Processes are vertical lanes. Time flows downward in event order, with actual ticks labeled. Solid arrows are transfers, dashed arrows are markers, purple paths are cuts, amber arrows are recorded channel state, and red arrows violate causality.</desc>`, html.EscapeString(run.Config.Name))
	f(`<defs><marker id="arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M 0 0 L 10 5 L 0 10 z" fill="context-stroke"/></marker></defs>`)
	f(`<style>text{font-family:ui-monospace,Consolas,monospace;fill:#dbe6f6;font-size:11px}.heading{font-size:20px;font-weight:700}.label{font-size:13px;font-weight:600}.tick{fill:#8797af;font-size:10px}.msg{paint-order:stroke;stroke:#101b2d;stroke-width:4px;stroke-linejoin:round}</style><rect width="100%%" height="100%%" fill="#101b2d"/>`)
	f(`<text x="30" y="32" class="heading">%s</text><text x="30" y="56">Event order ↓ · ticks shown at left · vertical distance is not elapsed time</text>`, html.EscapeString(run.Config.Name))
	f(`<text x="30" y="80">Solid: transfer · dashed: marker · purple: cut · amber: captured in flight · red: causal violation</text>`)
	y := func(index int) int { return 165 + index*32 }
	receives := map[string]sim.Event{}
	for _, v := range run.Trace {
		if v.Kind == "receive" || v.Kind == "marker_receive" {
			receives[v.MessageID] = v
		}
	}
	for panel, s := range run.Snapshots {
		x0 := 30 + panel*(panelWidth+40)
		xs := map[string]int{}
		check := report.Checks[panel]
		f(`<text x="%d" y="113" class="heading">%s</text>`, x0, html.EscapeString(s.Algorithm))
		f(`<text x="%d" y="135">total %s/%s · cut %t · channels %t · latency %d</text>`, x0, check.RecordedTotal, check.InitialTotal, check.ConsistentCut, check.ExactChannels, check.Latency)
		for i, p := range run.Config.Processes {
			x := x0 + 65 + i*180
			xs[p.ID] = x
			f(`<text x="%d" y="157" text-anchor="middle" class="label">%s (%d)</text><line x1="%d" y1="173" x2="%d" y2="%d" stroke="#364760" stroke-width="2"/>`, x, html.EscapeString(p.ID), p.Balance, x, x, height-40)
		}
		for _, v := range run.Trace {
			f(`<text x="%d" y="%d" class="tick">%d</text>`, x0, y(v.Index)+3, v.Tick)
		}
		captured := map[string]bool{}
		for _, messages := range s.Channels {
			for _, m := range messages {
				captured[m.ID] = true
			}
		}
		for _, v := range run.Trace {
			if v.Kind != "send" && v.Kind != "marker_send" {
				continue
			}
			r, ok := receives[v.MessageID]
			if !ok {
				continue
			}
			color := "#91a9c9"
			dash := ""
			label := fmt.Sprintf("%s: %d", v.MessageID, v.Amount)
			if v.Kind == "marker_send" {
				color = "#58cbb7"
				dash = ` stroke-dasharray="6 5"`
				label = v.MessageID
			}
			if captured[v.MessageID] {
				color = "#ffc36b"
			}
			if v.Kind == "send" && included(s, r.To, r.Index) && !included(s, v.From, v.Index) {
				color = "#ff7885"
			}
			x1, x2, y1, y2 := xs[v.From], xs[v.To], y(v.Index), y(r.Index)
			// Each send owns its own row, avoiding midpoint labels colliding
			// with other arrows or the saved-state badges.
			labelX, anchor := x1+12, "start"
			if x2 < x1 {
				labelX, anchor = x1-12, "end"
			}
			f(`<g><title>%s → %s; sent tick %d, received tick %d</title><line x1="%d" y1="%d" x2="%d" y2="%d" stroke="%s" stroke-width="2"%s marker-end="url(#arrow)"/><circle cx="%d" cy="%d" r="3" fill="%s"/><text x="%d" y="%d" text-anchor="%s" class="msg">%s</text></g>`, html.EscapeString(v.From), html.EscapeString(v.To), v.Tick, r.Tick, x1, y1, x2, y2, color, dash, x1, y1, color, labelX, y1-8, anchor, html.EscapeString(label))
		}
		var points []string
		for _, p := range run.Config.Processes {
			if local, ok := s.Locals[p.ID]; ok {
				points = append(points, fmt.Sprintf("%d,%d", xs[p.ID], y(local.Cut)))
			}
		}
		f(`<polyline points="%s" fill="none" stroke="#c89bff" stroke-width="3" stroke-dasharray="10 4" opacity="0.85"/>`, strings.Join(points, " "))
		for _, p := range run.Config.Processes {
			if local, ok := s.Locals[p.ID]; ok {
				x, cy := xs[p.ID], y(local.Cut)
				f(`<circle cx="%d" cy="%d" r="7" fill="#c89bff"/><rect x="%d" y="%d" width="110" height="21" rx="4" fill="#3f2b59"/><text x="%d" y="%d">saved %d</text>`, x, cy, x+10, cy-11, x+15, cy+4, local.Balance)
			}
		}
		f(`<text x="%d" y="%d">Channel state: %d messages / %s tokens</text>`, x0, height-14, check.ChannelEntries, check.ChannelAmount)
	}
	f(`</svg>`)
	_, err := io.WriteString(w, b.String())
	return err
}

func included(s snapshot.Result, process string, index int) bool {
	l, ok := s.Locals[process]
	return ok && index < l.Cut
}
