package render_test

import (
	"bytes"
	"encoding/xml"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/andrii2g/chandy-lamport-snapshot/internal/render"
	"github.com/andrii2g/chandy-lamport-snapshot/internal/sim"
	"github.com/andrii2g/chandy-lamport-snapshot/internal/verify"
)

func TestSVGIsStandaloneValidXML(t *testing.T) {
	f, err := os.Open("../../scenarios/marker-window.json")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	c, err := sim.Decode(f)
	if err != nil {
		t.Fatal(err)
	}
	c.Name = `<script>alert("x")</script> & example`
	r, err := sim.Execute(c, "both")
	if err != nil {
		t.Fatal(err)
	}
	report := verify.Analyze(r)
	var b bytes.Buffer
	if err := render.SVG(&b, r, report); err != nil {
		t.Fatal(err)
	}
	d := xml.NewDecoder(bytes.NewReader(b.Bytes()))
	cuts := 0
	for {
		token, err := d.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if start, ok := token.(xml.StartElement); ok {
			if start.Name.Local == "script" {
				t.Fatal("unescaped user input")
			}
			if start.Name.Local == "polyline" {
				cuts++
			}
		}
	}
	if cuts != 2 || !strings.Contains(b.String(), "saved 90") || !strings.Contains(b.String(), "#ffc36b") {
		t.Fatal("diagram missing cuts, balances, or captured transfer")
	}
	var again bytes.Buffer
	if err := render.SVG(&again, r, report); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(b.Bytes(), again.Bytes()) {
		t.Fatal("nondeterministic SVG")
	}
}
