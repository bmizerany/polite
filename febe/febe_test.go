package febe

import (
	"strings"
	"testing"

	"kr.dev/diff"
)

func TestWriteStartupMessage(t *testing.T) {
	var got strings.Builder
	c := New(nil, &got)

	if err := c.WriteStartupMessage("user", "testUser"); err != nil {
		t.Fatal(err)
	}

	want := "\x00\x00\x00\x16\x00\x03\x00\x00user\x00testUser\x00"
	if err := c.Flush(); err != nil {
		t.Fatal(err)
	}
	diff.Test(t, t.Errorf, got.String(), want)
}
