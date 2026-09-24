package main

import (
	"strings"
	"testing"
)

func TestParsePeerList(t *testing.T) {
	peers, err := parsePeerList("1=127.0.0.1:8001, 2=127.0.0.1:8002,3=h:3")
	if err != nil || len(peers) != 3 || peers[2] != "127.0.0.1:8002" {
		t.Fatalf("valid list: got %v, %v", peers, err)
	}
	for _, bad := range []string{"", "1", "x=a:1", "0=a:1", "1=a:1,1=b:2", "1="} {
		if _, err := parsePeerList(bad); err == nil {
			t.Errorf("parsePeerList(%q): expected an error", bad)
		}
	}
}

func TestParseConfig_RejectsNodeNotInPeers(t *testing.T) {
	_, err := parseConfig([]string{"--id=9"})
	if err == nil || !strings.Contains(err.Error(), "not listed") {
		t.Fatalf("expected an error for an ID missing from --peers, got %v", err)
	}
}
