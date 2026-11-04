package hiscores

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"
)

// A fake hiscores server: answers per table path, so the tests pin which URL each mode reads.
func fakeServer(t *testing.T, handler func(path, player string) (int, string)) *Client {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		code, body := handler(r.URL.Path, r.URL.Query().Get("player"))
		w.WriteHeader(code)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	c := NewClient("test")
	c.BaseURL = srv.URL + "/m=hiscore_oldschool"
	c.RetryDelay = time.Millisecond
	return c
}

func TestFetchReturnsTheRawBodyFromEachTable(t *testing.T) {
	fixture, err := os.ReadFile("testdata/lynx_titan.json")
	if err != nil {
		t.Fatal(err)
	}
	var paths []string
	c := fakeServer(t, func(path, player string) (int, string) {
		paths = append(paths, path)
		if player != "Lynx Titan" {
			t.Errorf("player = %q", player)
		}
		return 200, string(fixture)
	})

	for _, mode := range []Mode{
		ModeMain, ModeIronman, ModeHardcore, ModeUltimate,
		ModeHardcoreMains, ModeSeasonal, ModeDeadman, ModeFreshStart, ModeTournament,
	} {
		res := c.Fetch(context.Background(), "Lynx Titan", mode)
		if res.Outcome != OutcomeOK {
			t.Fatalf("%s: outcome %s (%v)", mode, res.Outcome, res.Err)
		}
		if string(res.Body) != string(fixture) {
			t.Fatalf("%s: body was altered", mode)
		}
	}
	want := []string{
		"/m=hiscore_oldschool/index_lite.json",
		"/m=hiscore_oldschool_ironman/index_lite.json",
		"/m=hiscore_oldschool_hardcore_ironman/index_lite.json",
		"/m=hiscore_oldschool_ultimate/index_lite.json",
		"/m=hiscore_oldschool_hardcore_mains/index_lite.json",
		"/m=hiscore_oldschool_seasonal/index_lite.json",
		"/m=hiscore_oldschool_deadman/index_lite.json",
		"/m=hiscore_oldschool_fresh_start/index_lite.json",
		"/m=hiscore_oldschool_tournament/index_lite.json",
	}
	if strings.Join(paths, ",") != strings.Join(want, ",") {
		t.Fatalf("paths = %v", paths)
	}
}

func TestFetchClassifiesFailures(t *testing.T) {
	calls := 0
	c := fakeServer(t, func(_, player string) (int, string) {
		calls++
		switch player {
		case "Gone":
			return 404, ""
		case "Html":
			return 200, "<html>busy</html>"
		default:
			return 503, ""
		}
	})

	if r := c.Fetch(context.Background(), "Gone", ModeMain); r.Outcome != OutcomeUnranked {
		t.Fatalf("404: %s", r.Outcome)
	}
	if calls != 1 {
		t.Fatalf("a 404 must not be retried; calls = %d", calls)
	}
	if r := c.Fetch(context.Background(), "Html", ModeMain); r.Outcome != OutcomeTransient {
		t.Fatalf("html body: %s", r.Outcome)
	}
	if r := c.Fetch(context.Background(), "Down", ModeMain); r.Outcome != OutcomeTransient {
		t.Fatalf("503: %s", r.Outcome)
	}
	before := calls
	if r := c.Fetch(context.Background(), "Down", Mode("grid_master")); r.Outcome != OutcomeUnranked {
		t.Fatalf("unknown mode: %s", r.Outcome)
	}
	if calls != before {
		t.Fatalf("an unknown mode must not reach the network; calls %d -> %d", before, calls)
	}
	if r := c.Fetch(context.Background(), "this name is far too long", ModeMain); r.Outcome != OutcomeUnranked {
		t.Fatalf("invalid rsn: %s", r.Outcome)
	}
}

// The Site's two "hardcore" tables must never be crossed: "hardcore" is the Hardcore Ironman ladder
// the account-type probe reads, "hardcore_mains" is the Hardcore Worlds save.
func TestHardcoreModesAreDistinctTables(t *testing.T) {
	if tableSuffix[ModeHardcore] != "_hardcore_ironman" {
		t.Fatalf("hardcore = %q", tableSuffix[ModeHardcore])
	}
	if tableSuffix[ModeHardcoreMains] != "_hardcore_mains" {
		t.Fatalf("hardcore_mains = %q", tableSuffix[ModeHardcoreMains])
	}
}

func TestKnown(t *testing.T) {
	for _, m := range []Mode{"main", "ironman", "hardcore", "ultimate", "hardcore_mains", "seasonal", "deadman", "fresh_start", "tournament"} {
		if !Known(m) {
			t.Errorf("%s should be known", m)
		}
	}
	for _, m := range []Mode{"", "beta", "grid-master", "seasonal-deadman", "leagues", "Main"} {
		if Known(m) {
			t.Errorf("%q should not be known", m)
		}
	}
}

// Every suffix is distinct: two modes on one table would mean one of them reads the wrong ladder.
func TestTableSuffixesAreUnique(t *testing.T) {
	seen := map[string]Mode{}
	for m, s := range tableSuffix {
		if prev, ok := seen[s]; ok {
			t.Fatalf("%s and %s share suffix %q", prev, m, s)
		}
		seen[s] = m
	}
}
