package wire

import "testing"

// TestSchemeHelpers covers the URL helpers the runtime uses instead of
// hand-rolled string prefixes.
func TestSchemeHelpers(t *testing.T) {
	if got, want := SchemeURL(SchemeTCP, "127.0.0.1:7731"), "tcp://127.0.0.1:7731"; got != want {
		t.Errorf("SchemeURL = %q, want %q", got, want)
	}
	if got, want := TCPPrefix, "tcp://"; got != want {
		t.Errorf("TCPPrefix = %q, want %q", got, want)
	}

	cases := []struct {
		url    string
		scheme string
		want   bool
	}{
		{"redis://127.0.0.1:6379", SchemeRedis, true},
		{"nats://127.0.0.1:4222", SchemeNATS, true},
		{"tcp://127.0.0.1:7731", SchemeTCP, true},
		{"redis://host", SchemeNATS, false},
		{"redis", SchemeRedis, false},
		{"", SchemeTCP, false},
	}
	for _, c := range cases {
		if got := HasScheme(c.url, c.scheme); got != c.want {
			t.Errorf("HasScheme(%q, %q) = %v, want %v", c.url, c.scheme, got, c.want)
		}
	}
}

// TestTopicPrefixesAreNamespaced guards the property every subscription relies
// on: all topics live under TopicRoot, so no user topic can shadow them.
func TestTopicPrefixesAreNamespaced(t *testing.T) {
	prefixes := []string{
		TopicPushPrefix, TopicPubPrefix, TopicResPrefix, TopicPipePrefix,
		TopicStreamPrefix, TopicCbPrefix, TopicHBPrefix, TopicControlPrefix,
		TopicControlExact, TopicBroadcast, CapInline,
	}
	for _, p := range prefixes {
		if len(p) < len(TopicRoot) || p[:len(TopicRoot)] != TopicRoot {
			t.Errorf("%q is not namespaced under %q", p, TopicRoot)
		}
	}
}
