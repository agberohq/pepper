package registry

import (
	"testing"
	"time"
)

// helpers

func pythonSpec(name string, groups ...string) *Spec {
	return &Spec{
		Name:    name,
		Runtime: RuntimePython,
		Source:  "./caps/" + name + ".py",
		Version: "1.0.0",
		Groups:  groups,
	}
}

func httpSpec(name string, groups ...string) *Spec {
	return &Spec{
		Name:    name,
		Runtime: RuntimeHTTP,
		Source:  "http://localhost:11434",
		Version: "0.0.0",
		Groups:  groups,
	}
}

// Runtime constants

func TestRuntimeValues(t *testing.T) {
	cases := []struct {
		rt   Runtime
		want string
	}{
		{RuntimePython, "python"},
		{RuntimeGo, "go"},
		{RuntimeHTTP, "http"},
		{RuntimeCLI, "cli"},
		{RuntimePipeline, "pipeline"},
	}
	for _, tc := range cases {
		if string(tc.rt) != tc.want {
			t.Errorf("Runtime = %q, want %q", tc.rt, tc.want)
		}
	}
}

// Add / Get

func TestAddAndGet(t *testing.T) {
	r := New()
	spec := pythonSpec("echo", "default")
	if err := r.Add(spec); err != nil {
		t.Fatalf("Add: %v", err)
	}
	got := r.Get("echo")
	if got == nil {
		t.Fatal("Get returned nil")
	}
	if got.Name != "echo" {
		t.Errorf("Name = %q, want echo", got.Name)
	}
	if got.Runtime != RuntimePython {
		t.Errorf("Runtime = %q, want python", got.Runtime)
	}
}

func TestGetMissingReturnsNil(t *testing.T) {
	r := New()
	if r.Get("nonexistent") != nil {
		t.Error("Get should return nil for unknown capability")
	}
}

func TestAddDuplicateReturnsError(t *testing.T) {
	r := New()
	r.Add(pythonSpec("dup", "default"))
	if err := r.Add(pythonSpec("dup", "default")); err == nil {
		t.Error("expected error on duplicate Add")
	}
}

// All

func TestAllEmpty(t *testing.T) {
	if len(New().All()) != 0 {
		t.Error("All on empty registry should return empty slice")
	}
}

func TestAllReturnsAllSpecs(t *testing.T) {
	r := New()
	r.Add(pythonSpec("cap-a", "default"))
	r.Add(pythonSpec("cap-b", "gpu"))
	r.Add(httpSpec("llm.generate", "gpu"))
	if len(r.All()) != 3 {
		t.Errorf("All returned %d specs, want 3", len(r.All()))
	}
}

// ForGroup

func TestForGroup(t *testing.T) {
	r := New()
	r.Add(pythonSpec("cap-gpu-1", "gpu"))
	r.Add(pythonSpec("cap-gpu-2", "gpu", "asr"))
	r.Add(pythonSpec("cap-cpu", "cpu"))
	r.Add(httpSpec("llm", "gpu"))

	if n := len(r.ForGroup("gpu")); n != 3 {
		t.Errorf("ForGroup(gpu) = %d, want 3", n)
	}
	if n := len(r.ForGroup("cpu")); n != 1 {
		t.Errorf("ForGroup(cpu) = %d, want 1", n)
	}
	if n := len(r.ForGroup("asr")); n != 1 {
		t.Errorf("ForGroup(asr) = %d, want 1", n)
	}
}

func TestForGroupEmpty(t *testing.T) {
	r := New()
	r.Add(pythonSpec("cap", "gpu"))
	if caps := r.ForGroup("nonexistent"); len(caps) != 0 {
		t.Errorf("ForGroup(nonexistent) = %v, want empty", caps)
	}
}

// ForRuntime

func TestForRuntime(t *testing.T) {
	r := New()
	r.Add(pythonSpec("py-1", "default"))
	r.Add(pythonSpec("py-2", "gpu"))
	r.Add(httpSpec("http-1", "gpu"))

	if n := len(r.ForRuntime(RuntimePython)); n != 2 {
		t.Errorf("ForRuntime(python) = %d, want 2", n)
	}
	if n := len(r.ForRuntime(RuntimeHTTP)); n != 1 {
		t.Errorf("ForRuntime(http) = %d, want 1", n)
	}
	if n := len(r.ForRuntime(RuntimeGo)); n != 0 {
		t.Errorf("ForRuntime(go) = %d, want 0", n)
	}
}

// Schema / Schemas

func TestSchemaFields(t *testing.T) {
	r := New()
	r.Add(&Spec{
		Name:    "face.recognize",
		Runtime: RuntimePython,
		Version: "2.0.0",
		Groups:  []string{"gpu"},
		InputSchema: map[string]any{
			"type": "object",
		},
	})

	s, err := r.Schema("face.recognize")
	if err != nil {
		t.Fatalf("Schema: %v", err)
	}
	if s.Name != "face.recognize" {
		t.Errorf("Name = %q", s.Name)
	}
	if s.Version != "2.0.0" {
		t.Errorf("Version = %q", s.Version)
	}
	if s.Runtime != "python" {
		t.Errorf("Runtime = %q", s.Runtime)
	}
	if len(s.Groups) != 1 || s.Groups[0] != "gpu" {
		t.Errorf("Groups = %v", s.Groups)
	}
	if s.InputSchema == nil {
		t.Error("InputSchema should be non-nil")
	}
}

func TestSchemaNotFound(t *testing.T) {
	r := New()
	if _, err := r.Schema("missing"); err == nil {
		t.Error("expected error for missing capability")
	}
}

func TestSchemasNoFilters(t *testing.T) {
	r := New()
	r.Add(pythonSpec("a", "default"))
	r.Add(pythonSpec("b", "gpu"))
	r.Add(httpSpec("c", "gpu"))
	if n := len(r.Schemas()); n != 3 {
		t.Errorf("Schemas() = %d, want 3", n)
	}
}

// FilterByGroup

func TestFilterByGroup(t *testing.T) {
	r := New()
	r.Add(pythonSpec("gpu-cap", "gpu"))
	r.Add(pythonSpec("cpu-cap", "cpu"))
	r.Add(pythonSpec("multi-cap", "gpu", "asr"))

	if n := len(r.Schemas(FilterByGroup("gpu"))); n != 2 {
		t.Errorf("FilterByGroup(gpu) = %d, want 2", n)
	}
}

func TestFilterByGroupMultiple(t *testing.T) {
	r := New()
	r.Add(pythonSpec("gpu-cap", "gpu"))
	r.Add(pythonSpec("cpu-cap", "cpu"))
	r.Add(httpSpec("http-cap", "tools"))

	if n := len(r.Schemas(FilterByGroup("gpu", "tools"))); n != 2 {
		t.Errorf("FilterByGroup(gpu,tools) = %d, want 2", n)
	}
}

// FilterByRuntime

func TestFilterByRuntime(t *testing.T) {
	r := New()
	r.Add(pythonSpec("py-cap", "default"))
	r.Add(httpSpec("http-cap", "default"))

	schemas := r.Schemas(FilterByRuntime(RuntimeHTTP))
	if len(schemas) != 1 {
		t.Errorf("FilterByRuntime(http) = %d, want 1", len(schemas))
	}
	if schemas[0].Runtime != "http" {
		t.Errorf("Runtime = %q, want http", schemas[0].Runtime)
	}
}

// Compound filters (AND)

func TestMultipleFiltersAreANDed(t *testing.T) {
	r := New()
	r.Add(pythonSpec("py-gpu", "gpu"))
	r.Add(httpSpec("http-gpu", "gpu"))
	r.Add(pythonSpec("py-cpu", "cpu"))

	schemas := r.Schemas(FilterByGroup("gpu"), FilterByRuntime(RuntimePython))
	if len(schemas) != 1 {
		t.Errorf("AND filters = %d, want 1", len(schemas))
	}
	if schemas[0].Name != "py-gpu" {
		t.Errorf("Name = %q, want py-gpu", schemas[0].Name)
	}
}

// DirNameToCap

func TestDirNameToCap(t *testing.T) {
	// Expected outputs derived from the actual implementation:
	// - strips ".", "..", "caps", and empty components
	// - joins remaining directory parts + filename with "."
	cases := []struct {
		path string
		want string
	}{
		{"./caps/face/recognize.py", "face.recognize"},
		{"./caps/speech/transcribe.py", "speech.transcribe"},
		{"./caps/echo.py", "echo"},
		{"caps/audio/denoise.py", "audio.denoise"},
		{"caps/echo.py", "echo"},
		{"./recognize.py", "recognize"},
		// Absolute path: leading empty + non-"caps" components are kept
		{"/abs/path/caps/doc/parse.py", "abs.path.doc.parse"},
		// Nested under a non-caps dir
		{"./runtime/python/caps/echo.py", "runtime.python.echo"},
	}
	for _, tc := range cases {
		got := DirNameToCap(tc.path)
		if got != tc.want {
			t.Errorf("DirNameToCap(%q) = %q, want %q", tc.path, got, tc.want)
		}
	}
}

// Spec field coverage

func TestSpecAllFields(t *testing.T) {
	spec := &Spec{
		Name:           "speech.transcribe",
		Version:        "1.0.0",
		Runtime:        RuntimePython,
		Source:         "./caps/speech/transcribe.py",
		Deps:           []string{"faster-whisper>=1.0"},
		Timeout:        60 * time.Second,
		MaxConcurrent:  4,
		Groups:         []string{"gpu", "asr"},
		PipePublishes:  []string{"pipeline:transcript"},
		PipeSubscribes: []string{"pipeline:raw-audio"},
		Config:         map[string]any{"model_size": "small"},
		InputSchema:    map[string]any{"type": "object"},
		OutputSchema:   map[string]any{"type": "object"},
	}

	r := New()
	if err := r.Add(spec); err != nil {
		t.Fatalf("Add: %v", err)
	}

	got := r.Get("speech.transcribe")
	if got.Timeout != 60*time.Second {
		t.Errorf("Timeout = %v, want 60s", got.Timeout)
	}
	if got.MaxConcurrent != 4 {
		t.Errorf("MaxConcurrent = %d, want 4", got.MaxConcurrent)
	}
	if len(got.Deps) != 1 || got.Deps[0] != "faster-whisper>=1.0" {
		t.Errorf("Deps = %v", got.Deps)
	}
	if got.Config["model_size"] != "small" {
		t.Errorf("Config model_size = %v", got.Config["model_size"])
	}
}

// Thread safety

func TestConcurrentReads(t *testing.T) {
	r := New()
	for i := 0; i < 10; i++ {
		r.Add(pythonSpec("cap-"+string(rune('a'+i)), "default"))
	}

	done := make(chan struct{}, 20)
	for i := 0; i < 20; i++ {
		go func() {
			r.All()
			r.ForGroup("default")
			r.Schemas()
			done <- struct{}{}
		}()
	}
	for i := 0; i < 20; i++ {
		<-done
	}
}
