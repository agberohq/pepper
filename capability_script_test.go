package pepper

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/agberohq/pepper/internal/registry"
)

// TestRuntimeFromExt verifies that runtimeFromExt maps extensions correctly.
func TestRuntimeFromExt(t *testing.T) {
	cases := []struct {
		path    string
		want    registry.Runtime
		wantErr bool
	}{
		{"./caps/echo.py", registry.RuntimePython, false},
		{"./caps/embed.jl", registry.RuntimeJulia, false},
		{"./caps/search.js", registry.RuntimeNode, false},
		{"./caps/tool.mjs", registry.RuntimeNode, false},
		{"CAPS/UPPER.PY", registry.RuntimePython, false}, // case-insensitive
		{"./caps/model.JL", registry.RuntimeJulia, false},
		{"./caps/unknown.rb", "", true},
		{"./caps/noext", "", true},
	}
	for _, tc := range cases {
		rt, err := runtimeFromExt(tc.path)
		if tc.wantErr {
			if err == nil {
				t.Errorf("runtimeFromExt(%q): expected error, got runtime=%q", tc.path, rt)
			}
			continue
		}
		if err != nil {
			t.Errorf("runtimeFromExt(%q): unexpected error: %v", tc.path, err)
			continue
		}
		if rt != tc.want {
			t.Errorf("runtimeFromExt(%q) = %q, want %q", tc.path, rt, tc.want)
		}
	}
}

// TestIsScriptFile verifies the include/exclude logic for Dir walking.
func TestIsScriptFile(t *testing.T) {
	include := []string{"echo.py", "embed.jl", "search.js", "tool.mjs", "caps.PY"}
	exclude := []string{
		"runtime.py", "runtime.jl", "runtime.js",
		"cap.py", "cap.jl", "cap.js",
		"transport.jl", "transport.js",
		"README.md", "config.json", "Makefile",
		".hidden.py",
	}
	for _, name := range include {
		if !isScriptFile(name) {
			t.Errorf("isScriptFile(%q) = false, want true", name)
		}
	}
	for _, name := range exclude {
		if isScriptFile(name) {
			t.Errorf("isScriptFile(%q) = true, want false", name)
		}
	}
}

// TestScriptBuildSpecs_Python verifies that .py files produce RuntimePython specs.
func TestScriptBuildSpecs_Python(t *testing.T) {
	specs, err := Script("echo", "./caps/echo.py").buildSpecs()
	if err != nil {
		t.Fatalf("buildSpecs: %v", err)
	}
	if len(specs) != 1 {
		t.Fatalf("len(specs) = %d, want 1", len(specs))
	}
	if specs[0].Runtime != registry.RuntimePython {
		t.Errorf("Runtime = %q, want python", specs[0].Runtime)
	}
	if specs[0].Name != "echo" {
		t.Errorf("Name = %q, want echo", specs[0].Name)
	}
}

// TestScriptBuildSpecs_Julia verifies that .jl files produce RuntimeJulia specs.
func TestScriptBuildSpecs_Julia(t *testing.T) {
	specs, err := Script("embed.text", "./caps/embed.jl").buildSpecs()
	if err != nil {
		t.Fatalf("buildSpecs: %v", err)
	}
	if specs[0].Runtime != registry.RuntimeJulia {
		t.Errorf("Runtime = %q, want julia", specs[0].Runtime)
	}
}

// TestScriptBuildSpecs_Node verifies that .js files produce RuntimeNode specs.
func TestScriptBuildSpecs_Node(t *testing.T) {
	for _, ext := range []string{".js", ".mjs"} {
		specs, err := Script("tools.search", "./caps/search"+ext).buildSpecs()
		if err != nil {
			t.Fatalf("buildSpecs(%s): %v", ext, err)
		}
		if specs[0].Runtime != registry.RuntimeNode {
			t.Errorf("Runtime(%s) = %q, want node", ext, specs[0].Runtime)
		}
	}
}

// TestScriptBuildSpecs_UnknownExt verifies that unknown extensions return an error.
func TestScriptBuildSpecs_UnknownExt(t *testing.T) {
	if _, err := Script("x", "./caps/x.rb").buildSpecs(); err == nil {
		t.Error("expected error for .rb extension, got nil")
	}
}

// TestScriptOptions verifies that Groups, Config, and Timeout chain correctly.
func TestScriptOptions(t *testing.T) {
	specs, err := Script("embed", "./caps/embed.jl").
		Groups("gpu", "asr").
		Config(map[string]any{"model": "small"}).
		buildSpecs()
	if err != nil {
		t.Fatalf("buildSpecs: %v", err)
	}
	spec := specs[0]
	if len(spec.Groups) != 2 || spec.Groups[0] != "gpu" {
		t.Errorf("Groups = %v", spec.Groups)
	}
	if spec.Config["model"] != "small" {
		t.Errorf("Config model = %v", spec.Config["model"])
	}
}

// TestDirBuildSpecs_MixedExtensions verifies that Dir correctly identifies
// Python, Julia, and Node capabilities in a mixed directory.
func TestDirBuildSpecs_MixedExtensions(t *testing.T) {
	tmp := t.TempDir()

	// Create mixed capability files
	files := map[string]registry.Runtime{
		"echo.py":         registry.RuntimePython,
		"embed.jl":        registry.RuntimeJulia,
		"search.js":       registry.RuntimeNode,
		"batch.mjs":       registry.RuntimeNode,
		"sub/classify.py": registry.RuntimePython,
		"sub/infer.jl":    registry.RuntimeJulia,
	}
	// Infrastructure files that must be skipped
	skipFiles := []string{
		"runtime.py", "runtime.jl", "runtime.js",
		"cap.py", "cap.jl", "cap.js",
		"transport.jl", "transport.js",
		"README.md",
	}

	for name := range files {
		full := filepath.Join(tmp, filepath.FromSlash(name))
		os.MkdirAll(filepath.Dir(full), 0o755)
		os.WriteFile(full, []byte("# pepper:name = "+name), 0o644)
	}
	for _, name := range skipFiles {
		os.WriteFile(filepath.Join(tmp, name), []byte("skip"), 0o644)
	}
	os.MkdirAll(filepath.Join(tmp, "node_modules", "some-pkg"), 0o755)
	os.WriteFile(filepath.Join(tmp, "node_modules", "some-pkg", "index.js"), []byte("// node module"), 0o644)

	specs, err := Dir(tmp).buildSpecs()
	if err != nil {
		t.Fatalf("Dir.buildSpecs: %v", err)
	}

	if len(specs) != len(files) {
		t.Errorf("got %d specs, want %d; specs: %v", len(specs), len(files), specNames(specs))
	}

	runtimeCount := map[registry.Runtime]int{}
	for _, s := range specs {
		runtimeCount[s.Runtime]++
	}
	if runtimeCount[registry.RuntimePython] != 2 {
		t.Errorf("Python caps = %d, want 2", runtimeCount[registry.RuntimePython])
	}
	if runtimeCount[registry.RuntimeJulia] != 2 {
		t.Errorf("Julia caps = %d, want 2", runtimeCount[registry.RuntimeJulia])
	}
	if runtimeCount[registry.RuntimeNode] != 2 {
		t.Errorf("Node caps = %d, want 2", runtimeCount[registry.RuntimeNode])
	}
}

// TestDirSkipsNodeModules verifies that node_modules is never walked.
func TestDirSkipsNodeModules(t *testing.T) {
	tmp := t.TempDir()
	os.MkdirAll(filepath.Join(tmp, "node_modules", "pkg"), 0o755)
	os.WriteFile(filepath.Join(tmp, "node_modules", "pkg", "index.js"), []byte("// pkg"), 0o644)
	os.WriteFile(filepath.Join(tmp, "real.js"), []byte("// real cap"), 0o644)

	specs, err := Dir(tmp).buildSpecs()
	if err != nil {
		t.Fatalf("Dir.buildSpecs: %v", err)
	}
	if len(specs) != 1 {
		t.Errorf("got %d specs, want 1 (node_modules should be skipped): %v", len(specs), specNames(specs))
	}
}

// TestDirGroupsPropagated verifies that Dir.Groups applies to all discovered specs.
func TestDirGroupsPropagated(t *testing.T) {
	tmp := t.TempDir()
	os.WriteFile(filepath.Join(tmp, "a.py"), []byte(""), 0o644)
	os.WriteFile(filepath.Join(tmp, "b.jl"), []byte(""), 0o644)
	os.WriteFile(filepath.Join(tmp, "c.js"), []byte(""), 0o644)

	specs, err := Dir(tmp).Groups("gpu", "fast").buildSpecs()
	if err != nil {
		t.Fatalf("Dir.buildSpecs: %v", err)
	}
	for _, s := range specs {
		if len(s.Groups) != 2 || s.Groups[0] != "gpu" {
			t.Errorf("spec %q: Groups = %v, want [gpu fast]", s.Name, s.Groups)
		}
	}
}

func specNames(specs []*registry.Spec) []string {
	names := make([]string, len(specs))
	for i, s := range specs {
		names[i] = s.Name + "(" + string(s.Runtime) + ")"
	}
	return names
}
