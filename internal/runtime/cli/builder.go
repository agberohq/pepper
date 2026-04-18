package cli

import "github.com/agberohq/pepper/internal/registry"

// BuildSpec implements pepper.CMDBuilder for *Builder.
// It stores the completed CMDSpec in spec.CLISpec so the CLI runner can
// retrieve it later without importing the pepper package.
func (b *Builder) BuildSpec(name string) *registry.Spec {
	spec := b.Build()
	groups := spec.Groups
	if len(groups) == 0 {
		groups = []string{"default"}
	}
	return &registry.Spec{
		Name:    name,
		Runtime: registry.RuntimeCLI,
		Version: "0.0.0",
		Source:  spec.Command,
		Groups:  groups,
		CLISpec: spec, // store the full CMDSpec for the runner
	}
}
