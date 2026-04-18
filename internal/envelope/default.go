package envelope

// DefaultEnvelope returns an Envelope with all safe defaults populated.
// Callers set only the fields they care about.
func DefaultEnvelope() Envelope {
	return Envelope{
		ProtoVer:   1,
		Dispatch:   DispatchAny,
		MaxHops:    10,
		MaxCbDepth: 5,
		Meta:       map[string]any{},
	}
}
