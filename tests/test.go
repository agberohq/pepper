package tests

import (
	"github.com/agberohq/pepper/internal/core"
	"github.com/olekukonko/ll"
)

var (
	runtimeFinder = core.NewRuntimeFinder()
	testLogger    = ll.New("test").Enable()
)
