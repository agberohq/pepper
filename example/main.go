// Package main — Pepper usage example.
//
// Demonstrates all four runtime types:
//
// Run:
//
//	cd example
//	go run main.go
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	pepper "github.com/agberohq/pepper"
	"github.com/agberohq/pepper/internal/core"
	"github.com/agberohq/pepper/internal/envelope"
	"github.com/agberohq/pepper/internal/hooks"
)

// UpperWorker is a Go-native capability that uppercases text.
type UpperWorker struct{}

func (w *UpperWorker) Setup(cap string, config map[string]any) error { return nil }

func (w *UpperWorker) Run(ctx context.Context, cap string, in core.In) (core.In, error) {
	text, _ := in["text"].(string)
	result := ""
	for _, c := range text {
		if c >= 'a' && c <= 'z' {
			result += string(rune(c - 32))
		} else {
			result += string(c)
		}
	}
	return core.In{"text": result}, nil
}

func (w *UpperWorker) Capabilities() []core.Capability {
	return []core.Capability{{
		Name:    "text.upper",
		Version: "1.0.0",
		Groups:  []string{"cpu"},
	}}
}

func main() {
	pp, err := pepper.New(
		pepper.Workers(
			pepper.NewWorker("w-1").Groups("default", "cpu").
				MaxRequests(10000).
				MaxUptime(24*time.Hour),
		),
		pepper.WithSerializer(pepper.MsgPack),
		pepper.ShutdownTimeout(5*time.Second),
		pepper.Debug(pepper.DebugEnvelope),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer pp.Stop()

	if err := pp.Register("echo", "../testdata/caps/echo.py"); err != nil {
		log.Fatal(err)
	}
	if err := pp.Include("text.upper", &UpperWorker{}); err != nil {
		log.Fatal(err)
	}

	// Global before hook
	pp.Hooks().Global().Before("logger", func(
		ctx context.Context,
		env *envelope.Envelope,
		in core.In,
	) (core.In, error) {
		fmt.Printf("[hook:before] cap=%s\n", env.Cap)
		return nil, nil
	})

	// Global after hook
	pp.Hooks().Global().After("logger", func(
		ctx context.Context,
		env *envelope.Envelope,
		in core.In,
		result hooks.Result,
	) (hooks.Result, error) {
		fmt.Printf("[hook:after] cap=%s err=%v\n", result.Cap, result.Err)
		return result, nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := pp.Start(ctx); err != nil {
		log.Fatal(err)
	}
	fmt.Println("pepper started")

	r1, err := pp.Do(ctx, "echo", core.In{"msg": "hello from Go"})
	if err != nil {
		log.Printf("echo error: %v", err)
	} else {
		fmt.Printf("echo result: %v\n", r1.AsJSON())
	}

	r2, err := pp.Do(ctx, "text.upper", core.In{"text": "hello world"})
	if err != nil {
		log.Printf("upper error: %v", err)
	} else {
		fmt.Printf("upper result: %v\n", r2.AsJSON())
	}

	schemas := pp.Capabilities(ctx)
	tools := pepper.ToOpenAITools(schemas)
	fmt.Printf("\nregistered as OpenAI tools:\n")
	for _, t := range tools {
		fn := t["function"].(map[string]any)
		fmt.Printf("  • %s\n", fn["name"])
	}

	sess := pp.Session("user-123")
	sess.Set("preference", "concise")
	if v, ok := sess.Get("preference"); ok {
		fmt.Printf("\nsession preference: %v\n", v)
	}

	_ = pepper.Ollama
	fmt.Println("\ndone")
}
