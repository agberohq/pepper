// Package main — Pepper usage example.
//
// Demonstrates the new typed API: pepper.Func, pepper.HTTP, pepper.Dir,
// pepper.Call[Out].Bind(pp).Do(ctx), .Execute(ctx), .Session(sess).Do(ctx).
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
	"strings"
	"time"

	pepper "github.com/agberohq/pepper"
)

// Typed input/output structs

type TextInput struct {
	Text string `json:"text"`
}

type TextOutput struct {
	Text string `json:"text"`
}

func main() {
	pp, err := pepper.New(
		pepper.WithWorkers(
			pepper.NewWorker("w-1").Groups("default").
				MaxRequests(10000).
				MaxUptime(24*time.Hour),
		),
		pepper.WithCodec(pepper.CodecMsgPack),
		pepper.WithShutdownTimeout(5*time.Second),
		pepper.WithTracking(true),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer pp.Stop()

	// Register capabilities — one method for all runtime types

	if err := pp.Register(pepper.Func("text.upper",
		func(ctx context.Context, in TextInput) (TextOutput, error) {
			return TextOutput{Text: strings.ToUpper(in.Text)}, nil
		},
	)); err != nil {
		log.Fatal(err)
	}

	if err := pp.Register(pepper.Func("text.reverse",
		func(ctx context.Context, in TextInput) (TextOutput, error) {
			runes := []rune(in.Text)
			for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
				runes[i], runes[j] = runes[j], runes[i]
			}
			return TextOutput{Text: string(runes)}, nil
		},
	)); err != nil {
		log.Fatal(err)
	}

	// Python script (requires python3 + msgpack)
	if err := pp.Register(pepper.Script("echo", "../testdata/caps/echo.py")); err != nil {
		log.Printf("echo not registered (requires python3 + msgpack): %v", err)
	}

	// Pipeline — no internal imports needed
	if err := pp.Compose("text.pipeline",
		pepper.Pipe("text.upper"),
		pepper.Transform(func(in map[string]any) (map[string]any, error) {
			text, _ := in["text"].(string)
			return map[string]any{"text": "~" + text + "~"}, nil
		}),
		pepper.Pipe("text.reverse"),
	); err != nil {
		log.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := pp.Start(ctx); err != nil {
		log.Fatal(err)
	}
	fmt.Println("pepper started")

	// Typed request/response — Call[Out]

	result, err := pepper.Call[TextOutput]{
		Cap:   "text.upper",
		Input: TextInput{Text: "hello world"},
	}.Bind(pp).Do(ctx)
	if err != nil {
		log.Printf("text.upper error: %v", err)
	} else {
		fmt.Printf("text.upper result: %q\n", result.Text)
	}

	// Fire-and-forget — Exec

	if err := (pepper.Exec{
		Cap:   "text.upper",
		Input: pepper.In{"text": "fire and forget"},
	}.Bind(pp).Do(ctx)); err != nil {
		log.Printf("exec error: %v", err)
	} else {
		fmt.Println("exec: sent")
	}

	// Pipeline with Process[Out] — unified tracking

	proc, err := pepper.Call[TextOutput]{
		Cap:   "text.pipeline",
		Input: TextInput{Text: "hello pipeline"},
	}.Bind(pp).Execute(ctx)
	if err != nil {
		log.Fatalf("Execute: %v", err)
	}
	fmt.Printf("process ID: %s\n", proc.ID())

	go func() {
		for event := range proc.Events() {
			switch event.Status {
			case pepper.StatusRunning:
				fmt.Printf("  → %-20s started\n", event.Stage)
			case pepper.StatusDone:
				fmt.Printf("  ✓ %-20s done (%dms, worker=%s)\n",
					event.Stage, event.DurationMs, event.WorkerID)
			case pepper.StatusFailed:
				fmt.Printf("  ✗ %-20s FAILED: %s\n", event.Stage, event.Error)
			}
		}
	}()

	pipeResult, err := proc.Wait(ctx)
	if err != nil {
		log.Fatalf("Wait: %v", err)
	}
	fmt.Printf("pipeline result: %q\n", pipeResult.Text)

	state := proc.State()
	fmt.Printf("process %s: %s (%d%%)\n", state.ID, state.Status, state.PercentDone)
	for i, a := range state.Actions {
		fmt.Printf("  stage[%d] %-20s %s %dms worker=%s\n",
			i, a.Stage, a.Status, a.DurationMs, a.WorkerID)
	}

	schemas := pp.Capabilities(ctx)
	tools := pepper.Tools(schemas, pepper.FormatOpenAI)
	fmt.Printf("\nregistered as OpenAI tools:\n")
	for _, t := range tools {
		fmt.Printf("  • %s\n", t.Function.Name)
	}

	// NewSession generates an ID you can persist (cookie, JWT, etc.)
	sess := pp.NewSession()
	fmt.Printf("\nsession ID: %s\n", sess.ID())
	sess.Set("preference", "concise")

	// Everything dispatched through sess carries the session ID automatically.
	// No passing IDs around — capabilities read it via SessionFromContext(ctx).
	if err := pp.Register(pepper.Func("echo.session",
		func(ctx context.Context, in TextInput) (TextOutput, error) {
			sessID := pepper.SessionFromContext(ctx) // available in every Go Func
			return TextOutput{Text: "session=" + sessID}, nil
		},
	)); err == nil {
		out, _ := pepper.Call[TextOutput]{Cap: "echo.session", Input: TextInput{}}.
			Session(sess).
			Do(ctx)
		fmt.Printf("capability saw session: %s\n", out.Text)
	}

	// Resume an existing session by ID (e.g. from a request cookie or JWT)
	sameSess := pp.Session(sess.ID())
	if v, ok := sameSess.Get("preference"); ok {
		fmt.Printf("resumed session preference: %v\n", v)
	}

	fmt.Println("\ndone")
}
