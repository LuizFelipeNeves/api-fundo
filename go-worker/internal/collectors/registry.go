package collectors

import (
	"context"
	"fmt"
	"os"
	"strings"
)

func verboseLogs() bool {
	if strings.EqualFold(strings.TrimSpace(os.Getenv("LOG_VERBOSE")), "true") {
		return true
	}
	level := strings.ToLower(strings.TrimSpace(os.Getenv("LOG_LEVEL")))
	return level == "debug" || level == "trace"
}

// Collector interface for all collectors
type Collector interface {
	Name() string
	Collect(ctx context.Context, req CollectRequest) (*CollectResult, error)
}

// CollectRequest represents a collection request
type CollectRequest struct {
	FundCode string
	CNPJ     string
	ID       string
}

// CollectResult represents a collection result
type CollectResult struct {
	Data      interface{}
	Timestamp string
}

// Registry holds all available collectors
type Registry struct {
	collectors map[string]Collector
}

// NewRegistry creates a new collector registry
func NewRegistry() *Registry {
	return &Registry{
		collectors: make(map[string]Collector),
	}
}

// Register adds a collector to the registry
func (r *Registry) Register(c Collector) {
	r.collectors[c.Name()] = c
}

// Get retrieves a collector by name
func (r *Registry) Get(name string) (Collector, error) {
	c, ok := r.collectors[name]
	if !ok {
		return nil, fmt.Errorf("collector not found: %s", name)
	}
	return c, nil
}

// List returns all collector names
func (r *Registry) List() []string {
	names := make([]string, 0, len(r.collectors))
	for name := range r.collectors {
		names = append(names, name)
	}
	return names
}
