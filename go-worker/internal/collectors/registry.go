package collectors

import (
	"context"
	"fmt"
)

// CollectRequest represents a request to collect data
type CollectRequest struct {
	Collector   string
	FundCode    string
	CNPJ        string
	ID          string
	TriggeredBy string
}

// CollectResult represents the result of a collection
type CollectResult struct {
	FundCode  string
	Data      interface{}
	Timestamp string
}

// Collector defines the interface for all collectors
type Collector interface {
	Name() string
	Collect(ctx context.Context, req CollectRequest) (*CollectResult, error)
}

// Registry holds all registered collectors
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

// All returns all registered collectors
func (r *Registry) All() []Collector {
	collectors := make([]Collector, 0, len(r.collectors))
	for _, c := range r.collectors {
		collectors = append(collectors, c)
	}
	return collectors
}
