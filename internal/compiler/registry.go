package compiler

import (
	"fmt"
	"sort"

	"github.com/uu999/evalfrog/internal/ir"
)

type Registry struct {
	handlers map[ir.NodeType]Handler
}

func NewRegistry(handlers ...Handler) (Registry, error) {
	registry := Registry{handlers: make(map[ir.NodeType]Handler, len(handlers))}
	for _, handler := range handlers {
		if handler == nil || !ir.ValidNodeType(handler.NodeType()) {
			return Registry{}, fmt.Errorf("compiler handler has an invalid node type")
		}
		if handler.Coordinate().Type == "" || handler.Coordinate().Version == 0 {
			return Registry{}, fmt.Errorf("compiler handler %s has an invalid operation coordinate", handler.NodeType())
		}
		if _, exists := registry.handlers[handler.NodeType()]; exists {
			return Registry{}, fmt.Errorf("compiler handler %s is registered twice", handler.NodeType())
		}
		registry.handlers[handler.NodeType()] = handler
	}
	return registry, nil
}

func (registry Registry) Handler(nodeType ir.NodeType) (Handler, bool) {
	handler, exists := registry.handlers[nodeType]
	return handler, exists
}

func (registry Registry) NodeTypes() []ir.NodeType {
	result := make([]ir.NodeType, 0, len(registry.handlers))
	for nodeType := range registry.handlers {
		result = append(result, nodeType)
	}
	sort.Slice(result, func(left, right int) bool { return result[left] < result[right] })
	return result
}
