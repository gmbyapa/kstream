package processors

import (
	"context"

	"github.com/gmbyapa/kstream/streams/topology"
)

type Merger struct {
	topology.DefaultNode
}

func (m *Merger) Build(ctx topology.SubTopologyContext) (topology.Node, error) {
	return &Merger{
		DefaultNode: m.DefaultNode,
	}, nil
}

func (m *Merger) Type() topology.Type {
	return topology.Type{
		Name:  "merger",
		Attrs: nil,
	}
}

func (m *Merger) Run(ctx context.Context, kIn, vIn interface{}) (kOut, vOut interface{}, cont bool, err error) {
	return m.Forward(ctx, kIn, vIn, true)
}
