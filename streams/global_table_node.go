package streams

import (
	"context"
	"github.com/gmbyapa/kstream/backend"
	"github.com/gmbyapa/kstream/streams/topology"
)

type GlobalTableNode struct {
	topology.DefaultNode
	OffsetBackend backend.Backend
	StoreName     string
	store         topology.StateStore
}

func (n *GlobalTableNode) Run(ctx context.Context, kIn, vIn interface{}) (kOut, vOut interface{}, cont bool, err error) {
	return nil, nil, false, err
}

func (n *GlobalTableNode) State() topology.StateStore {
	return n.store
}

func (n *GlobalTableNode) Init(ctx topology.NodeContext) error {
	n.store = ctx.Store(n.StoreName)
	return nil
}

func (n *GlobalTableNode) WritesAt() []string {
	return []string{n.StoreName}
}

func (n *GlobalTableNode) StateType() topology.StateType {
	return topology.StateReadWrite
}

func (n *GlobalTableNode) Type() topology.Type {
	return topology.Type{
		Name:  "global_table_processor",
		Attrs: nil,
	}
}

func (n *GlobalTableNode) Build(ctx topology.SubTopologyContext) (topology.Node, error) {
	return &GlobalTableNode{
		OffsetBackend: n.OffsetBackend,
		StoreName:     n.StoreName,
	}, nil
}
