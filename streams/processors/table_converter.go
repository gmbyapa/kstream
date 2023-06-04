package processors

import (
	"context"

	"github.com/gmbyapa/kstream/streams/stores"
	"github.com/gmbyapa/kstream/streams/topology"
)

type TableConverter struct {
	Store string

	store stores.Store
	topology.DefaultNode
}

func (j *TableConverter) Build(ctx topology.SubTopologyContext) (topology.Node, error) {
	return &TableConverter{
		DefaultNode: j.DefaultNode,
		Store:       j.Store,
	}, nil
}

func (j *TableConverter) Init(ctx topology.NodeContext) error {
	j.store = ctx.Store(j.Store)
	return nil
}

func (j *TableConverter) WritesAt() []string {
	return []string{j.Store}
}

func (j *TableConverter) Type() topology.Type {
	return topology.Type{
		Name:  "to_table",
		Attrs: nil,
	}
}

func (j *TableConverter) Run(ctx context.Context, kIn, vIn interface{}) (kOut, vOut interface{}, cont bool, err error) {
	if err := j.store.Set(ctx, kIn, vIn, 0); err != nil {
		return nil, nil, false, j.WrapErrWith(err, `state-store write failed`)
	}

	return j.Forward(ctx, kIn, vIn, true)
}
