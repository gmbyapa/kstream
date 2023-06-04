package processors

import (
	"context"

	"github.com/gmbyapa/kstream/streams/topology"
)

type Branch struct {
	topology.DefaultNode
}

func (b *Branch) Build(ctx topology.SubTopologyContext) (topology.Node, error) {
	return &Branch{
		DefaultNode: b.DefaultNode,
	}, nil
}

func (b *Branch) New(ctx topology.SubTopologyContext) (topology.Node, error) {
	return &Branch{
		DefaultNode: b.DefaultNode,
	}, nil
}

func (b *Branch) Run(ctx context.Context, kIn, vIn interface{}) (kOut, vOut interface{}, cont bool, err error) {
	for _, br := range b.Edges() {
		branch, _ := br.(*BranchChild)

		ok, err := branch.Predicate(ctx, kIn, vIn)
		if err != nil {
			return nil, nil, false, b.WrapErrWith(err, `predicate error`)
		}

		if ok {
			_, _, _, err := branch.Run(ctx, kIn, vIn)
			if err != nil {
				return nil, nil, false, b.WrapErr(err)
			}
			break
		}
	}

	return kIn, kOut, true, nil
}

func (b *Branch) Type() topology.Type {
	return topology.Type{
		Name:  "splitter",
		Attrs: nil,
	}
}
