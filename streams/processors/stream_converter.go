package processors

import (
	"context"

	"github.com/gmbyapa/kstream/streams/topology"
)

type StreamConverter struct {
	topology.DefaultNode
}

func (j *StreamConverter) Build(ctx topology.SubTopologyContext) (topology.Node, error) {
	return &StreamConverter{
		DefaultNode: j.DefaultNode,
	}, nil
}

func (j *StreamConverter) Type() topology.Type {
	return topology.Type{
		Name:  "to_stream",
		Attrs: nil,
	}
}

func (j *StreamConverter) Run(ctx context.Context, kIn, vIn interface{}) (kOut, vOut interface{}, cont bool, err error) {
	return j.Forward(ctx, kIn, vIn, true)
}
