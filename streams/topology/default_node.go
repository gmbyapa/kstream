package topology

import (
	base "context"
	"fmt"

	"github.com/gmbyapa/kstream/pkg/errors"
)

type KeyValPair struct {
	Key   interface{}
	Value interface{}
}

type DefaultNode struct {
	id    NodeId
	name  string
	edges []Node
}

func (n *DefaultNode) Id() NodeId {
	return n.id
}

func (n *DefaultNode) SetId(id NodeId) {
	n.id = id
}

func (n *DefaultNode) AddEdge(node Node) {
	n.edges = append(n.edges, node)
}

func (n *DefaultNode) Edges() []Node {
	return n.edges
}

func (n *DefaultNode) WrapErr(err error) error {
	return errors.WrapWithFrameSkip(err, fmt.Sprintf(`node error at [%s.%s]`, n.id.SubTopologyId(), n.id), 3)
}

func (n *DefaultNode) Err(message string) error {
	return errors.NewWithFrameSkip(fmt.Sprintf(`%s at [%s.%s]`, message, n.id.SubTopologyId(), n.id), 3)
}

func (n *DefaultNode) WrapErrWith(err error, message string) error {
	return errors.WrapWithFrameSkip(err, fmt.Sprintf(`%s at [%s.%s]`, message, n.id.SubTopologyId(), n.id), 3)
}

func (n *DefaultNode) Forward(ctx base.Context, kIn, vIn interface{}, cont bool) (interface{}, interface{}, bool, error) {
	for _, child := range n.Edges() {
		_, _, _, err := child.Run(ctx, kIn, vIn)
		if err != nil { // TODO whats !next
			return nil, nil, false, err
		}
	}

	return kIn, vIn, cont, nil
}

func (n *DefaultNode) ForwardAll(ctx base.Context, kvs []KeyValPair, cont bool) (kOut interface{}, vOut interface{}, next bool, err error) {
	for _, kv := range kvs {
		kOut, vOut, _, err = n.Forward(ctx, kv.Key, kv.Value, cont)
		if err != nil { // TODO whats !next
			return nil, nil, false, err
		}
	}

	return
}

func (n *DefaultNode) Ignore() (interface{}, interface{}, bool, error) {
	return nil, nil, false, nil
}

func (n *DefaultNode) IgnoreWithError(err error) (interface{}, interface{}, bool, error) {
	return nil, nil, false, err
}

func (n *DefaultNode) ReadsFrom() []string { return nil }

func (n *DefaultNode) WritesAt() []string { return nil }

func (n *DefaultNode) Setup(ctx SubTopologySetupContext) error { return nil }

func (n *DefaultNode) NameAs(name string) { n.name = name }

func (n *DefaultNode) NodeName() string {
	return n.name
}
