package processors

//import (
//	"context"
//	"github.com/tryfix/errors"
//	"github.com/tryfix/kstream/kstream/topology"
//	"github.com/tryfix/kstream/kstream/util"
//)
//
//type ValueTransformFunc func(ctx context.Context, key, value interface{}) (vOut interface{}, err error)
//
//type ValueTransformer struct {
//	NId                util.NodeId
//	ValueTransformFunc ValueTransformFunc
//	childBuilders      []topology.NodeBuilder
//	childs             []topology.Node
//}
//
//func (vt *ValueTransformer) Build() (topology.Node, error) {
//	var childs []topology.Node
//	//var childBuilders []node.NodeBuilder
//
//	for _, childBuilder := range vt.childBuilders {
//		child, err := childBuilder.Build()
//		if err != nil {
//			return nil, err
//		}
//
//		childs = append(childs, child)
//	}
//
//	return &ValueTransformer{
//		ValueTransformFunc: vt.ValueTransformFunc,
//		childs:             childs,
//		NId:                 vt.Id(),
//	}, nil
//}
//
//func (vt *ValueTransformer) ChildBuilders() []topology.NodeBuilder {
//	return vt.childBuilders
//}
//
//func (vt *ValueTransformer) AddChildBuilder(builder topology.NodeBuilder) {
//	vt.childBuilders = append(vt.childBuilders, builder)
//}
//
//func (vt *ValueTransformer) Next() bool {
//	return true
//}
//
//func (vt *ValueTransformer) Id() util.NodeId {
//	return vt.NId
//}
//
//func (vt *ValueTransformer) Run(ctx context.Context, kIn, vIn interface{}) (kOut, vOut interface{}, cont bool, err error) {
//	v, err := vt.ValueTransformFunc(ctx, kIn, vIn)
//	if err != nil {
//		return nil, nil, false, errors.WithPrevious(err, `error in value transform function`)
//	}
//
//	for _, child := range vt.childs {
//		_, _, next, err := child.Run(ctx, kIn, v)
//		if err != nil || !next {
//			return nil, nil, false, err
//		}
//	}
//
//	return kIn, v, true, err
//}
//
//func (vt *ValueTransformer) Type() topology.Type {
//	return topology.Type(`value_transformer`)
//}
//
//func (vt *ValueTransformer) Childs() []topology.Node {
//	return vt.childs
//}
//
//func (vt *ValueTransformer) AddChild(node topology.Node) {
//	vt.childs = append(vt.childs, node)
//}
