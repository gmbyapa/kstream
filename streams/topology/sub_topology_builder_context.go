package topology

//type subTopologyBuilderContext struct {
//	SubTopologySetupContext
//	partitionCount int32
//}

//func (t *subTopologyBuilderContext) MaxPartitionCount() int32 {
//	return t.partitionCount
//}

//func NewSubTopologyBuilderContext(
//	builderCtx SubTopologySetupContext,
//	partitionCount int32,
//) SubTopologyBuilderContext {
//	return &subTopologyBuilderContext{
//		SubTopologySetupContext:       builderCtx,
//		//logger: logger,
//		partitionCount: partitionCount,
//	}
//}

type SubTopologyBuilderContext interface {
	BuilderContext
	MaxPartitionCount() int32
}
