package topology

type NodeContext interface {
	SubTopologyContext
	Store(name string) StateStore
}

type nodeContext struct {
	SubTopologyContext
	topology SubTopology
}

func (n *nodeContext) Store(name string) StateStore {
	return n.topology.Store(name)
}

func NewNodeContext(parent SubTopologyContext, subTopology SubTopology) NodeContext {
	return &nodeContext{
		SubTopologyContext: parent,
		topology:           subTopology,
	}
}
