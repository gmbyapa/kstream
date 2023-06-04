package streams

import (
	"fmt"
	"github.com/gmbyapa/kstream/pkg/errors"
	"github.com/gmbyapa/kstream/streams/topology"
)

type kSubTopologyBuilder struct {
	id           topology.SubTopologyId
	kind         topology.SubTopologyKind
	nodeBuilders []topology.NodeBuilder
	sinks        []topology.Sink
	edges        []topology.Edge
	stores       map[string]topology.LoggableStoreBuilder // topic:[]store
}

func (b *kSubTopologyBuilder) Id() topology.SubTopologyId {
	return b.id
}

func (b *kSubTopologyBuilder) SetId(id topology.SubTopologyId) {
	b.id = id
}

func (b *kSubTopologyBuilder) AddNode(node topology.NodeBuilder) {
	// TODO set the path
	node.SetId(topology.NewNodeId(len(b.nodeBuilders), `test.path`, b.id.Name()))
	b.nodeBuilders = append(b.nodeBuilders, node)
}

func (b *kSubTopologyBuilder) RemoveNode(node topology.NodeBuilder) {
	for i, nd := range b.nodeBuilders {
		if nd.Id() == node.Id() {
			b.nodeBuilders = append(b.nodeBuilders[:i], b.nodeBuilders[i+1:]...)
		}
	}

	var edges []topology.Edge
	for _, edge := range b.edges {
		if edge.Parent().Id() == node.Id() || edge.Node().Id() == node.Id() {
			continue
		}
		edges = append(edges, edge)
	}

	b.edges = edges
}

func (b *kSubTopologyBuilder) RemoveAll() {
	b.nodeBuilders = nil
	b.edges = nil
}

func (b *kSubTopologyBuilder) AddNodeWithEdge(node, edge topology.NodeBuilder) {
	b.AddNode(edge)
	b.AddEdge(node, edge)
}

func (b *kSubTopologyBuilder) Parent(node topology.NodeBuilder) topology.NodeBuilder {
	for _, edge := range b.Edges() {
		if edge.Node().Id() == node.Id() {
			return edge.Parent()
		}
	}

	return nil
}

func (b *kSubTopologyBuilder) NodeSource(node topology.NodeBuilder) topology.Source {
	if src, ok := node.(topology.Source); ok {
		return src
	}

	if parent := b.Parent(node); parent != nil {
		src, ok := parent.(topology.Source)
		if !ok {
			return b.NodeSource(parent)
		}

		return src
	}

	return nil
}

// AddEdge adds an Edge to the topology
// Note: this method can create cyclic func calls
func (b *kSubTopologyBuilder) AddEdge(parent, node topology.NodeBuilder) {
	// TODO check cyclic dependencies
	b.edges = append(b.edges, topology.NewEdge(parent, node))
}

func (b *kSubTopologyBuilder) Edges() []topology.Edge {
	return b.edges
}

func (b *kSubTopologyBuilder) Nodes() []topology.NodeBuilder {
	return b.nodeBuilders
}

func (b *kSubTopologyBuilder) Sources() []topology.Source {
	var sources []topology.Source
	for _, nd := range b.nodeBuilders {
		if src, ok := nd.(topology.Source); ok {
			sources = append(sources, src)
		}
	}

	return sources
}

func (b *kSubTopologyBuilder) MergeSubTopology(subTp topology.SubTopologyBuilder) {
	for _, nd := range subTp.Nodes() {
		b.AddNode(nd)
	}

	for _, edge := range subTp.Edges() {
		b.AddEdge(edge.Parent(), edge.Node())
	}

	for _, stor := range subTp.(*kSubTopologyBuilder).stores {
		b.AddStore(stor)
	}

	subTp.RemoveAll()
}

func (b *kSubTopologyBuilder) AddSource(source topology.Source) {
	b.AddNode(source)
}

func (b *kSubTopologyBuilder) Kind() topology.SubTopologyKind {
	return b.kind
}

func (b *kSubTopologyBuilder) AddStore(builder topology.LoggableStoreBuilder) {
	b.stores[builder.Name()] = builder
}

func (b *kSubTopologyBuilder) StateStores() map[string]topology.LoggableStoreBuilder {
	return b.stores
}

func (b *kSubTopologyBuilder) Setup(ctx topology.SubTopologySetupContext) error {
	// setup nodes
	for _, node := range b.Nodes() {
		if err := node.Setup(ctx); err != nil {
			return errors.Wrapf(err, `topologyBuilder setup failed on node [%s]`, node.Id())
		}
	}

	// Check co-partitioning requirements
	sources := map[string]topology.Source{}
	for _, src := range b.Sources() {
		sources[src.Topic()] = src
	}

	for _, src := range sources {
		if src.CoPartitionedWith() != nil {
			if src.TopicConfigs().NumPartitions != src.CoPartitionedWith().TopicConfigs().NumPartitions {
				return errors.New(fmt.Sprintf(
					`Topology %s, topics must be co-partitioned. Topic %s(%d-Partitions) does not 
							co-partitioned with Topic %s(%d-Partitions)) `,
					b.id, src.Topic(), src.TopicConfigs().NumPartitions, src.CoPartitionedWith().Topic(), src.CoPartitionedWith().TopicConfigs().NumPartitions))
			}
		}
	}

	// setup changelogs
	for _, store := range b.StateStores() {
		if err := store.Changelog().Setup(ctx); err != nil {
			return errors.Wrap(err, `topologyBuilder setup failed`)
		}
	}

	return nil
}

func (b *kSubTopologyBuilder) Build(ctx topology.SubTopologyContext) (topology.SubTopology, error) {
	// build nodeBuilders
	nds := make([]topology.Node, len(b.nodeBuilders))
	var sources []topology.Source
	var sinks []topology.Sink
	for i, node := range b.nodeBuilders {
		nd, err := node.Build(ctx)
		if err != nil {
			return nil, err
		}
		nds[i] = nd

		// if node is a sink update the sink list
		if snk, ok := nd.(topology.Sink); ok {
			sinks = append(sinks, snk)
		}

		if src, ok := nd.(topology.Source); ok {
			sources = append(sources, src)
		}
	}

	// connect Edges
	for _, edge := range b.Edges() {
		// find parent and add Edge
		nds[edge.Parent().Id().Id()].AddEdge(nds[edge.Node().Id().Id()])
	}

	stores := map[string]topology.StateStore{}
	for _, storBuilder := range b.stores {
		stor, err := storBuilder.Build(ctx)
		if err != nil {
			return nil, err
		}
		stores[storBuilder.Name()] = stor
	}

	return &KSubTopology{
		id:      b.id,
		edges:   b.edges,
		nodes:   nds,
		sources: sources,
		sinks:   sinks,
		stores:  stores,
	}, nil
}
