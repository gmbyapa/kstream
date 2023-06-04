package topology

import "fmt"

type SubTopologyId struct {
	id   int
	name string
}

func NewSubTopologyId(id int, name string) SubTopologyId {
	return SubTopologyId{
		id:   id,
		name: name,
	}
}

func (id SubTopologyId) Id() int {
	return id.id
}

func (id SubTopologyId) Name() string {
	return id.name
}

func (id SubTopologyId) String() string {
	return fmt.Sprintf(`%s|%d`, id.name, id.id)
}

const (
	KindStream      SubTopologyKind = `stream`
	KindTable       SubTopologyKind = `table`
	KindGlobalTable SubTopologyKind = `global-table`
)

type SubTopologyKind string

type SubTopologyBuilder interface {
	Id() SubTopologyId
	SetId(SubTopologyId)
	AddNode(builder NodeBuilder)
	AddNodeWithEdge(node, edge NodeBuilder)
	Parent(node NodeBuilder) NodeBuilder
	NodeSource(node NodeBuilder) Source
	Setup(ctx SubTopologySetupContext) error
	Build(ctx SubTopologyContext) (SubTopology, error)
	AddEdge(parent, node NodeBuilder)
	RemoveNode(node NodeBuilder)
	RemoveAll()
	Edges() []Edge
	Sources() []Source
	MergeSubTopology(subTp SubTopologyBuilder)
	Nodes() []NodeBuilder
	AddStore(builder LoggableStoreBuilder)
	StateStores() map[string]LoggableStoreBuilder
	AddSource(source Source)
	Kind() SubTopologyKind
}

type Edge struct {
	parent, node NodeBuilder
}

func NewEdge(parent, node NodeBuilder) Edge {
	return Edge{parent: parent, node: node}
}

func (e Edge) Parent() NodeBuilder {
	return e.parent
}

func (e Edge) Node() NodeBuilder {
	return e.node
}
