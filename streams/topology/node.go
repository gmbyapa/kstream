package topology

import (
	base "context"
	"fmt"
)

type NodeId struct {
	id            int
	path          string
	subTopologyID string
}

func NewNodeId(id int, path, subTopologyID string) NodeId {
	return NodeId{
		id:            id,
		path:          path,
		subTopologyID: subTopologyID,
	}
}

func (id NodeId) Id() int {
	return id.id
}

func (id NodeId) String() string {
	return fmt.Sprintf(`%d`, id.id+1)
}

func (id NodeId) SubTopologyId() string {
	return id.subTopologyID
}

type NodeInfo interface {
	// Id returns a unique node id within the topology
	Id() NodeId
	// Type returns type of the node eg: Joiner, Flatmap
	Type() Type
	ReadsFrom() []string
	WritesAt() []string
	NameAs(name string)
	NodeName() string
}

type NodeBuilder interface {
	NodeInfo
	SetId(id NodeId)
	// Setup is called once when stream app starting (SubTopologyBuilder.Setup())
	// Eg usage: create changelog topics for node
	Setup(ctx SubTopologySetupContext) error
	// Build calls with every Consumer PartitionAssignEvent and this method is responsible to create a new instance of
	// the node
	Build(ctx SubTopologyContext) (Node, error)
}

type Type struct {
	Name  string
	Attrs map[string]string
}

type Node interface {
	NodeInfo
	Run(ctx base.Context, kIn, vIn interface{}) (kOut, vOut interface{}, cont bool, err error)
	AddEdge(node Node)
	Edges() []Node
}

type InitableNode interface {
	Node
	// Init is called once the Node build is completed and before message processing starts.
	// Please refer SubTopology.Init()
	Init(ctx NodeContext) error
}

type CloseableNode interface {
	Node
	// Close is called once the message processing stops
	// Please refer SubTopology.Close()
	Close() error
}

type StateType string

const (
	StateReadOnly  StateType = `read_only`
	StateReadWrite StateType = `read_write`
)

type State struct {
	Type  StateType
	Store LoggableStateStore
}

type StateBuilder struct {
	Type  StateType
	Store LoggableStateStore
}

type StateStoreNameFunc func(store string) string
