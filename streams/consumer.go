package streams

import "github.com/gmbyapa/kstream/v2/streams/topology"

type Consumer interface {
	Run(topologyBuilder topology.Topology) error
	Init(topologyBuilder topology.Topology) error
	Stop() error
	Ready() error
}
