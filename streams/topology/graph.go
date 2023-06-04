package topology

import (
	"fmt"
	"github.com/awalterschulze/gographviz"
	"github.com/gmbyapa/kstream/pkg/errors"
)

type Visualizer interface {
	AddTopology(node Builder)
	Visualize() (string, error)
}

type graphViz struct {
	topology Builder
	graph    *gographviz.Graph
}

func NewTopologyVisualizer() Visualizer {
	parent := `root`
	g := gographviz.NewGraph()
	if err := g.SetName(parent); err != nil {
		panic(err)
	}
	if err := g.SetDir(true); err != nil {
		panic(err)
	}

	if err := g.AddAttr(parent, `splines`, `true`); err != nil {
		panic(err)
	}

	if err := g.AddAttr(parent, `size`, `"50,12"`); err != nil {
		panic(err)
	}

	return &graphViz{
		graph: g,
	}
}

func (g *graphViz) AddTopology(topology Builder) {
	g.topology = topology
}

func (g *graphViz) Visualize() (string, error) {
	for _, sbTp := range g.topology.SubTopologies() {
		// add sub graph(cluster) into graph
		sbId := fmt.Sprintf(`cluster_%d`, sbTp.Id().Id())
		if err := g.graph.AddSubGraph(`root`, sbId, map[string]string{
			`color`: `blue`,
			`label`: fmt.Sprintf(`"Sub-topology: %d"`, sbTp.Id().Id()),
		}); err != nil {
			return "", err
		}

		// add source topics in the main graph
		for _, source := range sbTp.Sources() {
			topic := fmt.Sprintf(`"source_sink%s"`, source.Type().Attrs[`topic`])
			name := source.Type().Attrs[`topic`]
			if source.Internal() || source.AutoCreate() {
				name = fmt.Sprintf(`%s 🅸`, name)
			}
			if err := g.graph.AddNode(`root`, topic, map[string]string{
				`label`:     fmt.Sprintf(`"%s"`, name),
				`fillcolor`: `darkseagreen1`,
				`shape`:     `box`,
				`style`:     `"rounded,filled"`,
				`fontsize`:  `11`,
			}); err != nil {
				return ``, err
			}

			// add edges
			node := fmt.Sprintf(`"%d_%s"`, sbTp.Id().Id(), source.Id())
			if err := g.graph.AddEdge(topic, node, true, nil); err != nil {
				return ``, err
			}
		}

		for _, node := range sbTp.Nodes() {
			nId := fmt.Sprintf(`"%d_%s"`, sbTp.Id().Id(), node.Id())
			attrs := map[string]string{}
			g.applyAttributes(node, attrs)
			if err := g.graph.AddNode(sbId, nId, attrs); err != nil {
				return ``, err
			}

			for _, str := range node.ReadsFrom() {
				if sbTp.Kind() == KindGlobalTable {
					if err := g.addStore(str); err != nil {
						return "", err
					}
				} else {
					if err := g.addStateStore(sbId, str); err != nil {
						return "", err
					}
				}

				nId := fmt.Sprintf(`"%d_%s"`, sbTp.Id().Id(), node.Id())
				str := fmt.Sprintf(`"%s"`, str)
				attrs := map[string]string{
					`style`: `dashed`,
					`dir`:   `none`,
				}
				if err := g.graph.AddEdge(nId, str, true, attrs); err != nil {
					return ``, err
				}
			}

			for _, str := range node.WritesAt() {
				if sbTp.Kind() == KindGlobalTable {
					if err := g.addStore(str); err != nil {
						return "", err
					}
				} else {
					if err := g.addStateStore(sbId, str); err != nil {
						return "", err
					}
				}
				nId := fmt.Sprintf(`"%d_%s"`, sbTp.Id().Id(), node.Id())
				str := fmt.Sprintf(`"%s"`, str)
				attrs := map[string]string{
					`dir`: `none`,
				}
				if err := g.graph.AddEdge(nId, str, true, attrs); err != nil {
					return ``, err
				}
			}

			// add sink edge
			if node.Type().Name == `sink` {
				nId := fmt.Sprintf(`"%d_%s"`, sbTp.Id().Id(), node.Id())

				// add topic if not exists in the main graph
				topic := fmt.Sprintf(`"source_sink%s"`, node.Type().Attrs[`topic`])
				if !g.graph.IsNode(node.Type().Attrs[`topic`]) {
					if err := g.graph.AddNode(`root`, topic, map[string]string{
						`label`: fmt.Sprintf(`"%s"`, node.Type().Attrs[`topic`]),
					}); err != nil {
						return ``, err
					}
				}

				if err := g.graph.AddEdge(nId, topic, true, nil); err != nil {
					return ``, err
				}
			}
		}

		for _, edge := range sbTp.Edges() {
			if err := g.addEdge(sbTp, edge, nil); err != nil {
				return ``, err
			}
		}
	}

	graph, err := g.graph.WriteAst()
	if err != nil {
		return "", errors.Wrap(err, `graph failed`)
	}

	return graph.String(), nil
}

func (g *graphViz) addStore(stor string) error {
	strId := fmt.Sprintf(`"%s"`, stor)
	return g.graph.AddNode(`root`, strId, map[string]string{
		`label`: fmt.Sprintf(`"%s 🅶"`, stor),
		`shape`: `cylinder`,
	})
}

func (g *graphViz) addEdge(subTp SubTopologyBuilder, edge Edge, attrs map[string]string) error {
	parent := fmt.Sprintf(`"%d_%s"`, subTp.Id().Id(), edge.Parent().Id())
	node := fmt.Sprintf(`"%d_%s"`, subTp.Id().Id(), edge.Node().Id())
	// Check if nodes exists
	if !g.graph.IsNode(parent) {
		return errors.Errorf("Invalid parent, Sub Topology %s -> (%s)\nPlease refer the graph\n%s",
			subTp.Id(), edge.Parent().Id(), g.graph.String())
	}

	if !g.graph.IsNode(node) {
		return errors.Errorf("Invalid node, Sub Topology %s -> (%s)\nPlease refer the graph\n%s",
			subTp.Id(), edge.Node().Id(), g.graph.String())
	}

	return g.graph.AddEdge(parent, node, true, nil)
}

func (g *graphViz) addStateStore(parent string, stor string) error {
	strId := fmt.Sprintf(`"%s"`, stor)
	return g.graph.AddNode(parent, strId, map[string]string{
		`label`: fmt.Sprintf(`"%s"`, stor),
		`shape`: `cylinder`,
	})
}

func (g *graphViz) nodeLabel(node NodeBuilder, shortName string) string {
	if node.NodeName() != `` {
		return fmt.Sprintf(`"%s.%s\n%s"`, node.Id(), shortName, node.NodeName())
	}

	if node.Type().Name == `branch` {
		return fmt.Sprintf(`"%s.%s\n%s"`, node.Id(), shortName, node.Type().Attrs[`name`])
	}

	return fmt.Sprintf(`"%s.%s"`, node.Id(), shortName)
}

func (g *graphViz) applyAttributes(node NodeBuilder, attrs map[string]string) {
	attrs[`fontsize`] = `10`

	switch node.Type().Name {
	case `kSource`:
		attrs[`label`] = g.nodeLabel(node, `Source`)
		attrs[`fillcolor`] = `deepskyblue1`
		attrs[`style`] = `filled`
	case `sink`:
		attrs[`label`] = g.nodeLabel(node, `To`)
		attrs[`fillcolor`] = `deepskyblue1`
		attrs[`style`] = `filled`
		attrs[`tooltip`] = fmt.Sprintf(`"Partitioner:%s\n"`, node.Type().Attrs[`partitioner`])

	case `aggregator`:
		attrs[`label`] = g.nodeLabel(node, `AGG`)
		attrs[`fillcolor`] = `slateblue4`
		attrs[`fontcolor`] = `grey100`
		attrs[`style`] = `filled`

	case `branch`:
		attrs[`label`] = g.nodeLabel(node, `BR`)
		attrs[`fillcolor`] = `lightslateblue`
		attrs[`fontcolor`] = `grey100`
		attrs[`shape`] = `rectangle`
		attrs[`style`] = `"rounded,filled"`
	case `splitter`:
		attrs[`label`] = g.nodeLabel(node, `Splitter`)
		attrs[`fillcolor`] = `slateblue4`
		attrs[`fontcolor`] = `grey100`
		attrs[`fontname`] = `Arial`
		attrs[`style`] = `"rounded,filled"`
	case `for`:
		attrs[`label`] = g.nodeLabel(node, `ForEach`)
		attrs[`fillcolor`] = `slateblue4`
		attrs[`fontcolor`] = `grey100`
		attrs[`style`] = `filled`

	case `flatmap`:
		attrs[`label`] = g.nodeLabel(node, `FMap`)
		attrs[`fillcolor`] = `slateblue4`
		attrs[`fontcolor`] = `grey100`
		attrs[`style`] = `filled`

	case `flatmap_values`:
		attrs[`label`] = g.nodeLabel(node, `FMapVals`)
		attrs[`fillcolor`] = `slateblue4`
		attrs[`fontcolor`] = `grey100`
		attrs[`style`] = `filled`

	case `filter`:
		attrs[`label`] = g.nodeLabel(node, `F`)
		attrs[`fillcolor`] = `slateblue4`
		attrs[`fontcolor`] = `grey100`
		attrs[`style`] = `filled`

	case `key_selector`:
		attrs[`label`] = g.nodeLabel(node, `KS`)
		attrs[`fillcolor`] = `slateblue4`
		attrs[`fontcolor`] = `grey100`
		attrs[`style`] = `filled`

	case `merger`:
		attrs[`label`] = g.nodeLabel(node, `Merge`)
		attrs[`fillcolor`] = `slateblue4`
		attrs[`fontcolor`] = `grey100`
		attrs[`style`] = `filled`

	case `to_stream`:
		attrs[`label`] = g.nodeLabel(node, `ToStream`)
		attrs[`fillcolor`] = `slateblue4`
		attrs[`fontcolor`] = `grey100`
		attrs[`style`] = `filled`

	case `joiner`, `GTableJoiner`:
		attrs[`label`] = g.nodeLabel(node, fmt.Sprintf("Joiner(%s)", node.Type().Attrs[`side`]))
		attrs[`fillcolor`] = `slateblue4`
		attrs[`fontcolor`] = `grey100`
		attrs[`style`] = `filled`

	case `to_table`:
		attrs[`label`] = g.nodeLabel(node, `ToTable`)
		attrs[`fillcolor`] = `slateblue4`
		attrs[`fontcolor`] = `grey100`
		attrs[`style`] = `filled`

	case `value_mapper`:
		attrs[`label`] = g.nodeLabel(node, `MapVals`)
		attrs[`fillcolor`] = `slateblue4`
		attrs[`fontcolor`] = `grey100`
		attrs[`style`] = `filled`

	case `map`:
		attrs[`label`] = g.nodeLabel(node, `Map`)
		attrs[`fillcolor`] = `slateblue4`
		attrs[`fontcolor`] = `grey100`
		attrs[`style`] = `filled`

	case `global_table_processor`:
		attrs[`label`] = g.nodeLabel(node, `GlobalTable`)
		attrs[`fillcolor`] = `slateblue4`
		attrs[`fontcolor`] = `grey100`
		attrs[`style`] = `filled`

	default:
		attrs[`label`] = g.nodeLabel(node, node.Type().Name)
		attrs[`fillcolor`] = `slateblue4`
		attrs[`fontcolor`] = `grey100`
		attrs[`style`] = `filled`
	}
}
