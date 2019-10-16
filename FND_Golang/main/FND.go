package main

import (
	"fmt"
	"sync"
)

// operator overloading

var SOURCE_TYPES  = [2]string{"seriesSource", "dataFrame"}
// Sink Type


// Node a single node that composes the tree
type Node struct {
	name string
	primitive Primitive
	module Module
	isPrimitive bool

}



func (n *Node) String() string {
	return fmt.Sprintf("%v", n.name)
}

// ItemGraph the Items graph
type Network struct {
	nodes []*Node
	edges map[string][]*Node
	lock  sync.RWMutex
}


// Traverse implements the BFS traversing algorithm
func (g *Network) Traverse(f func(*Node)) {
	g.lock.RLock()
	q := NodeQueue{}
	q.New()
	n := g.nodes[0]
	q.Enqueue(*n)
	visited := make(map[*Node]bool)
	for {
		if q.IsEmpty() {
			break
		}
		node := q.Dequeue()
		visited[node] = true
		near := g.edges[node.name]

		for i := 0; i < len(near); i++ {
			j := near[i]
			if !visited[j] {
				q.Enqueue(*j)
				visited[j] = true
			}
		}
		if f != nil {
			f(node)
		}
	}
	g.lock.RUnlock()
}


// TODO: printReport()
func (g *Network) TestTraverse() {

	g.Traverse(func(n *Node) {
		fmt.Printf("%v\n", n)
	})
}


func (g *Network) String() string {
	g.lock.RLock()
	s := ""
	for i := 0; i < len(g.nodes); i++ {
		s += g.nodes[i].String() + " -> "
		near := g.edges[g.nodes[i].name]
		for j := 0; j < len(near); j++ {
			s += near[j].String()
			if j < len(near) - 1{
				s += ", "
			}

		}
		s += "\n"
	}
	//fmt.Println(s)
	g.lock.RUnlock()
	return s
}


// AddNode adds a node to the graph
func (g *Network) AddNode(n *Node) {
	g.lock.Lock()
	g.nodes = append(g.nodes, n)
	g.lock.Unlock()
}

// AddEdge adds an edge to the graph (MODULES ONLY)
func (g *Network) AddEdge(n1, n2 *Node) {
	g.lock.Lock()
	if g.edges == nil {
		g.edges = make(map[string][]*Node)
	}
	g.edges[n1.name] = append(g.edges[n1.name], n2)
	//g.edges[n2.name] = append(g.edges[n2.name], n1)
	g.lock.Unlock()
}







func ( n *Network) Init( ) {

}



type Series struct{
	name string
	isType string
	//  :(   Operator Overload
}

func (s Series) init( name string){


}





func networkSingleton () {
	// ? Still slightly unclear as to what's going on here
}

type DataFrame struct{
	// init
	// getItem

}

type Module struct{
	argument_a Series
	shortSpan, longSpan, signalSpan int
	isType, name string
	prims []Series
	// init
}

type Primitive struct{
	argument_a Series
	argument_b Series
	window int
	isType, name string

}



func seriesSource(name string ) Series{
	// addNode, this will be shared by everyone
	return Series{name, "seriesSource"}

}

func (n *Network) pushPrimitive( prim *Node){
	n.AddNode(prim)
}


func (n *Network) pushModule( module *Node, primitives *[]Node ){
	n.AddNode(module)
	for _, curr := range *primitives {
		cp := Node{ name:curr.name}
		n.AddEdge(module, &cp )
	}
}


func (n *Network) SMA(a Series, window int, optionalName string){
	prim := Node{name:"SMA", isPrimitive:true, primitive:Primitive{argument_a:a, isType:a.isType, name:optionalName, window:window}}
	n.pushPrimitive(&prim)
}


func (n *Network) MACD(a Series, optionalName string){

	module := Node{name:"MACD", isPrimitive:false, module:Module{argument_a:a, isType:a.isType, name:optionalName, shortSpan:12, longSpan:22, signalSpan:9}}
	primitives := make([]Node, 0)
	primitives = append(primitives, Node{name:"EMA", isPrimitive:true, })
	primitives = append(primitives, Node{name:"EMA", isPrimitive:true, })
	primitives = append(primitives, Node{name:"Subtract", isPrimitive:true, })
	primitives = append(primitives, Node{name:"EMA", isPrimitive:true, })
	primitives = append(primitives, Node{name:"Subtract", isPrimitive:true, })
	primitives = append(primitives, Node{name:"DataFrame", isPrimitive:true, })
	n.pushModule( &module, &primitives)
}






// TODO: MethodByName() call, requires that first letter is capitalized!!!!!

