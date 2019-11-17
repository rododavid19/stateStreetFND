package main

import (
	"fmt"
	"net"
	"sync"
)

// operator overloading

var SOURCE_TYPES  = [2]string{"seriesSource", "dataFrame"}

// Sink Type


// Node a single node that composes the tree
type Node struct {
	name string  // TODO: change this to type_name
	primitive Primitive
	module Module
	isPrimitive bool
	children []Node
	data int
	distributor Broadcaster

}



func (n *Node) String() string {
	return fmt.Sprintf("%v", n.name)
}

// ItemGraph the Items graph
type Network struct {
	nodes []*Node
	edges map[string][]*Node
	lock  sync.RWMutex
	sources []string
}


// Traverse implements the BFS traversing algorithm
//func (g *Network) Traverse(f func(*Node)) {
//	g.lock.RLock()
//	q := NodeQueue{}
//	q.New()
//	n := g.nodes[0]
//	q.Enqueue(*n)
//	visited := make(map[*Node]bool)
//	for {
//		if q.IsEmpty() {
//			break
//		}
//		node := q.Dequeue()
//		visited[node] = true
//		near := g.edges[node.name]
//
//		for i := 0; i < len(near); i++ {
//			j := near[i]
//			if !visited[j] {
//				q.Enqueue(*j)
//				visited[j] = true
//			}
//		}
//		if f != nil {
//			f(node)
//		}
//	}
//	g.lock.RUnlock()
//}


// TODO: printReport()
//func (g *Network) TestTraverse() {
//
//	g.Traverse(func(n *Node) {
//		fmt.Printf("%v\n", n)
//	})
//}


func (g *Network) String() string {
	g.lock.RLock()
	s := ""
	for i := 0; i < len(g.nodes); i++ {
		s += g.nodes[i].String() + " -> "
		near := g.edges[g.nodes[i].name]

		// TODO: delete bottom loop. Replace with g.node[i].Children and iterate thru each one and pritn the name
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
}

func (s Series) init( name string){


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
	window, span int
	isType, name string
}




func (n *Network) pushPrimitive( prim *Node){
	n.AddNode(prim)
}


func (n *Network) pushModule( module *Node, primitives *[]Node ){
	n.AddNode(module)
	module.children = *primitives
	//for _, curr := range *primitives {
	//	cp := Node{ name:curr.name}
	//	n.AddEdge(module, &cp )
	//}
}


// ### Source Operators ###


var handlers = map[string]Broadcaster{}


func seriesSource(source string, priceType string ) *Receiver {
	// addNode, this will be shared by everyone
	//defer barrier.Done()



	if _, ok := handlers[source]; ok {

		listener := handlers[source].Listen()
		listener.name = source
		return &listener
	}

	composer := NewBroadcaster()
	handlers[source] = composer
	composer.name = source

	go sinkSource(&composer, source + " " + priceType)

	listener := composer.Listen()
	listener.name = source +  " " + priceType


	return &listener


}



func sinkSource( composer *Broadcaster, contract string){

	var hostName = "127.0.0.1"
	var portNum =  "19192"
	var service = hostName + ":" + portNum
	var RemoteAddr, _ = net.ResolveUDPAddr("udp", service)
	println("Starting port: " , portNum)
	var conn, _ = net.DialUDP("udp", nil, RemoteAddr)


	message := []byte(contract)  // specify contract details, max period,
	_, _ = conn.Write(message)

	for{
		buffer := make([]byte, 512)
		data, _, _ := conn.ReadFromUDP(buffer)
		if data > 0{ }
		data_s := string(buffer[:])
		composer.Write(data_s)
	}

}

