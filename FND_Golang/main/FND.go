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
	name string  // TODO: change this to type_name
	primitive Primitive
	module Module
	isPrimitive bool
	children []Node
	data int

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

func seriesSource(name string ) {
	// addNode, this will be shared by everyone
	//defer barrier.Done()

	sourceDict := map[string]string{
		"eur/usd": "EUR CASH USD IDEALPRO",
	}

	message := []byte(sourceDict[name] )  // specify contract details, max period,
	_, err_ = conn.Write(message)


	// receive message from server
	//buffer := make([]byte, 512)
	//data, _, _ := conn.ReadFromUDP(buffer)


	//fmt.Println("UDP Server : ", addr)
	//  hotData <- string(buffer[:n])
	//fmt.Println("Received from UDP server : ", string(buffer[:data]) + " error code: " )


	for{
		buffer := make([]byte, 512)
		data, _, _ := conn.ReadFromUDP(buffer)
		if data > 0{ }
		data_s := string(buffer[:])
		// TODO: make dataframe and dist. in that format?
		//fmt.Println("RECIEVED", data_s, " at time ", 	time.Now())
		composer.Write(data_s)
		//composer <- data_s,  TODO: old version, one-to-one
	}

}




// ## Arithmetic Operator //

func (n *Network) Add (a Series, b Series, optionalName string){
	prim := Node{name:"add", isPrimitive:true, primitive:Primitive{argument_a:a, argument_b:b, isType:a.isType, name:optionalName}}
	n.pushPrimitive(&prim)
}

func (n *Network) Subtract (a Series, b Series, optionalName string){
	prim := Node{name:"subtract", isPrimitive:true, primitive:Primitive{argument_a:a, argument_b:b, isType:a.isType, name:optionalName}}
	n.pushPrimitive(&prim)
}


func (n *Network) Multiply (a Series, b Series, optionalName string){
	prim := Node{name:"multiply", isPrimitive:true, primitive:Primitive{argument_a:a, argument_b:b, isType:a.isType, name:optionalName}}
	n.pushPrimitive(&prim)
}

func (n *Network) Divide (a Series, b Series, optionalName string){
	prim := Node{name:"divide", isPrimitive:true, primitive:Primitive{argument_a:a, argument_b:b, isType:a.isType, name:optionalName}}
	n.pushPrimitive(&prim)
}

func (n *Network) Neg(a Series, b Series, optionalName string){
	prim := Node{name:"neg", isPrimitive:true, primitive:Primitive{argument_a:a, argument_b:b, isType:a.isType, name:optionalName}}
	n.pushPrimitive(&prim)
}


func (n *Network) Abs(a Series, span int, optionalName string){
	prim := Node{name:"abs", isPrimitive:true, primitive:Primitive{argument_a:a, isType:a.isType, name:optionalName, span:span}}
	n.pushPrimitive(&prim)
}

func (n *Network) Remainder(a Series, span int, optionalName string){
	prim := Node{name:"remainder", isPrimitive:true, primitive:Primitive{argument_a:a, isType:a.isType, name:optionalName, span:span}}
	n.pushPrimitive(&prim)
}


func (n *Network) Floor(a Series, span int, optionalName string){
	prim := Node{name:"floor", isPrimitive:true, primitive:Primitive{argument_a:a, isType:a.isType, name:optionalName, span:span}}
	n.pushPrimitive(&prim)
}

func (n *Network) Ceiling(a Series, span int, optionalName string){
	prim := Node{name:"ceiling", isPrimitive:true, primitive:Primitive{argument_a:a, isType:a.isType, name:optionalName, span:span}}
	n.pushPrimitive(&prim)
}

func (n *Network) Log(a Series, span int, optionalName string){
	prim := Node{name:"log", isPrimitive:true, primitive:Primitive{argument_a:a, isType:a.isType, name:optionalName, span:span}}
	n.pushPrimitive(&prim)
}

func (n *Network) lessThan(a Series, span int, optionalName string){
	prim := Node{name:"lessThan", isPrimitive:true, primitive:Primitive{argument_a:a, isType:a.isType, name:optionalName, span:span}}
	n.pushPrimitive(&prim)
}

func (n *Network) lessOrEqual(a Series, span int, optionalName string){
	prim := Node{name:"lessOrEqual", isPrimitive:true, primitive:Primitive{argument_a:a, isType:a.isType, name:optionalName, span:span}}
	n.pushPrimitive(&prim)
}

func (n *Network) Equal(a Series, span int, optionalName string){
	prim := Node{name:"equal", isPrimitive:true, primitive:Primitive{argument_a:a, isType:a.isType, name:optionalName, span:span}}
	n.pushPrimitive(&prim)
}

func (n *Network) greaterOrEqual(a Series, span int, optionalName string){
	prim := Node{name:"greaterOrEqual", isPrimitive:true, primitive:Primitive{argument_a:a, isType:a.isType, name:optionalName, span:span}}
	n.pushPrimitive(&prim)
}

func (n *Network) notEqual(a Series, span int, optionalName string){
	prim := Node{name:"notEqual", isPrimitive:true, primitive:Primitive{argument_a:a, isType:a.isType, name:optionalName, span:span}}
	n.pushPrimitive(&prim)
}

func (n *Network) greaterThan(a Series, span int, optionalName string){
	prim := Node{name:"greaterThan", isPrimitive:true, primitive:Primitive{argument_a:a, isType:a.isType, name:optionalName, span:span}}
	n.pushPrimitive(&prim)
}


// Fixed-Size Rolling Windows //

func (n *Network) SMA( window int, optionalName string){

	prim := Node{name:"sma", isPrimitive:true, primitive:Primitive{ name:optionalName, window:window}}
	n.pushPrimitive(&prim)
}


func (n *Network) STDEV(a Series, span int, optionalName string){
	prim := Node{name:"stdev", isPrimitive:true, primitive:Primitive{argument_a:a, isType:a.isType, name:optionalName, span:span}}
	n.pushPrimitive(&prim)
}

func (n *Network) Min(a Series, span int, optionalName string){
	prim := Node{name:"min", isPrimitive:true, primitive:Primitive{argument_a:a, isType:a.isType, name:optionalName, span:span}}
	n.pushPrimitive(&prim)
}

func (n *Network) Max(a Series, span int, optionalName string){
	prim := Node{name:"max", isPrimitive:true, primitive:Primitive{argument_a:a, isType:a.isType, name:optionalName, span:span}}
	n.pushPrimitive(&prim)
}

func (n *Network) Sum(a Series, span int, optionalName string){
	prim := Node{name:"sum", isPrimitive:true, primitive:Primitive{argument_a:a, isType:a.isType, name:optionalName, span:span}}
	n.pushPrimitive(&prim)
}

func (n *Network) Delay(a Series, span int, optionalName string){
	prim := Node{name:"delay", isPrimitive:true, primitive:Primitive{argument_a:a, isType:a.isType, name:optionalName, span:span}}
	n.pushPrimitive(&prim)
}


// Exponentially Weighted //
func (n *Network) EMA(a Series, window int, optionalName string){
	prim := Node{name:"ema", isPrimitive:true, primitive:Primitive{argument_a:a, isType:a.isType, name:optionalName, window:window}}
	n.pushPrimitive(&prim)
}



// Time Interval //

func (n *Network) intervalMean(a Series, window int, optionalName string){
	prim := Node{name:"intervalMean", isPrimitive:true, primitive:Primitive{argument_a:a, isType:a.isType, name:optionalName, window:window}}
	n.pushPrimitive(&prim)
}

func (n *Network) intervalSTDEV(a Series, window int, optionalName string){
	prim := Node{name:"intervalSTDEV", isPrimitive:true, primitive:Primitive{argument_a:a, isType:a.isType, name:optionalName, window:window}}
	n.pushPrimitive(&prim)
}

func (n *Network) intervalMin(a Series, window int, optionalName string){
	prim := Node{name:"intervalMin", isPrimitive:true, primitive:Primitive{argument_a:a, isType:a.isType, name:optionalName, window:window}}
	n.pushPrimitive(&prim)
}

func (n *Network) intervalMax(a Series, window int, optionalName string){
	prim := Node{name:"intervalMax", isPrimitive:true, primitive:Primitive{argument_a:a, isType:a.isType, name:optionalName, window:window}}
	n.pushPrimitive(&prim)
}

func (n *Network) intervalSum(a Series, window int, optionalName string){
	prim := Node{name:"intervalSum", isPrimitive:true, primitive:Primitive{argument_a:a, isType:a.isType, name:optionalName, window:window}}
	n.pushPrimitive(&prim)
}

func (n *Network) intervalCount(a Series, window int, optionalName string){
	prim := Node{name:"intervalCount", isPrimitive:true, primitive:Primitive{argument_a:a, isType:a.isType, name:optionalName, window:window}}
	n.pushPrimitive(&prim)
}


// Time-Weighted //

func (n *Network) timeWeightedMean(a Series, window int, optionalName string){
	prim := Node{name:"timeWeightedMean", isPrimitive:true, primitive:Primitive{argument_a:a, isType:a.isType, name:optionalName, window:window}}
	n.pushPrimitive(&prim)
}

func (n *Network) timeWeightedSTDEV(a Series, window int, optionalName string){
	prim := Node{name:"timeWeightedSTDEV", isPrimitive:true, primitive:Primitive{argument_a:a, isType:a.isType, name:optionalName, window:window}}
	n.pushPrimitive(&prim)
}




// Modules //
func (n *Network) MACD(a Series, optionalName string){

	//if strings.Contains(optionalName, "MACD_")
	if !contains(a.name, n.sources){
		// TODO: cache if many sources are created. IE. ask Bishop how many sources are usually used.
		n.sources = append(n.sources, a.name)
	}

	module := Node{name:"MACD", isPrimitive:false, module:Module{argument_a:a, isType:a.isType, name:optionalName, shortSpan:12, longSpan:22, signalSpan:9}}
	primitives := make([]Node, 0)
	primitives = append(primitives, Node{name:"add", isPrimitive:true, })
	primitives = append(primitives, Node{name:"add", isPrimitive:true, })
	primitives = append(primitives, Node{name:"add", isPrimitive:true, })
	primitives = append(primitives, Node{name:"add", isPrimitive:true, })
	primitives = append(primitives, Node{name:"add", isPrimitive:true, })
	primitives = append(primitives, Node{name:"add", isPrimitive:true, })
	n.pushModule( &module, &primitives)
}


func contains( target string ,sources []string) bool {

	for _, curr := range sources{

		if curr == target{
			return true
		}

	}

	return false
}



// TODO: MethodByName() call, requires that first letter is capitalized!!!!!

