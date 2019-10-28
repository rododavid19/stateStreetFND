package main

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"
	"time"
)

var OPERATORS = map[string]func(node *Node){
	"sma": sma,
	"add": add,
}

var file, err_csv = os.Create("result.csv")

var results = sync.Map{}

var barrier sync.WaitGroup


func add(n *Node){

	// TODO: individual routines that will check data source and compute as data is available.

	fmt.Println( n.name  )

}


func csvLaunch() {



}

func sma(n *Node ){

	defer barrier.Done()


	message := []byte("EUR CASH USD IDEALPRO " + strconv.Itoa(n.primitive.window) )  // specify contract details, max period,
	_, err_ = conn.Write(message)

	if err_ != nil {
		log.Println(err_)
	}

	// receive message from server
	buffer := make([]byte, 1024)
	data, _, _ := conn.ReadFromUDP(buffer)

	results.Store( n, 5 )

	//fmt.Println("UDP Server : ", addr)
	//  hotData <- string(buffer[:n])
	//fmt.Println("Received from UDP server : ", string(buffer[:data]) + " error code: " )

	fmt.Println(" count is ", count, " with data ", data, " requested by Primitive: " , n.primitive.name )

}


var sourceOracle = make(map[string][]int, 5)


var count = 0

func (n *Network) piEval(){
//	go startServer()// server should now continuously grab data
	// use channel to communicate



//	sizeSources := len(n.sources)
//	provider := make(map[string]chan[]int, sizeSources)

	//for _, src := range n.sources{
	//
	//	// TODO: go startDataClient(contract). Establish connection and update global sourceOracle thru channels.
	//
	//
	//
	//}


	for _, curr := range n.nodes{

		// TODO: establish data sources at the top


		if len(curr.children) > 0{

			fmt.Println( curr.name  )

			for _, child := range curr.children{
				var f  = OPERATORS[child.name]
				go f(&child)
				time.Sleep(time.Nanosecond)
			}
			//time.Sleep(time.Nanosecond)
		}else{

			var f  = OPERATORS[curr.name]
			count += 1
			barrier.Add(1)
			go f(curr)
			time.Sleep(time.Nanosecond)
		}


	}

	barrier.Wait()




}


