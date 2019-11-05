package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

var OPERATORS = map[string]func(node *Node){
	"sma": sma,
	"add": add,
}

var contracts = []string{}

var file, err_csv = os.Create("result.csv")

var results = sync.Map{}

var barrier sync.WaitGroup


func add(n *Node){


}



func sma(n *Node ){

	bidPrices := []float64{}		// treat like necessary value cache
	askPrices := []float64{}

	r := composer.Listen()
	for v := r.Read(); v != nil; v = r.Read() {

		fmt.Println( n.primitive.name, " RECIEVED: ", v);
		data_raw := fmt.Sprintf( "%v", v)
		data_split := strings.SplitAfter(data_raw, " ")
		got_bid_price := false
		got_ask_price := false

	for i, _ := range data_split{

		curr_fromEnd := data_split[len(data_split)-i-1]

		if(curr_fromEnd == "BIDPrice: " && !got_bid_price){
			bid_price := data_split[len(data_split)-i]
			bid_price = strings.TrimSpace(bid_price)
			bid_Float, _ := strconv.ParseFloat(bid_price, 64)
			bidPrices = append(bidPrices, bid_Float)
			got_bid_price = true
		}

		if(curr_fromEnd == "ASKPrice: " && !got_ask_price){
			ask_price := data_split[len(data_split)-i]
			ask_price = strings.TrimSpace(ask_price)
			ask_Float, _ := strconv.ParseFloat(ask_price, 64)
			askPrices = append(askPrices, ask_Float)
			got_ask_price = true
		}

		if (got_bid_price && got_ask_price){
			fmt.Println( n.primitive.name, " EXTRACTED: ",  "bid: ",bidPrices[len(bidPrices)-1], "ask: ", askPrices[len(askPrices)-1] );
			break
		}

	}

	if(got_bid_price && !got_ask_price){
		fmt.Println( n.primitive.name, " BID EXTRACTED: ",bidPrices[len(bidPrices)-1] );
	}

	if(!got_bid_price && got_ask_price){
		fmt.Println( n.primitive.name, " ASK EXTRACTED: ", askPrices[len(askPrices)-1] );

	}





}


	//defer barrier.Done()

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
	fmt.Println(time.Now())






}


