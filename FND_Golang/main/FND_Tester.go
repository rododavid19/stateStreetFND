package main



var composer = NewBroadcaster()

func main(){


	n := Network{}


	go seriesSource("eur/usd")
	n.SMA(30, "sma_1")
	n.SMA(20, "sma_2")
	n.SMA( 30, "sma_3")
	n.piEval()
}



// TODO: 5 sec, 25/100
