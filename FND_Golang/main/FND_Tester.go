package main
import "C"
import (
	"time"
)


//https://gist.github.com/helinwang/4f287a52efa4ab75424c01c65d77d939


// TODO: Write compiler build parser

// go build -buildmode=c-shared -o libpy.so FND_Tester.go FND.go Broadcast.go FND_Compiler.go NodeQueue.go ServerClient.go  // TODO: restucutre packaging so command line build is easier to make.

func main(){

	//GTE(SMA(seriesSource("EUR CASH USD IDEALPRO", "close"), 2, "short"),
	//	SMA(seriesSource("EUR CASH USD IDEALPRO", "close"), 8, "long"), "2-SMA-STRATEGY")

	//short := SMA(seriesSource("EUR CASH USD IDEALPRO"), 2, "close","short")
	//long := SMA(seriesSource("EUR CASH USD IDEALPRO"), 3, "close","long")
	//sellOrder := GTE(short, long, "sellOrder")
	//buyOrder := GTE(long, short, "buyOrder")
	//buyOrder := GTE(SMA(seriesSource("EUR CASH USD IDEALPRO"), 3, "close","long"),
	//	SMA(seriesSource("EUR CASH USD IDEALPRO"), 2, "close","short"), "buyOrder")
	//sellOrder := GTE(SMA(seriesSource("EUR CASH USD IDEALPRO"), 2, "close","short"),
	//	SMA(seriesSource("EUR CASH USD IDEALPRO"), 3, "short","long"), "sellOrder")
	//SUBTRACT(sellOrder, buyOrder, "Subtract")
	//ADD(seriesSource("EUR CASH USD IDEALPRO"), seriesSource("EUR CASH USD IDEALPRO"), "add")
	//SUBTRACT(seriesSource("EUR CASH USD IDEALPRO"), seriesSource("EUR CASH USD IDEALPRO"), "subtract")
	//GTE(seriesSource("EUR CASH USD IDEALPRO"), seriesSource("EUR CASH USD IDEALPRO"), "gte")

	//simple_2SMA_Strategy(seriesSource("EUR CASH USD IDEALPRO"), 2,3,20,"close","2SMA_Strategy" )
	simple_2EMA_strategy(seriesSource("EUR CASH USD IDEALPRO"), 2, 3, 20, "close", "2EMA_Strategy")
	time.Sleep(1)
	barrier.Wait()
}


