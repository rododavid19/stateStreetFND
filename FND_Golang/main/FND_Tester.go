package main
import "C"
import (
	"strings"
	"time"
)


//https://gist.github.com/helinwang/4f287a52efa4ab75424c01c65d77d939


// TODO: Write compiler build parser

//export py
func py(request string ) {
	request_array := strings.Split(request, " ")
	for _, curr := range request_array{
		if (curr == "Hello!"){
			SMA(seriesSource("USD CASH JPY IDEALPRO", "high"),30, "sma_2")
			continue
		}
		if(curr == "SMA"){
		}
	}
}

// go build -buildmode=c-shared -o libpy.so FND_Tester.go FND.go Broadcast.go FND_Compiler.go NodeQueue.go ServerClient.go  // TODO: restucutre packaging so command line build is easier to make.

func main(){

	//GTE(SMA(seriesSource("EUR CASH USD IDEALPRO", "close"), 2, "short"),
	//	SMA(seriesSource("EUR CASH USD IDEALPRO", "close"), 8, "long"), "2-SMA-STRATEGY")

	//short := SMA(seriesSource("EUR CASH USD IDEALPRO", "close"), 2, "short")
	//long := SMA(seriesSource("EUR CASH USD IDEALPRO", "close"), 8, "long")
	//sellOrder := GTE(short, long, "sellOrder")
	//buyOrder := GTE(long, short, "buyOrder")
	buyOrder := GTE(SMA(seriesSource("EUR CASH USD IDEALPRO", "close"), 3, "long"),
		SMA(seriesSource("EUR CASH USD IDEALPRO", "close"), 2, "short"), "buyOrder")
	sellOrder := GTE(SMA(seriesSource("EUR CASH USD IDEALPRO", "close"), 2, "short"),
		SMA(seriesSource("EUR CASH USD IDEALPRO", "close"), 3, "long"), "sellOrder")
	SUBTRACT(sellOrder, buyOrder, "Subtract")
	time.Sleep(1)
	barrier.Wait()
}


