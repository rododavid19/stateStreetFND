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

	//a := SMA(seriesSource("EUR CASH USD IDEALPRO" , "close"),30, "sma_0")
	//b := SMA(seriesSource("GBP CASH USD IDEALPRO", "high"),30, "sma_1")
	SMA(seriesSource("USD CASH CAD IDEALPRO", "high"),30, "sma_4")
	SMA(seriesSource("NZD CASH USD IDEALPRO", "high"),30, "sma_5")

	ADD(SMA(seriesSource("USD CASH JPY IDEALPRO", "high"),30, "sma_2"), SMA(seriesSource("USD CASH JPY IDEALPRO", "high"),30, "sma_3"))
	//ADD(a, b)
	//ADD(b, b)
	ADD(SMA(seriesSource("EUR CASH USD IDEALPRO", "close"),30, "sma_6"), SMA(seriesSource("NZD CASH USD IDEALPRO", "close"),30, "sma_7"))
	time.Sleep(1)
	barrier.Wait()

}


