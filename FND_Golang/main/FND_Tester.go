package main
import "C"
import (
	"fmt"
	"strings"
	"time"
)


//https://gist.github.com/helinwang/4f287a52efa4ab75424c01c65d77d939

//export py
func py(request string ) {

	request_array := strings.Split(request, " ")

	for _, curr := range request_array{


		if (curr == "Hello!"){
			SMA(seriesSource("USD CASH JPY IDEALPRO", "high"),30, "sma_2")
		}else{
			fmt.Println("HELLO PYTHON!!")
		}


	}

}

// go build -buildmode=c-shared -o libpy.so FND_Tester.go FND.go Broadcast.go FND_Compiler.go NodeQueue.go ServerClient.go  // TODO: restucutre packaging so command line build is easier to make.

func main(){


	//USD_EUR := seriesSource("EUR CASH USD IDEALPRO")
	//a := SMA(USD_EUR,30, "sma_1", "close")
	//SMA(USD_EUR,30, "sma_2", "close")
	//ADD(a, b)
	a := SMA(seriesSource("EUR CASH USD IDEALPRO" , "close"),30, "sma_0")
	b := SMA(seriesSource("GBP CASH USD IDEALPRO", "high"),30, "sma_1")
	SMA(seriesSource("USD CASH JPY IDEALPRO", "high"),30, "sma_2")
	SMA(seriesSource("AUD CASH USD IDEALPRO", "high"),30, "sma_3")
	SMA(seriesSource("USD CASH CAD IDEALPRO", "high"),30, "sma_4")
	SMA(seriesSource("NZD CASH USD IDEALPRO", "high"),30, "sma_5")
	//SMA(seriesSource("USD CASH JPY IDEALPRO", "high"),30, "sma_6")
	//SMA(seriesSource("AUD CASH USD IDEALPRO", "high"),30, "sma_7")
	//SMA(seriesSource("USD CASH CAD IDEALPRO", "high"),30, "sma_8")
	//SMA(seriesSource("NZD CASH USD IDEALPRO", "high"),30, "sma_9")
	ADD(a, b)
	ADD(a, a)
	ADD(b, b)

	//ADD(SMA(seriesSource("EUR CASH USD IDEALPRO"),30, "sma_3", "close"), SMA(seriesSource("EUR CASH USD IDEALPRO"),30, "sma_4", "close"))

	time.Sleep(1)
	barrier.Wait()



}


