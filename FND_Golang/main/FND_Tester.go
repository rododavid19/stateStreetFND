package main

import (
	"fmt"
	"github.com/rocketlaunchr/dataframe-go"
)




// https://1forge.com/forex-data-api
func main(){


	s1 := dataframe.NewSeriesInt64("day", nil,1, 2, 3, 4, 5, 6, 7, 8 )
	s2 := dataframe.NewSeriesFloat64("sales", nil, 50.3, 23.4, 56.2, nil, nil, 84.2, 72, 89)
	df := dataframe.NewDataFrame(s1, s2)
	fmt.Println(df.Table())

	//a := seriesSource("day")
	n := Network{}
	//n.MACD(a, "" )

	n.SMA(seriesSource("day"), 10, "sma_1")
	n.SMA(seriesSource("fake"), 20, "sma_2")
	n.SMA(seriesSource("day"), 30, "sma_3")
	//fmt.Println(n.String())
	n.piEval()

}