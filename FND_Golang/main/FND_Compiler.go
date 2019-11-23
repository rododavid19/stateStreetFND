package main

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"
)

var barrier sync.WaitGroup

var compiler_handlers = map[string]Broadcaster{}

var registered_recievers = map[string]bool{}

var price_map = map[string]int{
	"open" : 0,
	"high" : 1,
	"low" : 2,
	"close" : 3,
}

func ADD(a *Receiver, b *Receiver) Receiver{



	add_toSring := a.name + " + " + b.name

	if _, ok := compiler_handlers[add_toSring]; ok {

		listener := compiler_handlers[add_toSring].Listen()
		return listener
	}



	if _, ok := registered_recievers[a.name]; ok {
		list := compiler_handlers[a.name].Listen()
		list.name = a.name
		a = &list
	}

	if _, ok := registered_recievers[b.name]; ok {

		if a!= b{
			list := compiler_handlers[b.name].Listen()
			list.name = b.name
			b = &list
		}

	}

	registered_recievers[a.name] = true
	if a != b {
		registered_recievers[b.name] = true
	}



	composer := NewBroadcaster()
	compiler_handlers[add_toSring] = composer

	fmt.Println( "ADD" + add_toSring + " created at ", time.Now()  );
	go add_eval( a, b, &composer, add_toSring)

	listener := composer.Listen()



	return listener


}

func add_eval(source_a *Receiver, source_b *Receiver, output *Broadcaster, id string){
	if( source_a == source_b){
		for b := source_b.Read(); b != nil; b = source_b.Read() {
			data_b := fmt.Sprintf("%v", b)
			println("SAME SOURCE ADD prim is adding: " + data_b + " + " + data_b )
		}

	}
	for a := source_a.Read(); a != nil; a = source_a.Read() {
		for b := source_b.Read(); b != nil; {
			data_a := fmt.Sprintf("%v", a)
			data_b := fmt.Sprintf("%v", b)
			println(  "ADD: ", id, " ", data_a,  " + " + data_b )
			break
		}
	}
}

//TODO: add mapping of priceType string to index in

func SMA( seriesSource *Receiver, window int, optionalName string ) *Receiver {

	if _, ok := compiler_handlers[optionalName]; ok {

		listener := compiler_handlers[optionalName].Listen()
		return &listener
	}

	 composer := NewBroadcaster()
	 compiler_handlers[optionalName] = composer

	fmt.Println( optionalName + " created at ", time.Now()  );
	go sma_eval( seriesSource,&composer, optionalName, window)

	listener := composer.Listen()
	listener.name = optionalName

	 return &listener

	}
	//defer barrier.Done()

func boolToInt(seriesSource *Receiver, optionalName string) *Receiver {
	if _, ok := compiler_handlers[optionalName]; ok {

		listener := compiler_handlers[optionalName].Listen()
		return &listener
	}

	composer := NewBroadcaster()
	compiler_handlers[optionalName] = composer

	fmt.Println( optionalName + " created at ", time.Now()  );
	go boolToInt_eval(seriesSource,&composer, optionalName)

	listener := composer.Listen()
	listener.name = optionalName
	return &listener
}

func boolToInt_eval(source *Receiver, output *Broadcaster, id string){
	barrier.Add(1)
	r := source
	for v := r.Read(); v != nil; v = r.Read() {
		b, err := strconv.ParseBool(v.(string))
		if err == nil{
			bitSetVar := int8(0)
			if b {
				bitSetVar = 1
			}
			output.Write(bitSetVar)
		}
		fmt.Println(id, " RECEIVED at time ", time.Now()   , " from receiver ", source.name  ); //v);
	}
}




func sma_eval( source *Receiver, output *Broadcaster, id string, window int) {

	barrier.Add(1)

	//openPrices := []float64{}
	//highPrices := []float64{}
	//composer := NewBroadcaster()
	closePrices := []float64{}

	r := source
	for v := r.Read(); v != nil; v = r.Read() {
		close_price := strings.TrimSuffix(strings.Split(v.(string), " ")[5], "$")
		close_float, _ := strconv.ParseFloat(close_price, 64)
		ret_value := math.NaN()
		if len(closePrices) == window{
			closePrices = shift(closePrices)
		}
		closePrices = append(closePrices, close_float)
		if len(closePrices) == window{
			var sum float64 = 0 					//returning avg
			for i:= 0; i < len(closePrices); i++{
				sum = sum + closePrices[i]
			}
			ret_value = sum/float64(len(closePrices))
		}
		//var num_sets = float64(len(res1))/8 //count how many sets of data are there
		//sets_int := math.Round(num_sets)
		//if num_sets > sets_int{
		//	sets_int += 1
		//}
		//for i := 0.0 ; i < sets_int; i++{ //if array length is window size, shift the array to accommodate how many closing prices will be added
		//	if float64(len(closePrices)) + i > float64(window){
		//		closePrices = shift(closePrices)
		//	}
		//}
		//for i := 0; i < len(res1); i++{
		//	if strings.Contains(res1[i], "Close"){
		//		close_price := strings.Split(res1[i], " ")
		//		close_float, _ := strconv.ParseFloat(close_price[2], 64)
		//		closePrices = append(closePrices, close_float)
		//	}
		//}

		output.Write(ret_value)
		fmt.Println(id, " RECEIVED at time ", time.Now()   , " from receiver ", source.name  ); //v);
	}
	//TODO: use mapping of index to priceType to access correct data in data_split
}

func GTE(seriesSourceShort *Receiver, seriesSourceLong *Receiver, optionalName string ) *Receiver {

	if _, ok := compiler_handlers[optionalName]; ok {

		listener := compiler_handlers[optionalName].Listen()
		return &listener
	}

	composer := NewBroadcaster()
	compiler_handlers[optionalName] = composer

	fmt.Println( optionalName + " created at ", time.Now()  );
	go gte_eval(seriesSourceShort, seriesSourceLong, &composer, optionalName)

	listener := composer.Listen()
	listener.name = optionalName

	return &listener

}
//defer barrier.Done()


/* greater than only checks to see if the first source is greater than or equal the second*/
func gte_eval(short *Receiver, long *Receiver, output *Broadcaster, id string) {

	barrier.Add(1)

	for long_read := long.Read(); long_read != nil; long_read = long.Read() {
		short_read := short.Read()
		fmt.Println(id, " RECEIVED at time ", time.Now()   , " from receiver ", short.name  ); //v);
		fmt.Println(id, " RECEIVED at time ", time.Now()   , " from receiver ", long.name  ); //v);
		data_long := fmt.Sprintf("%v", long_read)
		if data_long == "NaN"{
			continue
		}
		short_float, _ := strconv.ParseFloat(fmt.Sprintf("%v", short_read), 64)
		long_float, _ := strconv.ParseFloat(data_long, 64)

		if short_float > long_float{
			output.Write(1)
			fmt.Println("short: ",short_float, "long: ", long_float, "action: BUY")
		} else if short_float < long_float{
			output.Write(-1)
			fmt.Println("short: ",short_float, "long: ", long_float, "action: SELL")
		} else {
			output.Write(0)
			fmt.Println("short: ",short_float, "long: ", long_float, "action: NONE")
		}
	}
}

func shift(n []float64) []float64{
	for i := 0; i < len(n)-1; i++ {
		n[i] = n[i+1]
	}
	n = n[:len(n)-1]
	return n
}
//
//func comparator(a ma_ret, b ma_ret) int{
//	if a.window > b.window { //a long
//		if a.value > b.value{
//			return 1 //buy
//		} else if a.value < b.value {
//			return -1 //sell
//		} else {
//			return 0 // do nothing
//		}
//	} else if a.window < b.window {
//		if a.value > b.value{ //a short
//			return -1 //sell
//		} else if a.value < b.value{
//			return 1 //buy
//		} else {
//			return 0 //do nothing
//		}
//	} else {
//		return 0 // do nothing
//	}
//}
//

//type ma_ret struct {
//	value float64
//	window  int
//}
//
//func sma(n *Node){
//
//	//bidPrices := []float64{}		// treat like necessary value cache
//	//askPrices := []float64{}
//	closePrices := []float64{}
//
//	r := composer.Listen()
//	for v := r.Read(); v != nil; v = r.Read() {
//
//		if len(closePrices) == n.primitive.window{
//			closePrices = shift(closePrices)
//		}
//
//		fmt.Println( n.primitive.name, " RECIEVED: ", v)
//		data_raw := fmt.Sprintf( "%v", v)
//		data_split := strings.SplitAfter(data_raw, " ")
//		//got_bid_price := false
//		//got_ask_price := false
//		got_close_price := false
//
//		for i, _ := range data_split{
//
//			curr_fromEnd := data_split[len(data_split)-i-1]
//
//			if(curr_fromEnd == "CLOSEPrice: " && !got_close_price){
//				close_price := data_split[len(data_split)-i]
//				close_price = strings.TrimSpace(close_price)
//				close_Float, _ := strconv.ParseFloat(close_price, 64)
//				closePrices = append(closePrices, close_Float)
//				got_close_price = true
//			}
//
//			//if(curr_fromEnd == "ASKPrice: " && !got_ask_price){
//			//	ask_price := data_split[len(data_split)-i]
//			//	ask_price = strings.TrimSpace(ask_price)
//			//	ask_Float, _ := strconv.ParseFloat(ask_price, 64)
//			//	askPrices = append(askPrices, ask_Float)
//			//	got_ask_price = true
//			//}
//
//			//if (got_bid_price && got_ask_price){
//			//	fmt.Println( n.primitive.name, " EXTRACTED: ",  "bid: ",bidPrices[len(bidPrices)-1], "ask: ", askPrices[len(askPrices)-1] );
//			//	break
//			//}
//			if got_close_price {
//				fmt.Println( n.primitive.name, " EXTRACTED: ",  "close: ",closePrices[len(closePrices)-1])
//				break
//			}
//
//		}
//		var sum float64 = 0 					//returning avg
//		for i:= 0; i < len(closePrices); i++{
//			sum = sum + closePrices[i]
//		}
//
//		var ret ma_ret
//		ret.value = sum/float64(len(closePrices))
//		ret.window = n.primitive.window
//
//		bc := NewBroadcaster()
//		bc.Write(ret)
//
//		//if(got_bid_price && !got_ask_price){
//		//	fmt.Println( n.primitive.name, " BID EXTRACTED: ",bidPrices[len(bidPrices)-1] );
//		//}
//		//
//		//if(!got_bid_price && got_ask_price){
//		//	fmt.Println( n.primitive.name, " ASK EXTRACTED: ", askPrices[len(askPrices)-1] );
//		//
//		//}
//
//	}
//	//defer barrier.Done()
//}
//
//func ema(n *Node){
//	closePrices := []float64{}
//	ema_prev := -10.0
//	r := composer.Listen()
//	for v := r.Read(); v != nil; v = r.Read() { //forever
//
//		if len(closePrices) == n.primitive.window{
//			closePrices = shift(closePrices)
//		}
//
//		fmt.Println( n.primitive.name, " RECIEVED: ", v)
//		data_raw := fmt.Sprintf( "%v", v)
//		data_split := strings.SplitAfter(data_raw, " ")
//		got_close_price := false
//		close_float := 0.0
//		for i, _ := range data_split{
//
//			curr_fromEnd := data_split[len(data_split)-i-1]
//
//			if(curr_fromEnd == "CLOSEPrice: " && !got_close_price){
//				close_price := data_split[len(data_split)-i]
//				close_price = strings.TrimSpace(close_price)
//				close_Float, _ := strconv.ParseFloat(close_price, 64)
//				closePrices = append(closePrices, close_Float)
//				got_close_price = true
//			}
//
//			if got_close_price {
//				fmt.Println( n.primitive.name, " EXTRACTED: ",  "close: ",closePrices[len(closePrices)-1])
//				break
//			}
//
//		}
//
//		if ema_prev == -10.0{
//			var sum float64 = 0 					//returning sma
//			for i:= 0; i < len(closePrices); i++{
//				sum = sum + closePrices[i]
//			}
//			ema_prev = sum/float64(len(closePrices))
//		} else {
//			multiplier := float64(2/n.primitive.window +1)
//			ema_res := (close_float - ema_prev) * multiplier + ema_prev
//			ema_prev = ema_res
//		}
//
//		var ret ma_ret
//		if ema_prev == -10.0{
//			ret.value = 0.0
//		}else{
//			ret.value = ema_prev
//		}
//		ret.window = n.primitive.window
//		bc := NewBroadcaster()
//		bc.Write(ret)
//	}
//}
//
//func min(n *Node){
//	closePrices := []float64{}
//
//	r := composer.Listen()
//	for v := r.Read(); v != nil; v = r.Read() { //forever
//
//		if len(closePrices) == n.primitive.window{
//			closePrices = shift(closePrices)
//		}
//
//		fmt.Println( n.primitive.name, " RECIEVED: ", v)
//		data_raw := fmt.Sprintf( "%v", v)
//		data_split := strings.SplitAfter(data_raw, " ")
//		got_close_price := false
//		for i, _ := range data_split{
//
//			curr_fromEnd := data_split[len(data_split)-i-1]
//
//			if(curr_fromEnd == "CLOSEPrice: " && !got_close_price){
//				close_price := data_split[len(data_split)-i]
//				close_price = strings.TrimSpace(close_price)
//				close_Float, _ := strconv.ParseFloat(close_price, 64)
//				closePrices = append(closePrices, close_Float)
//				got_close_price = true
//			}
//
//			if got_close_price {
//				fmt.Println( n.primitive.name, " EXTRACTED: ",  "close: ",closePrices[len(closePrices)-1])
//				break
//			}
//
//		}
//
//		var ret float64 = 0.0
//		for i:= 0; i < len(closePrices); i++{
//			if ret > closePrices[i]{
//				ret = closePrices[i]
//			}
//		}
//		bc := NewBroadcaster()
//		bc.Write(ret)
//	}
//}
//
//func max(n *Node){
//	closePrices := []float64{}
//
//	r := composer.Listen()
//	for v := r.Read(); v != nil; v = r.Read() { //forever
//
//		if len(closePrices) == n.primitive.window{
//			closePrices = shift(closePrices)
//		}
//
//		fmt.Println( n.primitive.name, " RECIEVED: ", v)
//		data_raw := fmt.Sprintf( "%v", v)
//		data_split := strings.SplitAfter(data_raw, " ")
//		got_close_price := false
//		for i, _ := range data_split{
//
//			curr_fromEnd := data_split[len(data_split)-i-1]
//
//			if(curr_fromEnd == "CLOSEPrice: " && !got_close_price){
//				close_price := data_split[len(data_split)-i]
//				close_price = strings.TrimSpace(close_price)
//				close_Float, _ := strconv.ParseFloat(close_price, 64)
//				closePrices = append(closePrices, close_Float)
//				got_close_price = true
//			}
//
//			if got_close_price {
//				fmt.Println( n.primitive.name, " EXTRACTED: ",  "close: ",closePrices[len(closePrices)-1])
//				break
//			}
//
//		}
//
//		var ret float64 = -10.0
//		for i:= 0; i < len(closePrices); i++{
//			if ret < closePrices[i]{
//				ret = closePrices[i]
//			}
//		}
//		bc := NewBroadcaster()
//		bc.Write(ret)
//	}
//}
