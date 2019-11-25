package main

import (
	"bufio"
	"fmt"
	"math"
	"net"
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

func SUBTRACT(a *Receiver, b *Receiver, optionalName string ) *Receiver {
	signal_id := a.name + " " + b.name
	if _, ok := compiler_handlers[signal_id]; ok {
		listener := compiler_handlers[signal_id].Listen()
		listener.name = optionalName
		return &listener
	}
	composer := NewBroadcaster()
	composer.name = optionalName
	compiler_handlers[signal_id] = composer
	fmt.Println("Subtract " +  optionalName + " created at ", time.Now());
	go subtract_eval(a, b, &composer, optionalName)
	listener := composer.Listen()
	listener.name = optionalName
	return &listener
}

func subtract_eval(source_a *Receiver, source_b *Receiver, output *Broadcaster, id string){
	if( source_a.master_id == source_b.master_id){
		for b := source_b.Read(); b != nil; b = source_b.Read() {
			data_b := fmt.Sprintf("%v", b)
			println("SAME SOURCE SUBTRACT prim is subtracting: " + data_b + " - " + data_b )
			output.Write(0)
		}
	}
	for a := source_a.Read(); a != nil; a = source_a.Read() {
		for b := source_b.Read(); b != nil; {
			data_a := fmt.Sprintf("%v", a)
			data_b := fmt.Sprintf("%v", b)
			fmt.Println(id, " RECEIVED a:", data_a," & b:", data_b, "at time ", time.Now()   , " from receiver ", source_a.name, "& from receiver ", source_b.name ); //v);
			if data_a == "false" || data_a == "true"{
				if data_b == "false" || data_b == "true"{
					a_val := 0
					b_val := 0
					if data_a == "true"{
						a_val = 1
					}
					if data_b == "true"{
						b_val = 1
					}
					if a_val == 0 && b_val == 0{
						fmt.Println("Error both inputs to " + id + " were 0")
					}
					ret_val := a_val - b_val
					output.Write(ret_val)

					fmt.Println("Data a: ", a_val, "Data b: ", b_val, "Result: ", ret_val)

					break
				}
				fmt.Println("Error data_a is a boolean but data_b is not")
			}
			if data_b == "false" || data_b == "true"{
				fmt.Println("Error data_b is a boolean but data_a is not")
			}
			a_float, _ := strconv.ParseFloat(data_a, 64)
			b_float, _ := strconv.ParseFloat(data_b, 64)
			ret_val := a_float - b_float
			output.Write(ret_val)
			fmt.Println("Data a: ", a_float, "Data b: ", b_float, "Result: ", ret_val)
			break
		}
	}
}

func ADD(a *Receiver, b *Receiver, optionalName string) *Receiver{
	add_toSring := a.name + " + " + b.name
	if _, ok := compiler_handlers[add_toSring]; ok {
		listener := compiler_handlers[add_toSring].Listen()
		listener.name = optionalName
		return &listener
	}
	composer := NewBroadcaster()
	composer.name = optionalName
	compiler_handlers[add_toSring] = composer
	fmt.Println( "ADD" + add_toSring + " created at ", time.Now()  );
	go add_eval( a, b, &composer, add_toSring)
	listener := composer.Listen()
	listener.name = optionalName
	return &listener
}

func add_eval(source_a *Receiver, source_b *Receiver, output *Broadcaster, id string){
	if( source_a.master_id == source_b.master_id){
		for b := source_b.Read(); b != nil; b = source_b.Read() {
			data_b := fmt.Sprintf("%v", b)
			b_float, _ := strconv.ParseFloat(data_b, 64)
			ret_val := 2 * b_float
			output.Write(ret_val)
			fmt.Println("Data b: ", b_float, "Result: ", ret_val)
		}
	}
	for a := source_a.Read(); a != nil; a = source_a.Read() {
		for b := source_b.Read(); b != nil; {
			data_a := fmt.Sprintf("%v", a)
			data_b := fmt.Sprintf("%v", b)
			a_float, _ := strconv.ParseFloat(data_a, 64)
			b_float, _ := strconv.ParseFloat(data_b, 64)
			ret_val := a_float + b_float
			output.Write(ret_val)
			fmt.Println("Data a: ", a_float, "Data b: ", b_float, "Result: ", ret_val)
			break
		}
	}
}



//TODO: add mapping of priceType string to index in


func SMA( seriesSource *Receiver, window int, priceType string, optionalName string) *Receiver {
	signal_id := seriesSource.name + " " + string(window) + " "+  priceType
	if _, ok := compiler_handlers[signal_id]; ok {
		listener := compiler_handlers[signal_id].Listen()
		listener.name = optionalName
		print("Returning same SMA signal pointer")
		return &listener
	}
	composer := NewBroadcaster()
	composer.name = optionalName
	compiler_handlers[signal_id] = composer
	fmt.Println(  "SMA " + signal_id + " created at ", time.Now());
	go sma_eval( seriesSource,&composer, optionalName, window, priceType)
	listener := composer.Listen()
	listener.name = optionalName
	return &listener

}
func sma_eval( source *Receiver, output *Broadcaster, id string, window int, priceType string) {

	index := price_map[priceType]
	barrier.Add(1)
	closePrices := []float64{}
	r := source
	for v := r.Read(); v != nil; v = r.Read() {
		close_price := strings.TrimSuffix(strings.Split(v.(string), " ")[index], "$") //TODO used to be 5
		close_price = strings.TrimSuffix(close_price, ",")
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
		if id == "short"{
			fmt.Println("sma_short_variables", closePrices, "resulted in ", ret_value)
		} else if id == "long"{
			fmt.Println("sma_long_variables", closePrices, "resulted in ", ret_value)
		}
		output.Write(ret_value)
		fmt.Println(id, " RECEIVED at time ", time.Now()   , " from receiver ", source.name ); //v);
	}
	//TODO: use mapping of index to priceType to access correct data in data_split
}

func GTE(a *Receiver, b *Receiver, optionalName string ) *Receiver {
	signal_id := a.name + " " + b.name
	if _, ok := compiler_handlers[signal_id]; ok {
		listener := compiler_handlers[signal_id].Listen()
		listener.name = optionalName
		return &listener
	}
	composer := NewBroadcaster()
	composer.name = optionalName
	compiler_handlers[signal_id] = composer
	fmt.Println( "GTE " + signal_id + " created at ", time.Now()  );
	go gte_eval(a, b, &composer, optionalName)
	listener := composer.Listen()
	listener.name = optionalName
	return &listener
}
func gte_eval(a *Receiver, b *Receiver, output *Broadcaster, id string) {
	if(a.master_id == b.master_id){
		for b2 := b.Read(); b2 != nil; b2 = b.Read() {
			print("GTE SAME")
			output.Write(true)
		}
	}
	barrier.Add(1)
	for b_read := b.Read(); b_read != nil; b_read = b.Read() {
		a_read := a.Read()
		fmt.Println(id, " RECEIVED at time ", time.Now()   , " from A receiver ", a.name  ); //v);
		fmt.Println(id, " RECEIVED at time ", time.Now()   , " from B receiver ", b.name  ); //v);
		a_string := fmt.Sprintf("%v", a_read)
		b_string := fmt.Sprintf("%v", b_read)
		if a_string == "NaN" || b_string == "NaN"{
			continue
		}
		a_float, _ := strconv.ParseFloat(a_string, 64)
		b_float, _ := strconv.ParseFloat(b_string, 64)
		if a_float < b_float{
			fmt.Println(id ," Calculated @ time, ",time.Now(), "variables: ", a_float, " < ", b_float, "results in: ", false)
			output.Write(false)
		} else {
			fmt.Println(id ," Calculated @ time, ",time.Now(), "variables: ", a_float, " >= ", b_float, "results in: ", true)
			output.Write(true)
		}
	}
}

/* exponential moving average is like simple moving average but takes the age of each data point into account*/
func EMA( seriesSource *Receiver, window int, priceType string, optionalName string) *Receiver {
	signal_id := seriesSource.name + " " + string(window) + " "+  priceType
	if _, ok := compiler_handlers[signal_id]; ok {
		listener := compiler_handlers[signal_id].Listen()
		listener.name = optionalName
		return &listener
	}
	composer := NewBroadcaster()
	composer.name = optionalName
	compiler_handlers[signal_id] = composer
	fmt.Println( optionalName + " created at ", time.Now()  )
	go ema_eval(seriesSource,&composer, optionalName, window, priceType)
	listener := composer.Listen()
	listener.name = optionalName
	return &listener
}

func ema_eval( source *Receiver, output *Broadcaster, id string, window int, priceType string) {
	index := price_map[priceType]
	barrier.Add(1)
	ema_prev := -10.0
	closePrices := []float64{}
	r := source
	for v := r.Read(); v != nil; v = r.Read() {
		close_price := strings.TrimSuffix(strings.Split(v.(string), " ")[index], "$")
		close_price = strings.TrimSuffix(close_price, ",")
		close_float, _ := strconv.ParseFloat(close_price, 64)
		ret_value := math.NaN()
		if len(closePrices) == window{
			closePrices = shift(closePrices)
		}
		closePrices = append(closePrices, close_float)
		if len(closePrices) == window{
			if ema_prev == -10.0{
				var sum float64 = 0 					//returning sma
				for i:= 0; i < len(closePrices); i++{
					sum = sum + closePrices[i]
				}
				ema_prev = sum/float64(len(closePrices))
			} else {
				multiplier := float64(2/window +1)
				ema_res := (close_float - ema_prev) * multiplier + ema_prev
				ema_prev = ema_res
			}
		}
		if ema_prev == -10.0 {
			ret_value = 0.0
		} else {
			ret_value = ema_prev
		}
		if id == "short"{
			fmt.Println("ema_short_variables", closePrices, "resulted in ", ret_value)
		} else if id == "long"{
			fmt.Println("ema_long_variables", closePrices, "resulted in ", ret_value)
		}
		output.Write(ret_value)
		fmt.Println(id, " RECEIVED at time ", time.Now()   , " from receiver ", source.name  )
	}
}

func MIN( seriesSource *Receiver, window int, priceType string, optionalName string) *Receiver {
	signal_id := seriesSource.name + " " + string(window) + " "+  priceType
	if _, ok := compiler_handlers[signal_id]; ok {
		listener := compiler_handlers[signal_id].Listen()
		listener.name = optionalName
		return &listener
	}
	composer := NewBroadcaster()
	composer.name = optionalName
	compiler_handlers[signal_id] = composer
	fmt.Println( "MIN " + signal_id + " created at ", time.Now()  )
	go min_eval( seriesSource,&composer, optionalName, window, priceType)
	listener := composer.Listen()
	listener.name = optionalName
	return &listener
}

func min_eval( source *Receiver, output *Broadcaster, id string, window int, priceType string) {
	index := price_map[priceType]
	barrier.Add(1)
	closePrices := []float64{}
	r := source
	for v := r.Read(); v != nil; v = r.Read() {
		close_price := strings.TrimSuffix(strings.Split(v.(string), " ")[index], "$")
		close_price = strings.TrimSuffix(close_price, ",")
		close_float, _ := strconv.ParseFloat(close_price, 64)
		ret_value := math.NaN()
		if len(closePrices) == window{
			closePrices = shift(closePrices)
		}
		closePrices = append(closePrices, close_float)
		if len(closePrices) == window{
			ret_value := math.NaN()
			for i:= 0; i < len(closePrices); i++{
				if math.IsNaN(ret_value){
					ret_value = closePrices[i]
				} else if ret_value > closePrices[i]{
					ret_value = closePrices[i]
				}
			}
		}
		output.Write(ret_value)
		fmt.Println(id, " RECEIVED at time ", time.Now()   , " from receiver ", source.name  )
	}
}

func MAX( seriesSource *Receiver, window int, priceType string, optionalName string) *Receiver {
	signal_id := seriesSource.name + " " + string(window) + " "+  priceType
	if _, ok := compiler_handlers[signal_id]; ok {
		listener := compiler_handlers[signal_id].Listen()
		listener.name = optionalName
		return &listener
	}
	composer := NewBroadcaster()
	composer.name = optionalName
	compiler_handlers[signal_id] = composer
	fmt.Println( "MAX " +  signal_id + " created at ", time.Now()  )
	go max_eval( seriesSource,&composer, optionalName, window, priceType)
	listener := composer.Listen()
	listener.name = optionalName
	return &listener
}


func max_eval( source *Receiver, output *Broadcaster, id string, window int, priceType string) {
	index := price_map[priceType]
	barrier.Add(1)
	closePrices := []float64{}
	r := source
	for v := r.Read(); v != nil; v = r.Read() {
		close_price := strings.TrimSuffix(strings.Split(v.(string), " ")[index], "$")
		close_price = strings.TrimSuffix(close_price, ",")
		close_float, _ := strconv.ParseFloat(close_price, 64)
		ret_value := math.NaN()
		if len(closePrices) == window{
			closePrices = shift(closePrices)
		}
		closePrices = append(closePrices, close_float)
		if len(closePrices) == window{
			ret_value := math.NaN()
			for i:= 0; i < len(closePrices); i++{
				if math.IsNaN(ret_value){
					ret_value = closePrices[i]
				} else if ret_value < closePrices[i]{
					ret_value = closePrices[i]
				}
			}
		}
		output.Write(ret_value)
		fmt.Println(id, " RECEIVED at time ", time.Now()   , " from receiver ", source.name  )
	}
}

func simple_2SMA_Strategy(a *Receiver , shortWindow int, longWindow int, quantity int, priceType string, optionalName string) *Receiver{
	if _, ok := compiler_handlers[optionalName]; ok {
		listener := compiler_handlers[optionalName].Listen()
		return &listener
	}
	composer := NewBroadcaster()
	composer.name = optionalName
	compiler_handlers[optionalName] = composer
	fmt.Println( optionalName + " created at ", time.Now())
	buyOrder := GTE(SMA(seriesSource("EUR CASH USD IDEALPRO"), longWindow, priceType,"sma_0"),
		SMA(seriesSource("EUR CASH USD IDEALPRO"), shortWindow, priceType,"sma_1"), "buyOrder")
	sellOrder := GTE(SMA(seriesSource("EUR CASH USD IDEALPRO"), shortWindow,priceType,"sma_3"),
		SMA(seriesSource("EUR CASH USD IDEALPRO"), longWindow, priceType,"sma_2"), "sellOrder")
	time.Sleep(4)
	// TODO: add bool return from primitives, to ensure that they've entered their prim_eval functions
	orderSignal := SUBTRACT(sellOrder, buyOrder, optionalName)
	go simple_2SMA_Strategy_eval( orderSignal, &composer, optionalName)
	listener := composer.Listen()
	listener.name = optionalName
	return &listener
}

func simple_2SMA_Strategy_eval( source_a *Receiver, output *Broadcaster, optionalName string ){
	var hostName = "127.0.0.1"
	var portNum =  "19192"
	//var service = hostName + ":" + portNum
	//var RemoteAddr, _ = net.ResolveUDPAddr("udp", service)
	println("Starting port: " , portNum)
	var conn, _ = net.Dial("tcp", hostName + ":"+ portNum)
	for a := source_a.Read(); a != nil; {
		data_a := fmt.Sprintf("%v", a)
		a_float, _ := strconv.ParseFloat(data_a, 64)
		if (a_float == 1.0){   // buy
			message := []byte("1")
			_, _ = conn.Write(message)
			fmt.Fprintf(conn, "")
			output.Write(1)
			continue
		}
		if (a_float == -1.0){   // sell
			message := []byte("-1")
			_, _ = conn.Write(message)
			fmt.Fprintf(conn, "")
			output.Write(-1)
			continue
		}
		if( a_float == 0.0) {continue}   // do nothing
		fmt.Println("Incorrect Order Signal: " , data_a )
	}
}

func simple_2EMA_strategy(a *Receiver , shortWindow int, longWindow int, quantity int, priceType string, optionalName string) *Receiver{
	if _, ok := compiler_handlers[optionalName]; ok {
		listener := compiler_handlers[optionalName].Listen()
		return &listener
	}
	composer := NewBroadcaster()
	composer.name = optionalName
	compiler_handlers[optionalName] = composer
	fmt.Println( optionalName + " created at ", time.Now())
	buyOrder := GTE(EMA(seriesSource("EUR CASH USD IDEALPRO"), longWindow, priceType,"long"),
		EMA(seriesSource("EUR CASH USD IDEALPRO"), shortWindow, priceType,"short"), "buyOrder")
	sellOrder := GTE(EMA(seriesSource("EUR CASH USD IDEALPRO"), shortWindow,priceType,"short"),
		EMA(seriesSource("EUR CASH USD IDEALPRO"), longWindow, priceType,"long"), "sellOrder")
	time.Sleep(4)
	// TODO: add bool return from primitives, to ensure that they've entered their prim_eval functions
	//orderSignal := SUBTRACT(sellOrder, buyOrder, optionalName)
	SUBTRACT(sellOrder, buyOrder, optionalName)
	//go simple_2EMA_Strategy_eval( orderSignal, &composer, optionalName)
	listener := composer.Listen()
	listener.name = optionalName
	return &listener
}
func simple_2EMA_Strategy_eval( source_a *Receiver, output *Broadcaster, optionalName string ){
	var hostName = "127.0.0.1"
	var portNum =  "19192"
	//var service = hostName + ":" + portNum
	//var RemoteAddr, _ = net.ResolveUDPAddr("udp", service)
	println("Starting port: " , portNum)
	var conn, _ = net.Dial("tcp", hostName + ":"+ portNum)
	for a := source_a.Read(); a != nil; {
		data_a := fmt.Sprintf("%v", a)
		a_float, _ := strconv.ParseFloat(data_a, 64)
		if (a_float == 1.0){   // buy
			message := []byte("1")
			_, _ = conn.Write(message)
			fmt.Fprintf(conn, "")
			output.Write(1)
			continue
		}
		if (a_float == -1.0){   // sell
			message := []byte("-1")
			_, _ = conn.Write(message)
			fmt.Fprintf(conn, "")
			output.Write(-1)
			continue
		}
		if( a_float == 0.0) {continue}   // do nothing
		fmt.Println("Incorrect Order Signal: " , data_a )
	}
}
func brokerRequest(orderSignal *Receiver){

}

func shift(n []float64) []float64{
	for i := 0; i < len(n)-1; i++ {
		n[i] = n[i+1]
	}
	n = n[:len(n)-1]
	return n
}



var handlers = map[string]Broadcaster{}

func seriesSource(source string ) *Receiver {
	// addNode, this will be shared by everyone
	//defer barrier.Done()
	if _, ok := handlers[source]; ok {
		listener := handlers[source].Listen()
		listener.name = source
		print("Returning same source pointer!" )
		return &listener
	}
	composer := NewBroadcaster()
	handlers[source] = composer
	composer.name = source
	go sinkSource(&composer, source)
	listener := composer.Listen()
	listener.name = source
	return &listener
}

func sinkSource( composer *Broadcaster, contract string){
	var hostName = "127.0.0.1"
	var portNum =  "19192"
	//var service = hostName + ":" + portNum
	//var RemoteAddr, _ = net.ResolveUDPAddr("udp", service)
	println("Starting port: " , portNum)
	var conn, _ = net.Dial("tcp", hostName + ":"+ portNum)
	message := []byte(contract)  // specify contract details, max period,
	_, _ = conn.Write(message)
	barrier.Add(1)
	for{
		// send to socket
		fmt.Fprintf(conn, "")
		// listen for reply
		server_data, err := bufio.NewReader(conn).ReadString('$')
		if (err == nil){
			composer.Write(server_data)
		}
	}

}




