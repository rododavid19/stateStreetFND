package main

import (
	"fmt"
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
	go sma_eval( seriesSource,&composer, optionalName)

	listener := composer.Listen()
	listener.name = optionalName


	 return &listener



	}
	//defer barrier.Done()



func sma_eval( source *Receiver, output *Broadcaster, id string) {

	barrier.Add(1)

	//openPrices := []float64{}
	//highPrices := []float64{}

	//composer := NewBroadcaster()

	r := source
	for v := r.Read(); v != nil; v = r.Read() {

		//TODO: use mapping of index to priceType to access correct data in data_split


		output.Write(v)
		fmt.Println(id, " RECIEVED at time ", time.Now()   , " from reciever ", source.name  ); //v);
	}

}



