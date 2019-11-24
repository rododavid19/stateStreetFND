package main

import (
	"strconv"
	"strings"
)

var arithmetic_Operators = map[string]func(seriesSource *Receiver, window int, priceType string ,optionalName string) *Receiver{
	"sma": SMA,
}

var reciever_operator = map[string]func(a *Receiver, b *Receiver, optionalName string) *Receiver{
	"add": ADD,
	"subtract": SUBTRACT,
}

// TODO: Fix priceType below



var operators_1 = []string{ "add", "subtract"}
var operators_2 = []string{ "sma"}

func Find(slice []string, val string)  bool {
	for _, item := range slice {
		if item == val {
			return  true
		}
	}
	return  false
}

//export py
func py(request string ) {

	request_array := strings.Split(request, " , ")
	recievers := map[string]*Receiver{}

	for _, curr := range request_array{
		parser := strings.Split( curr, " " )

		curr_primitive := parser[0]


		if Find(operators_1, curr_primitive){

			a := &Receiver{}
			b := &Receiver{}

			f := reciever_operator[curr_primitive]
			if _, ok := recievers[parser[1]]; ok {
				a = recievers[parser[1]]
			}else{ print("Fatal Error: signal missing for ", parser[0], " Primitive/Module." )}
			if _, ok := recievers[parser[2]]; ok {
				b = recievers[parser[2]]
			} else{ print("Fatal Error: signal missing for ", parser[0], " Primitive/Module." )}

			add_name := parser[3]
			recievers[add_name] = f(a, b, add_name)
		}

		if (curr_primitive == "sma"){
			f := arithmetic_Operators["sma"]
			window, _ := strconv.Atoi(parser[5])
			contract := parser[1] + " " + parser[2] + " " + parser[3] + " " + parser[4]
			sma_name := parser[6]
			recievers[sma_name] = f(seriesSource(contract), window ,  "close",sma_name)
		}


	}


	//print(args) TODO: start appending info to return message for Python

}

