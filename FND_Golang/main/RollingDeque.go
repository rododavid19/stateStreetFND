package main

import (
	"fmt"
)

func main(){
	// Create a deque an push some data in
	d := deque.New()
	for i := 0; i < 3; i++ {
		d.PushLeft(i)
	}
	// Pop out the deque contents and display them
	fmt.Println(d.PopLeft())
	fmt.Println(d.PopRight())
	fmt.Println(d.PopLeft())

}