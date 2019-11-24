package main

type broadcast struct {
	c chan broadcast
	v interface{}
}

// Broadcaster allows
type Broadcaster struct {
	cc    chan broadcast
	sendc chan<- interface{}
	name string
}

// Receiver can be used to wait for a broadcast value.
type Receiver struct {
	c chan broadcast
	name string
	master_id string
}

// NewBroadcaster returns a new broadcaster object.
func NewBroadcaster() Broadcaster {
	cc := make(chan broadcast, 1)
	sendc := make(chan interface{})
	b := Broadcaster{
		sendc: sendc,
		cc:    cc,
	}

	go func() {
		for {
			select {
			case v := <-sendc:
				if v == nil {
					b.cc <- broadcast{}
					return
				}
				c := make(chan broadcast, 1)
				newb := broadcast{c: c, v: v}
				b.cc <- newb
				b.cc = c
			}
		}
	}()

	return b
}

// Listen starts returns a Receiver that
// listens to all broadcast values.
func (b Broadcaster) Listen() Receiver {
	return Receiver{b.cc, "", b.name}
}

// Write broadcasts a a value to all listeners.
func (b Broadcaster) Write(v interface{}) {
	b.sendc <- v
}

// Read reads a value that has been broadcast,
// waiting until one is available if necessary.
func (r *Receiver) Read() interface{} {
	b := <-r.c
	v := b.v
	r.c <- b
	r.c = b.c
	return v
}