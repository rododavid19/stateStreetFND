package main

import (
	"fmt"
	"log"
	"net"
)


var hostName = "127.0.0.1"
var portNum = "19192"
var service = hostName + ":" + portNum
var RemoteAddr, err = net.ResolveUDPAddr("udp", service)
var conn, err_ = net.DialUDP("udp", nil, RemoteAddr)

func startServer() {


	// note : you can use net.ResolveUDPAddr for LocalAddr as well
	//        for this tutorial simplicity sake, we will just use nil

	if err != nil {
		log.Fatal(err)
	}

	log.Printf("Established connection to %s \n", service)
	log.Printf("Remote UDP address : %s \n", conn.RemoteAddr().String())
	log.Printf("Local UDP client address : %s \n", conn.LocalAddr().String())
	defer conn.Close()

	// TODO: use sourceOracle as deposit for data



	// TODO:


	for{

		message := []byte("jjj!")

		_, err = conn.Write(message)

		if err != nil {
			log.Println(err)
		}

		// receive message from server
		buffer := make([]byte, 1024)


		n, addr, _ := conn.ReadFromUDP(buffer)

		fmt.Println("UDP Server : ", addr)
		//  hotData <- string(buffer[:n])
		fmt.Println("Received from UDP server : ", string(buffer[:n]) + " error code: " )

	}
	// write a message to server




}