package main

import (
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"sync/atomic"
	"math/rand"
	"time"
)

var wormgatePort string
var segmentPort string

var hostname string
var hostaddress string

var targetSegments int32
var actualSegments int32

var startedNodes []string


func main() {

	hostname, _ = os.Hostname()

	hostaddress = strings.Split(hostname, ".")[0]
	fmt.Println("hostaddress", hostaddress)

	actualSegments = int32(len(startedNodes))
	log.SetPrefix(hostname + " segment: ")

	var spreadMode = flag.NewFlagSet("spread", flag.ExitOnError)
	addCommonFlags(spreadMode)
	var spreadHost = spreadMode.String("host", "localhost", "host to spread to")

	var runMode = flag.NewFlagSet("run", flag.ExitOnError)
	addCommonFlags(runMode)

	if len(os.Args) == 1 {
		log.Fatalf("No mode specified\n")
	}

	switch os.Args[1] {
	case "spread":
		spreadMode.Parse(os.Args[2:])
		sendSegment(*spreadHost)
	case "run":
		runMode.Parse(os.Args[2:])
		startSegmentServer()

	default:
		log.Fatalf("Unknown mode %q\n", os.Args[1])
	}
}

func addCommonFlags(flagset *flag.FlagSet) {
	flagset.StringVar(&wormgatePort, "wp", ":8181", "wormgate port (prefix with colon)")
	flagset.StringVar(&segmentPort, "sp", ":8182", "segment port (prefix with colon)")
}

func random(min, max int) int {
	rand.Seed(time.Now().Unix())
	return rand.Intn(max - min) + min
}

func contains(s []string, e string) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}

func selectAddress() string {
	var addressList = fetchReachableHosts()
	var addresses = len(addressList)
	var index = random(0, addresses)
	var address = addressList[index]

	return address
}

func stringify(input []string) string {
	return strings.Join(input, ",")
}


func broadcast() {

	fmt.Printf("\nBroadcasting: %s\n", startedNodes)

	nodeString := stringify(startedNodes)

	for _, addr := range startedNodes {
		url := fmt.Sprintf("http://%s%s/broadcast", addr, segmentPort)
		if addr != startedNodes[0] {
			addressBody := strings.NewReader(nodeString)
			http.Post(url, "string", addressBody)
		}
		//for _, addr2 := range startedNodes {
			//addressBody := strings.NewReader(addr2)
			//http.Post(url, "string", addressBody)
			//fmt.Printf("\nGot into broadcast, just sent: %s to %s\n", addr2, addr)
			//fmt.Printf("\nWhole address list is: %s\n", startedNodes)
		//}
	}
}

func broadcastTs() {

	for _, addr := range startedNodes {
		url := fmt.Sprintf("http://%s%s/broadcastTs", addr, segmentPort)
		tsBody := strings.NewReader(fmt.Sprint(targetSegments))
		http.Post(url, "int", tsBody)
	}
}


func growWorm() {

	//fmt.Println(addressList)

	var address = selectAddress()
	for contains(startedNodes, address) {
		address = selectAddress()
	}

	//fmt.Printf("actual segments: %d\n", actualSegments)
	//fmt.Printf("target segments: %d\n", targetSegments)

	//fmt.Println("DIIIIIILDOOOOOOONAMMMM!!!!")
	fmt.Println(address)

	if actualSegments < targetSegments {
		sendSegment(address)
		startedNodes = append(startedNodes, address)
		actualSegments = int32(len(startedNodes))
	}

	//time.Sleep(1000 * time.Millisecond)
	broadcastTs()
	broadcast()
	//actualSegments++
	//}
}


func sendSegment(address string) {


	//Find available address from wormgate?

	url := fmt.Sprintf("http://%s%s/wormgate?sp=%s", address, wormgatePort, segmentPort)

	//fmt.Printf("Started nodes is: %s\n", startedNodes)


	filename := "tmp.tar.gz"

	log.Printf("Spreading to %s", url)

	// ship the binary and the qml file that describes our screen output
	tarCmd := exec.Command("tar", "-zc", "-f", filename, "segment")
	tarCmd.Run()
	defer os.Remove(filename)

	file, err := os.Open(filename)
	if err != nil {
		log.Panic("Could not read input file", err)
	}

	resp, err := http.Post(url, "string", file)
	if err != nil {
		log.Panic("POST error ", err)
	}

	io.Copy(ioutil.Discard, resp.Body)
	if resp.StatusCode == 200 {
		log.Println("Received OK from server")
	} else {
		log.Println("Response: ", resp)
	}
}

func startSegmentServer() {
	//func HandleFunc(pattern string, handler func(ResponseWriter, *Request))
	http.HandleFunc("/", IndexHandler)
	http.HandleFunc("/targetsegments", targetSegmentsHandler)
	http.HandleFunc("/shutdown", shutdownHandler)
	http.HandleFunc("/broadcast", broadcastHandler)
	http.HandleFunc("/broadcastTs", broadcastTsHandler)

	log.Printf("Starting segment server on %s%s\n", hostname, segmentPort)
	startedNodes = append(startedNodes, hostaddress)
	actualSegments = int32(len(startedNodes))
	log.Printf("Reachable hosts: %s", strings.Join(fetchReachableHosts()," "))
	log.Printf("\nStarted nodes is: %s\n", startedNodes)
	err := http.ListenAndServe(segmentPort, nil)
	if err != nil {
		log.Panic(err)
	}
}



func IndexHandler(w http.ResponseWriter, r *http.Request) {

	// We don't use the request body. But we should consume it anyway.
	io.Copy(ioutil.Discard, r.Body)
	r.Body.Close()

	killRateGuess := 2.0

	fmt.Fprintf(w, "%.3f\n", killRateGuess)

}

func retrieveAddresses(addr string) []string {

	if contains(startedNodes, addr) {
		return startedNodes
	} else {
		startedNodes = append(startedNodes, addr)
		return startedNodes
	}
}

func broadcastHandler(w http.ResponseWriter, r *http.Request) {

	var addrString string

	pc, rateErr := fmt.Fscanf(r.Body, "%s", &addrString)
	if pc != 1 || rateErr != nil {
		log.Printf("Error parsing broadcast (%d items): %s", pc, rateErr)
	}

	fmt.Printf("addrString: %s\n", addrString)
	stringList := strings.Split(addrString, ",")
	fmt.Printf("addrString: %s\n", stringList)

	for _, addr := range stringList {

		startedNodes = retrieveAddresses(addr)
	}

	actualSegments = int32(len(startedNodes))

	//fmt.Println("\nGot into broadcastHandler\n")
	fmt.Printf("Lenght og startedNodes: %d\n", len(startedNodes))
	fmt.Printf("startedNodes is %s\n", startedNodes)
	fmt.Printf("actual segments: %d\n", actualSegments)
	fmt.Printf("target segments: %d\n", targetSegments)

	if actualSegments < targetSegments {
		growWorm()
	}

	io.Copy(ioutil.Discard, r.Body)
	r.Body.Close()
}

func broadcastTsHandler(w http.ResponseWriter, r *http.Request) {

	var bodjey int32
	pc, rateErr := fmt.Fscanf(r.Body, "%d", &bodjey)
	if pc != 1 || rateErr != nil {
		log.Printf("Error parsing broadcastTs (%d items): %s", pc, rateErr)
	}

	io.Copy(ioutil.Discard, r.Body)
	r.Body.Close()

	targetSegments = int32(bodjey)

	fmt.Printf("target segments: %d\n", targetSegments)
}

func targetSegmentsHandler(w http.ResponseWriter, r *http.Request) {

	var ts int32
	pc, rateErr := fmt.Fscanf(r.Body, "%d", &ts)
	if pc != 1 || rateErr != nil {
		log.Printf("Error parsing targetSegments (%d items): %s", pc, rateErr)
	}

	// Consume and close rest of body
	io.Copy(ioutil.Discard, r.Body)
	r.Body.Close()

	log.Printf("New targetSegments: %d", ts)
	atomic.StoreInt32(&targetSegments, ts)

	fmt.Printf("actual segments: %d\n", actualSegments)
	fmt.Printf("target segments: %d\n", targetSegments)

	growWorm()
	//go broadcast()

}

func shutdownHandler(w http.ResponseWriter, r *http.Request) {

	// Consume and close body
	io.Copy(ioutil.Discard, r.Body)
	r.Body.Close()

	// Shut down
	log.Printf("Received shutdown command, committing suicide")
	os.Exit(0)
}

func fetchReachableHosts() []string {
	url := fmt.Sprintf("http://localhost%s/reachablehosts", wormgatePort)
	resp, err := http.Get(url)
	if err != nil {
		return []string{}
	}

	var bytes []byte
	bytes, err = ioutil.ReadAll(resp.Body)
	body := string(bytes)
	resp.Body.Close()

	trimmed := strings.TrimSpace(body)
	nodes := strings.Split(trimmed, "\n")

	for i, v := range nodes {
		if v == "compute-1-4" {
			nodes = append(nodes[:i], nodes[i+1])
			break
		}
		if v == "compute-2-20" {
			nodes = append(nodes[:i], nodes[i+1])
			break
		}
	}

	return nodes
}





