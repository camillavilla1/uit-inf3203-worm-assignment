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
	"hash/fnv"
)

var wormgatePort string
var segmentPort string

var hostname string
var hostaddress string

var targetSegments int32
var actualSegments int32

var startedNodes []string
var reachableHosts []string

var biggestAddress string


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

//Check if a value is in a list/slice and return true or false
func listContains(s []string, e string) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}

// Select a random address from reachable hosts
func selectAddress() string {
	var addressList = fetchReachableHosts()
	var addresses = len(addressList)
	var index = random(0, addresses)
	var address = addressList[index]

	return address
}

func selectStartedAddress() string {
	var addressList = startedNodes
	var addresses = len(addressList)
	var index = random(0, addresses)
	var address = addressList[index]

	return address
}

func stringify(input []string) string {
	return strings.Join(input, ",")
}

//Remove a element in a slice
/*func removeElement(input []string, element string) []string {
	for i, v := range input {
		if v == element {
			input = append(input[:i], input[i+1])
			break
		}
	}

	return input
}*/


//Remove a element in a slice
/*func removeElement(input []string, element string) []string {
	for i, v := range input {
		if len(input) == 1 {
			input = append(input[:i])
		}else {
			if v == element {
				input = append(input[:i], input[i+1])
				break
			}
		}
		
	}
	return input
}*/

//In the other remove-function we didnt have s[i+1:] COLON!!!
func removeElement(s []string, r string) []string {
	for i, v := range s {
		if len(s) == 1 {
			fmt.Println("hello")
			s = append(s[:i])
		}else {
			if v == r {
				return append(s[:i], s[i+1:]...)
			}
		}
	}
	return s
}



//Remove duplicates in a slice
func removeDuplicatesUnordered(elements []string) []string {
    encountered := map[string]bool{}

    // Create a map of all unique elements.
    for v:= range elements {
	encountered[elements[v]] = true
    }

    // Place all keys from the map into a slice.
    result := []string{}
    for key, _ := range encountered {
	result = append(result, key)
    }
    return result
}

//Get address, check if it is started nodes slice, if not: append the address
func retrieveAddresses(addr string) []string {

	if listContains(startedNodes, addr) {
		return startedNodes
	} else {
		startedNodes = append(startedNodes, addr)
		return startedNodes
	}
}

//Ping all reachable host to check if dead or alive
func heartbeat() {
	log.Printf("\n\nHeartbeating nodes\n")
	for {
		for _, addr := range reachableHosts {
			url := fmt.Sprintf("http://%s%s/", addr, segmentPort)
			if addr != hostaddress {
				resp, err := http.Get(url)
				if err != nil {
					if listContains(startedNodes, addr) {
						checkHash(hostaddress)
						tellChief()
						//startedNodes = removeElement(startedNodes, addr)
						//actualSegments = int32(len(startedNodes))
					}
					
					//_, err = ioutil.ReadAll(resp.Body)
					//resp.Body.Close()

				} else {
					_, err = ioutil.ReadAll(resp.Body)
					startedNodes = retrieveAddresses(addr)
					resp.Body.Close()
				}
			}
		}
		
		time.Sleep(250 * time.Millisecond)
	}
}

func checkList() {
	for _, addr := range startedNodes {
		url := fmt.Sprintf("http://%s%s/", addr, segmentPort)
		if addr != hostaddress {
			resp, err := http.Get(url)
			if err != nil {
				//if listContains(startedNodes, addr) {	
				fmt.Printf("\n[Checklist]: Address %s should be removed from nodelist: %s", addr, startedNodes)
				startedNodes = removeElement(startedNodes, addr)
				fmt.Printf("[Checklist - REMOVED element] in nodelist: %s\n\n", startedNodes)
				actualSegments = int32(len(startedNodes))
				//}
			} else {
				_, err = ioutil.ReadAll(resp.Body)
				startedNodes = retrieveAddresses(addr)
				resp.Body.Close()
			}
		}
	} 
}

//broadcast addresses to every active node
func broadcast() {

	fmt.Printf("\nBroadcasting: %s\n", startedNodes)

	var nodeString string
	nodeString = ""

	nodeString = stringify(startedNodes)

	for _, addr := range startedNodes {
		url := fmt.Sprintf("http://%s%s/broadcast", addr, segmentPort)
		if addr != hostaddress {
			addressBody := strings.NewReader(nodeString)
			http.Post(url, "string", addressBody)
			fmt.Printf("\n[broadcast] Broadcast to %s with addressbody: %s\n", url, addressBody)
		}
	}
}

//broadcast ts to every active node
func broadcastTs() {

	for _, addr := range startedNodes {
		url := fmt.Sprintf("http://%s%s/broadcastTs", addr, segmentPort)
		tsBody := strings.NewReader(fmt.Sprint(targetSegments))
		http.Post(url, "int", tsBody)
	}
}

func selectAvailableAddress() string{

	var address = selectAddress()
	for listContains(startedNodes, address) {
		address = selectAddress()
		//fmt.Printf("\n[selectAvailableAddress] Selected new address is: %s\n", address)
	}

	return address
}


func growOrShrinkWorm() {
	//fmt.Printf("\n------------\nGrowing or shrinking worm!\n----------------\n")
	if actualSegments < targetSegments {

		for actualSegments < targetSegments {
			fmt.Printf("\n------------\nGrowing worm!\n-------------\n")
			address := selectAvailableAddress()
			fmt.Println(address)
			fmt.Printf("\n[growOrShrinkWorm] Growing worm (as: %d, ts: %d)\n", actualSegments, targetSegments)
			sendSegment(address)
			startedNodes = append(startedNodes, address)

			fmt.Printf("\n[growOrShrinkWorm] Started Nodes are: %s\n", startedNodes)
			actualSegments = int32(len(startedNodes))
			broadcastTs()
			broadcast()
			fmt.Printf("\n------------\nFinished growing worm!\nBroadcasted ts and normal broadcast\n-------------\n")
		}
	} else if actualSegments > targetSegments {
		for actualSegments > targetSegments {
			fmt.Printf("\n------------\nShrinking worm!\n-------------\n")

			var address = selectStartedAddress()
			for address == hostaddress {
				address = selectStartedAddress()
			}
			//if address != hostaddress {
			url := fmt.Sprintf("http://%s%s/shutdown", address, segmentPort)
			message := "u dead"
			addressBody := strings.NewReader(message)
			http.Post(url, "string", addressBody)
			fmt.Printf("\n[growOrShrinkWorm] Removing %s address\n", address)
			
			if listContains(startedNodes, address) {
				fmt.Printf("\n[growOrShrinkWorm] Address %s should be removed from startedNodes: %s\n", address, startedNodes)
				startedNodes = removeElement(startedNodes, address)
			}
			actualSegments = int32(len(startedNodes))
			fmt.Printf("\n[growOrShrinkWorm] Told %s to kill themselves\n", url)
			fmt.Printf("[growOrShrinkWorm] Started nodes is %s\n", startedNodes)
			fmt.Printf("[growOrShrinkWorm] actual segments = %d\n", actualSegments)
			broadcastTs()
			broadcast()
			fmt.Printf("\n------------\nFinished shrinking worm!\nBroadcasted ts and normal broadcast\n-------------\n")
			//}
		}
	}
}

func checkHash(address string) bool {

	var biggest uint32

	for i, addr := range startedNodes {

		h := fnv.New32a()
		h.Write([]byte(addr))
		bs := h.Sum32()
		fmt.Println(addr)
		fmt.Printf("[checkHash] BS: %x\n", bs)

		if i == 0 {
			biggest = bs
			biggestAddress = addr
			fmt.Printf("\n[checkHash 1 (first)] Biggest hash value: %x\n", biggest)
		} else {
			if bs > biggest {
				fmt.Printf("\n[checkHash 2] Biggest hash value: %x\n", biggest)
				biggest = bs
				biggestAddress = addr
			}
		}
	}

	h := fnv.New32a()
	h.Write([]byte(address))
	hashedAddress := h.Sum32()
	if biggest == hashedAddress {
		fmt.Println("checkHash: Returned True")
		return true
	} else {
		fmt.Println("checkHash: Returned False")
		return false
	}
}

func tellChief() {
	broadcastTs()
	url := fmt.Sprintf("http://%s%s/chief", biggestAddress, segmentPort)
	message := "you the man!"
	addressBody := strings.NewReader(message)
	http.Post(url, "string", addressBody)
	fmt.Printf("\n[tellChief] Told chief to %s with addressbody: %s\n", url, message)
}

func sendSegment(address string) {


	//Find available address from wormgate?

	url := fmt.Sprintf("http://%s%s/wormgate?sp=%s", address, wormgatePort, segmentPort)

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
	http.HandleFunc("/chief", chiefHandler)

	log.Printf("Starting segment server on %s%s\n", hostname, segmentPort)
	reachableHosts = fetchReachableHosts()
	startedNodes = append(startedNodes, hostaddress)

	actualSegments = int32(len(startedNodes))
	log.Printf("Reachable hosts: %s", strings.Join(fetchReachableHosts()," "))
	log.Printf("\nStarted nodes is: %s\n", startedNodes)
	go heartbeat()
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



func broadcastHandler(w http.ResponseWriter, r *http.Request) {

	var addrString string

	pc, rateErr := fmt.Fscanf(r.Body, "%s", &addrString)
	if pc != 1 || rateErr != nil {
		log.Printf("Error parsing broadcast (%d items): %s", pc, rateErr)
	}

	fmt.Printf("[broadcastHandler] addrString: %s\n", addrString)
	stringList := strings.Split(addrString, ",")
	fmt.Printf("addrString: %s\n", stringList)

	for _, addr := range stringList {
		startedNodes = retrieveAddresses(addr)
		fmt.Printf("\n\n[Broadcast handler] Started nodes: %s\n", startedNodes)

	}

	actualSegments = int32(len(startedNodes))

	//fmt.Println("\nGot into broadcastHandler\n")
	//fmt.Printf("[broadcastHandler] startedNodes is %s\n", startedNodes)
	fmt.Printf("[broadcastHandler] Lenght og startedNodes: %d\n", len(startedNodes))
	fmt.Printf("[broadcastHandler] actual segments: %d\n", actualSegments)
	fmt.Printf("[broadcastHandler] target segments: %d\n", targetSegments)

	//if actualSegments < targetSegments {
		//growWorm()
	//}

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

	fmt.Printf("[BroadcastTsHandler] target segments: %d\n", targetSegments)
}

func chiefHandler(w http.ResponseWriter, r *http.Request) {

	var addrString string

	pc, rateErr := fmt.Fscanf(r.Body, "%s", &addrString)
	if pc != 1 || rateErr != nil {
		log.Printf("Error parsing broadcast (%d items): %s", pc, rateErr)
	}

	io.Copy(ioutil.Discard, r.Body)
	r.Body.Close()

	fmt.Printf("[chiefHandler] Chief here! Growign worm, affirmative!\n")
	fmt.Printf("[chiefHandler] chiefs actual segements: %d\n", actualSegments)
	fmt.Printf("[chiefHandler] chiefs target segements: %d\n", targetSegments)
	fmt.Printf("[chiefHandler] startedNodes are {%s}. \n Also checking checklist..\n", startedNodes)
	checkList()
	if actualSegments != targetSegments {
		fmt.Printf("chiefs segments unequal, should shrink or grow\n")
		growOrShrinkWorm()
	}
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

	log.Printf("[targetSegmentsHandler]New targetSegments: %d", ts)
	atomic.StoreInt32(&targetSegments, ts)


	fmt.Printf("[targetSegmentsHandler]actual segments: %d\n", actualSegments)
	fmt.Printf("[targetSegmentsHandler]target segments: %d\n", targetSegments)

	if checkHash(hostaddress) {
		fmt.Printf("\nIs chief, growing or shrinking worm...\n")
		growOrShrinkWorm()
	} else {
		tellChief()
	}
	//go broadcast()

}

func shutdownHandler(w http.ResponseWriter, r *http.Request) {

	// Consume and close body
	io.Copy(ioutil.Discard, r.Body)
	r.Body.Close()

	//startedNodes = removeElement(startedNodes, hostaddress)
	//targetSegments = int32(len(startedNodes))
	//broadcast()
	//tellChief()

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





