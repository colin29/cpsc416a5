package main

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"strings"
	"sync"
)

//RPC Shared Types
type Void struct {
}

/////////////*RPC Service for the Client*/////////////

/*
List of functions:
GetWorkers
Crawl
Domains
Overlap
*/

type MServer int

type GetWorkersReq struct{}

type GetWorkersRes struct {
	WorkerIPsList []string // List of workerIP string
}

func (t *MServer) GetWorkers(req *GetWorkersReq, reply *GetWorkersRes) error {
	for worker, _ := range workers {
		reply.WorkerIPsList = append(reply.WorkerIPsList, worker)
	}
	return nil
}

type CrawlReq struct {
	URL   string // URL of the website to crawl
	Depth int    // Depth to crawl to from URL
}

type CrawlRes struct {
	WorkerIP string // workerIP
}

var crawlWg sync.WaitGroup //tracks number of recursive crawl requests to seperate domains that still need to be sent out/finished by the server

func (t *MServer) Crawl(req *CrawlReq, reply *CrawlRes) error {

	workersMutex.Lock()
	if len(workers) == 0 {
		workersMutex.Unlock()
		return errors.New("No workers connected")
	}
	workersMutex.Unlock()

	//Let the crawling begin;
	reply.WorkerIP = crawl(req.URL, req.Depth)

	crawlWg.Wait()
	fmt.Println("Crawl Request completed")
	return nil
}

type DomainsReq struct {
	WorkerIP string // IP of worker
}

type DomainsRes struct {
	Domains []string // List of domain string
}

func (t *MServer) Domains(req *DomainsReq, reply *DomainsRes) error {
	return nil
}

type OverlapReq struct {
	URL1 string // URL arg to Overlap
	URL2 string // The other URL arg to Overlap
}

type OverlapRes struct {
	NumPages int // Computed overlap between two URLs
}

func (t *MServer) Overlap(req *OverlapReq, reply *OverlapRes) error {
	return nil
}

/////////////*RPC Service for the Workers*/////////////

/*
List of functions:
RegisterWorker
CrawlFurther
*/

type WMaster int

type WRegisterWorkerReq struct {
	WorkerIp string
}

func (t *WMaster) RegisterWorker(req *WRegisterWorkerReq, reply *Void) error {
	log.Println("Recieved RegisterWorker Request:", req.WorkerIp)
	workersMutex.Lock()
	workers[req.WorkerIp] = true //value doesn't matter
	workersMutex.Unlock()
	log.Println("List of workers:", workers)
	doOnWorkerJoin(req.WorkerIp)
	return nil
}

type CrawlFurtherReq struct {
	Domain string
	URL    string
	Depth  int // new depth
}

func (t *WMaster) CrawlFurther(req *CrawlFurtherReq, reply *Void) error {

	fmt.Println("Got RPC Request: ", req.Domain, " {CrawlFurther}")

	//start a new go-routine and continue this crawl
	crawlWg.Add(1)
	go crawlFurther(req.Domain, req.URL, req.Depth)

	return nil
}

func crawlFurther(domain string, url string, depth int) {
	defer crawlWg.Done()
	crawl(url, depth)
}

/////////////* Implementation of worker-related functions*//////////////

//For testing purposes;
func doOnWorkerJoin(workerIpPort string) {
	//askWorkerToCrawl(workerIpPort, []string{"http://www.ugrad.cs.ubc.ca/~cs411/2016w2/index.html"})
	//"http://www.cs.ubc.ca/~bestchai/teaching/cs416_2016w2/index.html" is too difficult
}

//returns the worker who owns the domain of the url given
func crawl(url string, depth int) string {

	var worker string

	//First, look up which worker owns the domain
	worker = lookupDomain(getDomain(url))

	conn := setupTCPConn(worker)

	client := rpc.NewClient(conn)
	crawlReq := &StoreAndCrawlReq{}
	crawlReq.Urls = []string{url}
	crawlReq.Depth = depth

	err := client.Call("WWorker.StoreAndCrawl", &crawlReq, &void)
	checkF(err)
	conn.Close()

	fmt.Println("A crawl chain of: ", url, " finished.")
	return worker
}

type AddDomainReq struct {
	Domain string
}

//Returns the worker who owns the domain. If there is none, measures and assigns a worker before returning
//Callee Locking (caller should release locks before calling)
func lookupDomain(domain string) string {
	domainOwnerMutex.Lock()
	defer domainOwnerMutex.Unlock()

	if domainOwner[domain] != "" {
		return domainOwner[domain]
	}

	//Otherwise, start up a measurement to determine who should be the owner.
	indexPage := "http://" + domain + "/index.html"
	fmt.Println("Requesting a measure of site:", indexPage)

	measureReq := &WWebsiteReq{}
	measureReq.SamplesPerWorker = 5 //Hard-coded
	measureReq.URI = indexPage

	var results map[string]LatencyStats = allMeasureWebSite(measureReq)

	//pick the worker with the lowest mininum record time

	var closestWorker string = ""
	var minLatency int = 9999999
	for workerIpPort, stats := range results {
		if closestWorker == "" {
			closestWorker = workerIpPort
			minLatency = stats.Min
		} else {
			if stats.Min < minLatency {
				closestWorker = workerIpPort
			}
		}
	}

	fmt.Println("Closest worker is ", closestWorker, "Latency: ", minLatency)

	//Set the domain owner to that worker
	domainOwner[domain] = closestWorker
	//Tell that worker that it now owns that domain.
	conn := setupTCPConn(closestWorker)

	client := rpc.NewClient(conn)
	domReq := &AddDomainReq{}
	domReq.Domain = domain

	err := client.Call("WWorker.AddDomain", &domReq, &void)
	checkF(err)
	conn.Close()

	return closestWorker
}

//General Types:

//Fields
var void Void

var clientConnMutex = &sync.Mutex{} //ensures that only one connection (worker or client) is active at one time

var (
	workersMutex                 = &sync.Mutex{}
	workers      map[string]bool = map[string]bool{} //Holds the list of all workers and info about each of them (currently no information)
)

var (
	domainOwnerMutex                   = &sync.Mutex{}
	domainOwner      map[string]string = map[string]string{} //Map between domain to IP of worker who owns it
)

//Main workhorse method
func main() {
	args := os.Args[1:]

	if len(args) != 2 {
		log.Fatal("server.go expected two arguments: [worker-incoming ip:port] [client-incoming ip:port] ")
	}

	workerListenIp := args[0]
	clientListenIp := args[1]

	workerListenPort := strings.Split(workerListenIp, ":")[1]
	clientListenPort := strings.Split(clientListenIp, ":")[1]

	go setupRPCService(new(WMaster), workerListenPort, nil)
	go setupRPCService(new(MServer), clientListenPort, clientConnMutex)

	blockForever()
}

//RPC implementation

//RPC for asking the workers
type WWorker int

type StoreAndCrawlReq struct {
	Urls  []string //should be absolute, canonical addresses
	Depth int
}

type WWebsiteReq struct {
	URI              string // URI of the website to measure
	SamplesPerWorker int    // Number of samples, >= 1
}

type LatencyStats struct {
	Min    int // min measured latency in milliseconds to host
	Median int // median measured latency in milliseconds to host
	Max    int // max measured latency in milliseconds to host
}

//Concurrently asks all workers to measure the website
func allMeasureWebSite(req *WWebsiteReq) map[string]LatencyStats {

	results := make(map[string]LatencyStats)
	resultsMutex := &sync.Mutex{}

	workersMutex.Lock()

	var wg sync.WaitGroup
	wg.Add(len(workers))

	log.Println("Current workers: ", workers)

	for workerIpPort, _ := range workers {
		fmt.Println("workerIpPort", workerIpPort)
		go measureWebSite(workerIpPort, req, results, resultsMutex, &wg)
		//todo: do something with the latency stats
	}
	workersMutex.Unlock()
	wg.Wait()
	log.Println("All results in\n", results)
	return results
}

func measureWebSite(workerIpPort string, req *WWebsiteReq, results map[string]LatencyStats, resultsMutex *sync.Mutex,
	wg *sync.WaitGroup) {

	defer wg.Done()

	conn := setupTCPConn(workerIpPort)
	//Make RPC call
	ls := LatencyStats{}
	client := rpc.NewClient(conn)
	err := client.Call("WWorker.MeasureWebsiteLatency", &req, &ls)
	checkF(err)
	conn.Close()
	log.Println("Worker {", workerIpPort, "} finished measuring:", ls)

	resultsMutex.Lock()
	results[workerIpPort] = ls
	resultsMutex.Unlock()
}

//RPC utility functions

func splicePort(ipPort string, actualPort string) string {
	ip := strings.Split(ipPort, ":")[0]
	return ip + ":" + actualPort
}

func setupTCPConn(ipPort string) *net.TCPConn {
	raddr, err := net.ResolveTCPAddr("tcp", ipPort)
	checkF(err)
	conn, err := net.DialTCP("tcp", nil, raddr)
	checkF(err)
	return conn
}

//URL helper functions

//Input: a canonical URL
func getDomain(url string) string {
	temp := strings.Split(url, "//")
	domain := strings.Split(temp[1], "/")[0]
	return domain
}

//Tries to set up an RPC service using the given rpc object and port number
//If given a mutex, the service will serve its connections exclusively with regards to that mutex
func setupRPCService(rpcObject interface{}, listenPort string, mutex *sync.Mutex) {
	rpc.Register(rpcObject)
	//wonder how we can check for error here
	log.Println("Listening for RPC requests")
	l, err := net.Listen("tcp", ":"+listenPort)
	check(err)

	for {
		conn, err := l.Accept()
		check(err)
		if err == nil {
			log.Println("Client arrived")
			if mutex != nil {
				mutex.Lock()
			}
			log.Println("Serving a client")
			rpc.ServeConn(conn)
			if mutex != nil {
				mutex.Unlock()
			}
		}
	}

}

///////////* Utility functions (non-project specific) *////////////

func check(err error) {
	if err != nil {
		log.Println(err)
	}
}

func checkF(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func Use(vals ...interface{}) {
	for _, val := range vals {
		_ = val
	}
}

func blockForever() {
	select {}
}
