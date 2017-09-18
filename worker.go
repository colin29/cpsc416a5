package main

import (
	"errors"
	"fmt"
	"golang.org/x/net/html"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

//RPC Shared Types

type Void struct {
}

//RPC for serving the server
type WWorker int

type WWebsiteReq struct {
	URI              string // URI of the website to measure
	SamplesPerWorker int    // Number of samples, >= 1
}
type LatencyStats struct {
	Min    int // min measured latency in milliseconds to host
	Median int // median measured latency in milliseconds to host
	Max    int // max measured latency in milliseconds to host
}

type AddDomainReq struct {
	Domain string
}

func (t *WWorker) MeasureWebsiteLatency(req *WWebsiteReq, reply *LatencyStats) error {
	log.Println("Recieved MeasureWebsite Request")
	testWebSiteLatency(req.URI, req.SamplesPerWorker, reply)
	return nil
}

func (t *WWorker) AddDomain(req *AddDomainReq, reply *Void) error {

	domainsOwnedMutex.Lock()
	domainsOwned[req.Domain] = true // value doesn't matter
	fmt.Println("Worker: Recieved domain ", req.Domain)
	domainsOwnedMutex.Unlock()

	return nil
}

/////////////*RPC Service for the Server*/////////////

/*
List of functions:
MeasureWebsiteLatency
AddDomain
StoreAndCrawl
*/

type StoreAndCrawlReq struct {
	Urls  []string //should be absolute, canonical addresses
	Depth int
}

//Input: Req with a list of urls and a crawl depth
//Effects: Tells the worker to store nodes of these urls, and then crawl and populate the fields of those nodes.
//-These urls are assumed to be canonical; futhermore it is assumed that the worker already owns the domain to these urls
func (t *WWorker) StoreAndCrawl(req *StoreAndCrawlReq, reply *Void) error {
	fmt.Printf("Received request to store and crawl %v\n", req.Urls)
	storeAndCrawl(req.Urls, req.Depth)

	printGraph(nodes)
	return nil
}

type CaculateOverlapReq struct {
	Url1    string
	Url2    string
	Worker2 string
}

func (t *WWorker) CalculateOverlap(req *CaculateOverlapReq, reply *int) error {
	fmt.Println("Recieved CaculateOverlap Request")

	//Step 1: calculate the list of accessible urls within this domain, starting from url1

	accessiblePages, err := getAccessiblePages(req.Url1)
	Use(accessiblePages, err)
	if err != nil {
		return err
	}
	fmt.Println(accessiblePages)

	//Step 2: call the required worker2 to do one direction of the overlap calculations.

	if req.Worker2 == myID {
		fmt.Println("Case where one worker owns both domains")

		accessiblePages2, err := getAccessiblePages(req.Url2)
		subTotal, err := calculateOneWayOverlap(accessiblePages2, accessiblePages)
		if err != nil {
			return err
		}
		subTotal2, err := calculateOneWayOverlap(accessiblePages, accessiblePages2)
		if err != nil {
			return err
		}
		*reply = subTotal + subTotal2
		fmt.Println("Finished Overlap Request")
		return nil
	}

	fmt.Println("sending request to other worker")
	conn := setupTCPConn(req.Worker2)

	client := rpc.NewClient(conn)

	r := CalculateOverlapHelperReq{}
	r.AccessiblePagesUrl1 = accessiblePages
	r.Url2 = req.Url2

	ans := CalculateOverlapHelperRes{}

	err = client.Call("WWorker.CalculateOverlapHelper", &r, &ans)
	checkF(err)
	conn.Close()

	//Step 3: use the information to do the calculation of the other direction

	fmt.Println("Calculating second half of overlap")
	subTotal, err := calculateOneWayOverlap(accessiblePages, ans.AccessiblePagesUrl2)
	if err != nil {
		return err
	}

	//Combine results
	*reply = ans.SubTotal + subTotal

	fmt.Println("Finished Overlap Request")

	return nil
}

type CalculateOverlapHelperReq struct {
	AccessiblePagesUrl1 []string
	Url2                string
}

type CalculateOverlapHelperRes struct {
	AccessiblePagesUrl2 []string
	SubTotal            int
}

func (t *WWorker) CalculateOverlapHelper(req *CalculateOverlapHelperReq, reply *CalculateOverlapHelperRes) error {
	domain := getDomain(req.Url2)
	pages := make([]string, 0)

	nodesMutex.Lock()
	err := getPagesHelper(req.Url2, &pages, domain)
	check(err)
	nodesMutex.Unlock()

	(*reply).AccessiblePagesUrl2 = pages

	//Todo: calculate overlap one-way from D2 -> D1
	subTotal, err := calculateOneWayOverlap(pages, req.AccessiblePagesUrl1)
	if err != nil {
		return err
	}
	reply.SubTotal = subTotal

	return nil
}

//Returns a list of pages accessibe from the given url, within the same domain.
func getAccessiblePages(url string) (pages []string, err error) {
	domain := getDomain(url)

	pages = make([]string, 0)

	nodesMutex.Lock()
	err = getPagesHelper(url, &pages, domain)
	check(err)
	nodesMutex.Unlock()
	return
}

//depth first flood-fill
func getPagesHelper(url string, pages *[]string, domain string) error {
	*pages = append(*pages, url)
	node, ok := nodes[url]
	if !ok {
		return errors.New("Url: " + url + " doesn't have a node for it")
	}

	for _, linkUrl := range node.childNodes {
		fmt.Println("link:  ", linkUrl)
		if getDomain(linkUrl) != domain {
			fmt.Println("External link, skipping.")
			continue
		}
		if !contains(*pages, linkUrl) {
			err := getPagesHelper(linkUrl, pages, domain)
			if err != nil {
				return err
			}
		} else {
			fmt.Println("Already visited node: ", linkUrl)
			continue
		}
	}
	return nil
}

//Callee locking
func calculateOneWayOverlap(accessiblePages []string, pagesFromOtherDomain []string) (int, error) {
	total := 0
	nodesMutex.Lock()
	for _, url := range accessiblePages {
		node, ok := nodes[url]
		if !ok {
			return -1, errors.New("Error: supposedly accessible page " + url + " is not in crawl-store.")
			continue
		}
		for _, linkUrl := range node.childNodes {
			if contains(pagesFromOtherDomain, linkUrl) {
				total += 1
				fmt.Println("+1 overlap")
			}
		}

	}
	nodesMutex.Unlock()
	return total, nil
}

//Local version of StoreAndCrawl
//Effects: Tells the worker to store nodes of these urls, and then crawl and populate the fields of those nodes.
//-These urls are assumed to be canonical; futhermore it is assumed that the worker already owns the domain to these urls
func storeAndCrawl(urls []string, depth int) {
	fmt.Printf("Crawling %v sites: (depth %v)  ", len(urls), depth)
	for _, url := range urls {
		node := PageNode{}

		//Later:

		nodesMutex.Lock()
		_, ok := nodes[url]
		if ok {
			fmt.Println("url: ", url, " is already stored, skipping.")
			nodesMutex.Unlock()
			continue
		}
		nodesMutex.Unlock()

		//Check that this worker owns the url's domain

		haveit := doIHaveDomain(getDomain(url))

		if !haveit {
			fmt.Println("Error: this worker doesn't own domain for url: ", url)
			continue
		}

		//Store the unfinished node temporarily
		nodesMutex.Lock()
		nodes[url] = node
		nodesMutex.Unlock()

		if depth == 0 {
			return //if the depth is zero, we just leave create an empty node for that url, we don't inspect the page or add any more edges
		}

		//Crawl the url
		pageInfo := crawlPage(url)
		node.childNodes = pageInfo.urlLinks

		//Store the complete node
		nodesMutex.Lock()
		nodes[url] = node
		nodesMutex.Unlock()

		if depth > 0 {
			for _, linkUrl := range node.childNodes {

				domain := getDomain(linkUrl)
				haveit := doIHaveDomain(domain)

				if haveit {
					storeAndCrawl([]string{linkUrl}, depth-1) //store and crawl accepts a slice
				} else {
					askServerToCrawlFurther(linkUrl, domain, depth-1)
				}
			}

		}

		//We Now need to start a crawl on all the next pages.
		/*
			If we own the domain, crawl the url {depth -1}
			If we don't own the domain,ask the server who owns it.
				If it's us, record that we own the domain, and crawl the url {depth -1}
				If it's not us, send a request to that worker {StoreAndCrawl, with depth -1}
		*/
	}

}

//Checks locally to see if this worker owns that domain
//Callee Locking
func doIHaveDomain(domain string) bool {
	domainsOwnedMutex.Lock()
	_, ok := domainsOwned[domain]
	domainsOwnedMutex.Unlock()
	return ok
}

/*  Constants */
const maxNumOfTests int = 5 //DON't TOUCH THIS. Minor safeguard against adding huge amounts of digits
const runTests bool = false

//Fields
var void Void

var (
	domainsOwnedMutex                 = sync.Mutex{}
	domainsOwned      map[string]bool = make(map[string]bool) //is just a set of all domains owned.
)

type PageNode struct {
	childNodes []string
}

var (
	nodesMutex                     = sync.Mutex{}
	nodes      map[string]PageNode = make(map[string]PageNode)
)

var serverIpPort string
var myID string //the worker's id is equal to the ip:port that it registered with the server

var workerPort string

//Main workhorse Method
func main() {
	//Handle Arguments
	args := os.Args[1:]

	if len(args) != 2 {
		log.Fatal("worker.go expected two arguments: [server ip:port] [worker port] ")
	}

	serverIpPort = args[0]
	workerPort = args[1]

	// if runTests {
	// 	RunURLTests()
	// }

	go serveServerRequests(workerPort)
	registerWithServer(serverIpPort)

	//Testing Area

	//fmt.Println("owner of: ", askServerWhoOwnsDomain("www.hpcs.cs.tsukaba.ac.jp"))
	// http://www.ugrad.cs.ubc.ca/~cs411/2016w2/index.html
	// http://www.cs.ubc.ca/~bestchai/teaching/cs416_2016w2/index.html

	//Lastly:
	blockForever()

}

//returns: nothing
//Callee Locking
func askServerToCrawlFurther(url string, domain string, depth int) {
	conn := setupTCPConn(serverIpPort)
	client := rpc.NewClient(conn)
	req := &CrawlFurtherReq{}
	req.URL = url
	req.Domain = domain
	req.Depth = depth

	fmt.Println("Calling server CrawlFurther: ", domain, "  ", serverIpPort)
	err := client.Call("WMaster.CrawlFurther", &req, &void)
	checkF(err)
	conn.Close()
	fmt.Printf("Request sent")
}

type pageInfo struct {
	urlLinks []string
}

//RPC Service Implementation

//Input: url of target page
//Returns: Information from the target page, in this case with all the links found
func crawlPage(pageUrl string) pageInfo {
	resp, err := http.Get(pageUrl)
	checkF(err)

	Use(ioutil.ReadAll)
	Use(fmt.Println)

	listOfNodes, err := html.ParseFragment(resp.Body, nil)
	checkF(err)
	resp.Body.Close()

	nodeTree := listOfNodes[0] //expect exactly one node (the html node)
	links := make([]string, 0)
	getLinks(nodeTree, &links)
	fmt.Printf("Crawled [%v]: (%v links found)\n", pageUrl, len(links))
	for i, _ := range links {
		rawUrl := links[i]
		urlInfo, err := parseHTMLURL(pageUrl, rawUrl)
		check(err)
		links[i] = urlInfo.Url
	}

	info := pageInfo{}
	info.urlLinks = links

	return info
}

func getLinks(node *html.Node, accum *[]string) {
	if node == nil {
		return
	}

	if node.Type == html.ElementNode {
		if node.Data == "a" {
			for _, attr := range node.Attr {
				// fmt.Printf("[%v], %v\n", attr.Key, attr.Val)
				if attr.Key == "href" && strings.HasSuffix(attr.Val, ".html") {
					// fmt.Println(attr.Val)
					*accum = append(*accum, attr.Val)
				}
			}
		}
	}

	if node.FirstChild != nil {
		getLinks(node.FirstChild, accum)
	}
	if node.NextSibling != nil {
		getLinks(node.NextSibling, accum)
	}
}

func testWebSiteLatency(url string, timesToTest int, ls *LatencyStats) {

	var lats []int //download latencies of the different runs

	if timesToTest > maxNumOfTests {
		timesToTest = maxNumOfTests
	}

	for i := 0; i < timesToTest; i++ {
		startTime := time.Now()
		resp, err := http.Get(url)
		endTime := time.Now()

		timeToRetrieve := int(endTime.Sub(startTime).Seconds() * 1000)
		log.Println("downloadLatency:", timeToRetrieve)
		lats = append(lats, timeToRetrieve)

		checkF(err)
		log.Println(resp.Status)

		defer resp.Body.Close()
		//_, err = io.Copy(os.Stdout, resp.Body)
		// checkF(err)
	}
	*ls = *calculateLatencyStats(lats)
	log.Println(ls)
}

//Input: a slice with at least one reading.
func calculateLatencyStats(lats []int) *LatencyStats {
	sort.Ints(lats)

	ls := new(LatencyStats)
	ls.Max = lats[len(lats)-1]
	ls.Min = lats[0]

	N := len(lats)
	if N%2 == 1 {
		ls.Median = lats[(N-1)/2]
	} else {
		ls.Median = (lats[(N-1)/2] + lats[(N-1)/2+1]) / 2
	}
	return ls
}

//RPC for calling the Server
type MServer int

type WRegisterWorkerReq struct {
	WorkerIp string
}

type CrawlFurtherReq struct {
	Domain string
	URL    string
	Depth  int // new depth
}

//RPC Setup
func registerWithServer(serverIpPort string) {
	//Connect to Server
	raddr, err := net.ResolveTCPAddr("tcp", serverIpPort)
	checkF(err)
	conn, err := net.DialTCP("tcp", nil, raddr)
	checkF(err)

	//Make RPC call
	client := rpc.NewClient(conn)

	registerAddr := splicePort(conn.LocalAddr().String(), workerPort)

	req := WRegisterWorkerReq{WorkerIp: registerAddr}
	myID = registerAddr //have worker remember it's own ID
	err = client.Call("WMaster.RegisterWorker", &req, &void)
	checkF(err)
	conn.Close()
}

// const serverListenPort = "40009"

func serveServerRequests(listenPort string) {
	wWorker := new(WWorker)
	rpc.Register(wWorker)
	setupRPCService(wWorker, listenPort, nil)
}

//General helper functions

//Callee Locking
func printGraph(nodes map[string]PageNode) {

	nodesMutex.Lock()
	defer nodesMutex.Unlock()

	temp := []string{}
	for url, _ := range nodes {
		temp = append(temp, url)
	}
	sort.Strings(temp)

	for _, url := range temp {
		fmt.Println("Page: ", url, " # of links: ", len(nodes[url].childNodes))
		for _, linkUrl := range nodes[url].childNodes {
			fmt.Println("-", linkUrl)
		}
	}
}

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

//Tries to set up an RPC service using the given rpc object and port number
//	-If given a mutex, the service will serve its connections exclusively with regards to that mutex
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

//URL helper functions
type linkType int

const (
	RELATIVE = iota
	ABSOLUTE
)

type urlInfo struct {
	linkType linkType
	Url      string
}

//Input: a canonical URL
func getDomain(url string) string {
	temp := strings.Split(url, "//")
	domain := strings.Split(temp[1], "/")[0]
	return domain
}

/*
Input: the absolute of the parent page, the raw url "href" attribute
Return Value: the resolved absolute url of the link, or an error if either url is invalid or either is not an html page.
	-An Absolute URL has the format: 'http:// {domain} (/) (path)', ending in html
*/
func parseHTMLURL(parentPage string, linkUrl string) (urlInfo, error) {
	info := urlInfo{} //the resulant url info of the link

	temp := strings.Split(parentPage, "//")
	//log.Printf("temp: %v %v \n", len(temp), temp)
	if len(temp) >= 3 {
		return urlInfo{}, errors.New("invalid url syntax, multiple //'s")
	}
	if len(temp) == 1 {
		return urlInfo{}, errors.New("parentPage url expected to be an absolute url")
	}
	var parentDomainPath string = temp[1]

	if parentDomainPath == "" {
		return info, errors.New("domain in url is blank")
	}

	temp2 := strings.SplitN(parentDomainPath, "/", 2)
	// log.Printf("parentDomainPath: %v\n", parentDomainPath)
	// log.Printf("temp2: %v %v \n", len(temp2), temp2)
	if len(temp2) == 1 {
		return info, errors.New("expected a / marking the end of the domain")
	}
	parentDomain := temp2[0]
	path := temp2[1]
	//fmt.Printf("parentDomain: %v, path: %v\n", parentDomain, path)

	pathChunks := strings.Split(path, "/")
	//log.Printf("Parent url path has %v chunks: %v", len(pathChunks), pathChunks)
	fileName := pathChunks[len(pathChunks)-1]
	if !strings.HasSuffix(fileName, ".html") {
		return info, errors.New("url does not end with .html")
	}
	containingDir := strings.Join(pathChunks[:len(pathChunks)-1], "/")
	if containingDir != "" {
		containingDir = "/" + containingDir
	}
	// log.Printf("parent containingDir: %#v", containingDir)

	//Deal with the link url

	//Determine whether link is a relative or absolute url
	temp = strings.Split(linkUrl, "//")
	if len(temp) == 1 {
		info.linkType = RELATIVE
	} else if len(temp) == 2 {
		info.linkType = ABSOLUTE
	} else {
		return info, errors.New("invalid url syntax, multiple //'s")
	}

	if info.linkType == ABSOLUTE {
		// log.Printf("Parsing Absolute link:")

		//get domain and path
		temp := strings.SplitN(linkUrl, "/", 2)
		if len(temp) != 2 {
			return info, errors.New("link: expected a / marking the end of the domain")
		}
		domain := temp[0]
		path := temp[1]

		pathChunks := strings.Split(path, "/")
		// log.Printf("Link url path has %v chunks: %v", len(pathChunks), path)
		fileName = pathChunks[len(pathChunks)-1]
		if !strings.HasSuffix(fileName, ".html") {
			return info, errors.New("url does not end with .html")
		}
		info.Url = domain + "/" + path
		return info, nil

	} else { //info.linkType == RELATIVE
		//log.Printf("Parsing Relative link:")
		linkPath := linkUrl

		pathChunks := strings.Split(path, "/")
		// log.Printf("Link url path has %v chunks: %v", len(pathChunks), path)
		fileName = pathChunks[len(pathChunks)-1]
		if !strings.HasSuffix(fileName, ".html") {
			return info, errors.New("link url does not end with .html")
		}

		//canonalize the path
		rawPath := containingDir + "/" + linkPath
		path, err := canonicalizePath(rawPath)
		if err != nil {
			return info, err
		}

		info.Url = "http://" + parentDomain + path //store the absolute url
		return info, nil
	}

}

/*
Input: takes a string that represents a path and resolves all "./" and "../" fragments. Returns an error if resolving fails.
	-Assumes path is well formed, with the form "/foo/fileName"
*/
func canonicalizePath(path string) (string, error) {
	temp1 := strings.Split(path, "/")

	//remove /./ style fragments
	var temp2 []string
	for _, val := range temp1 {
		if val != "." {
			temp2 = append(temp2, val)
		}
	}

	//process out /../ style fragments
	const emptyPathSize = 1
	var temp3 []string
	for _, val := range temp2 {

		if val == ".." {
			if len(temp3) == emptyPathSize {
				return "", errors.New("canonize url: out of bounds from /../ ")
			}
			highestIndex := len(temp3) - 1
			temp3 = append(temp3[:highestIndex], temp3[highestIndex+1:]...)
		} else {
			temp3 = append(temp3, val)
		}
	}

	// log.Printf("temp3: %v\n", temp3)

	canonUrl := strings.Join(temp3, "/")

	return canonUrl, nil
}

///////////* Utility functions (non-project specific) *////////////

func contains(s []string, e string) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}

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
