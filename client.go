/*
Implements the client in assignment 5 for UBC CS 416 2016 W2.

Usage:

GetWorkers:
go run client.go -g [server ip:port]

Crawl:
go run client.go -c [server ip:port] [url] [depth]

Domains:
go run client.go -d [server ip:port] [workerIP]

Overlap:
go run client.go -o [server ip:port] [url1] [url2]

EC1 (Page rank):
go run client.go -r [server ip:port] [url1]

EC2 (Search):
go run client.go -s [server ip:port] [string]

Example:
go run client.go -g 127.0.0.1:19001

*/

package main

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"strconv"
)

var (
	logger       *log.Logger // Global logger.
	client       *rpc.Client // RPC client.
	serverIpPort string      // RPC server (-g, -c, -d, -o, -r, -s)
	urlToCrawl   string      // URL to crawl (-c)
	depth        int         // Depth to Crawl (-c)
	workerIP     string      // Worker IP (-d)
	url1         string      // Url1 (-o)
	url2         string      // Url2 (-o)
	urlToRank    string      // URL to compute page rank for (-r)
	searchStr    string      // String to search for (-s)
)

//Modes of operation
const (
	GETWORKERS = iota
	CRAWL
	DOMAINS
	OVERLAP
	PAGERANK
	SEARCH
)

/////////////// RPC structs

// Resource server type.
type MServer int

/////////

// Request that client sends in RPC call to MServer.GetWorkers
type GetWorkersReq struct{}

// Response to MServer.GetWorkers
type GetWorkersRes struct {
	WorkerIPsList []string // List of workerIP string
}

/////////

// Request that client sends in RPC call to MServer.Crawl
type CrawlReq struct {
	URL   string // URL of the website to crawl
	Depth int    // Depth to crawl to from URL
}

// Response to MServer.Crawl
type CrawlRes struct {
	WorkerIP string // workerIP
}

/////////

// Request that client sends in RPC call to MServer.Domains
type DomainsReq struct {
	WorkerIP string // IP of worker
}

// Response to MServer.Domains
type DomainsRes struct {
	Domains []string // List of domain string
}

/////////

// Request that client sends in RPC call to MServer.Overlap
type OverlapReq struct {
	URL1 string // URL arg to Overlap
	URL2 string // The other URL arg to Overlap
}

// Response to MServer.Overlap
type OverlapRes struct {
	NumPages int // Computed overlap between two URLs
}

/////////

// Request that client sends in RPC call to MServer.PageRank
type PageRankReq struct {
	UrlToRank string // URL arg to PageRank
}

// Response to MServer.PageRank
type PageRankRes struct {
	PageRank float32 // Computed page rank for a page
}

/////////

// Request that client sends in RPC call to MServer.Search
type SearchReq struct {
	SearchStr string // String to search for
}

// Response to MServer.Search
type SearchRes struct {
	URLs []string // List of URLs containing SearchStr
}

/////////////// /RPC structs

// Main workpuppy method.
func main() {
	// Parse the command line args, panic if error
	mode, err := ParseArguments()
	if err != nil {
		panic(err)
	}

	// Create RPC client for contacting the server.
	client = getRPCClient()

	switch mode {
	case GETWORKERS:
		req := GetWorkersReq{}
		fmt.Printf("Req: %+v\n", req)
		var res GetWorkersRes
		err := client.Call("MServer.GetWorkers", req, &res)
		if err != nil {
			fmt.Println(err)
		}
		fmt.Printf("Res: %+v\n", res)

	case CRAWL:
		req := CrawlReq{
			URL:   urlToCrawl,
			Depth: depth,
		}
		fmt.Printf("Req: %+v\n", req)
		var res CrawlRes
		err := client.Call("MServer.Crawl", req, &res)
		if err != nil {
			fmt.Println(err)
		}
		fmt.Printf("Res: %+v\n", res)

	case DOMAINS:
		req := DomainsReq{
			WorkerIP: workerIP,
		}
		fmt.Printf("Req: %+v\n", req)
		var res DomainsRes
		err := client.Call("MServer.Domains", req, &res)
		if err != nil {
			fmt.Println(err)
		}
		fmt.Printf("Res: %+v\n", res)

	case OVERLAP:
		req := OverlapReq{
			URL1: url1,
			URL2: url2,
		}
		fmt.Printf("Req: %+v\n", req)
		var res OverlapRes
		err := client.Call("MServer.Overlap", req, &res)
		if err != nil {
			fmt.Println(err)
		}
		fmt.Printf("Res: %+v\n", res)

	case PAGERANK:
		req := PageRankReq{
			UrlToRank: urlToRank,
		}
		fmt.Printf("Req: %+v\n", req)
		var res PageRankRes
		err := client.Call("MServer.PageRank", req, &res)
		if err != nil {
			fmt.Println(err)
		}
		fmt.Printf("Res: %+v\n", res)

	case SEARCH:
		req := SearchReq{
			SearchStr: searchStr,
		}
		fmt.Printf("Req: %+v\n", req)
		var res SearchRes
		err := client.Call("MServer.Search", req, &res)
		if err != nil {
			fmt.Println(err)
		}
		fmt.Printf("Res: %+v\n", res)

	default:
		err = fmt.Errorf("Invalid mode")
	}
	client.Close()
}

// Parses the command line args.
func ParseArguments() (mode int, err error) {
	args := os.Args[1:]

	if len(args) < 2 {
		err = fmt.Errorf("Usage: go run client.go [-g,-c,-d,-o,-r,-s] [...]")
		return
	}

	serverIpPort = args[1]

	if len(args) == 2 && args[0] == "-g" {
		mode = GETWORKERS
	} else if len(args) == 4 && args[0] == "-c" {
		mode = CRAWL
		urlToCrawl = args[2]
		depth, err = strconv.Atoi(args[3])
		if err != nil {
			logger.Fatal(err)
		}
	} else if len(args) == 3 && args[0] == "-d" {
		mode = DOMAINS
		workerIP = args[2]
	} else if len(args) == 4 && args[0] == "-o" {
		mode = OVERLAP
		url1 = args[2]
		url2 = args[3]
	} else if len(args) == 3 && args[0] == "-r" {
		mode = PAGERANK
		urlToRank = args[2]
	} else if len(args) == 3 && args[0] == "-s" {
		mode = SEARCH
		searchStr = args[2]
	} else {
		err = fmt.Errorf("Usage: go run client.go [-g,-c,-d,-o,-r,-s] [...]")
		return
	}
	return
}

// Create RPC client for contacting the server.
func getRPCClient() *rpc.Client {
	raddr, err := net.ResolveTCPAddr("tcp", serverIpPort)
	if err != nil {
		logger.Fatal(err)
	}
	conn, err := net.DialTCP("tcp", nil, raddr)
	if err != nil {
		logger.Fatal(err)
	}
	client := rpc.NewClient(conn)

	return client
}
