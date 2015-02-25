package main

import (
	"fmt"
	as "github.com/aerospike/aerospike-client-go"
	"net/http"
	//	"net/url"
	"runtime"
	"strconv"
	"time"
)

type Event struct {
	id    string
	event string
	value string
	ip    string
	time  string
}

const (
	CacheLimit = 1
)

var (
	client      *as.Client
	ch          chan Event
	err         error
	writePolicy *as.WritePolicy
	i           int = 0
)

func worker(c <-chan Event) {
	fmt.Println("started worker !")
	cache := make([]Event, 0, CacheLimit)
	tick := time.NewTicker(1 * time.Second)

	for {
		select {
		case m := <-c:
			cache = append(cache, m)
			//fmt.Println("%d", len(cache))
			if len(cache) >= CacheLimit {
				send(cache)
				cache = cache[:0]
			}
		case <-tick.C:
			if len(cache) > 0 {
				send(cache)
				cache = cache[:0]
			}
		}
	}
}

func send(cache []Event) {
	//fmt.Println("sending %s", len(cache))
	for index, element := range cache {
		_ = index
		//fmt.Println(element.id, element.event, element.value)
		//fmt.Println(index)
		key, _ := as.NewKey("test", "totals", "total_votes")
		ops := []*as.Operation{
			as.AddOp(as.NewBin("total_votes", 1)), // add the value of the bin to the existing value
			as.GetOp(),                            // get the value of the record after all operations are executed
		}

		_, _ = client.Operate(nil, key, ops...)

		key, _ = as.NewKey("test", "totals", element.value)
		ops = []*as.Operation{
			as.AddOp(as.NewBin(element.value, 1)), // add the value of the bin to the existing value
			as.GetOp(),                            // get the value of the record after all operations are executed
		}

		_, _ = client.Operate(nil, key, ops...)
		//panicOnError(err)
		//fmt.Println(rec)

		//		client.Set(element.id, element.value)
		//		client.Get(element.id)
		//		client.Incr("total_votes")
		//		client.Incr(element.value)
	}
}

func panicOnError(err error) {
	if err != nil {
		panic(err)
	}
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	// defer client.Close()
	clientPolicy := as.NewClientPolicy()
	clientPolicy.ConnectionQueueSize = 100
	clientPolicy.Timeout = 50 * time.Millisecond
	client, err = as.NewClient("172.31.53.227", 3000)
	panicOnError(err)

	writePolicy = as.NewWritePolicy(0, 0)
	ch = make(chan Event, 1000)
	key, err := as.NewKey("test", "totals", "test")
	panicOnError(err)
	// define some bins with data
	bins := as.BinMap{
		"value": 100,
	}
	err = client.Put(nil, key, bins)
	panicOnError(err)

	time.AfterFunc(1*time.Second, func() {
		for i := 1; i < 74; i++ {
			fmt.Println("starting worker %d", i)
			go worker(ch)
		}
	})

	http.HandleFunc("/vote", voteHandler)
	http.HandleFunc("/loaderio-35df9c4fffde902e3b0e3e0115816d82.html", validationHandler)
	http.ListenAndServe(":80", nil)
}

func validationHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	w.Write([]byte(fmt.Sprintf("loaderio-35df9c4fffde902e3b0e3e0115816d82")))
}

func voteHandler(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	w.Header().Set("Content-Type", "text/plain")

	e := Event{id: r.FormValue("u"), event: "vote", value: r.FormValue("v")}

	//fmt.Println(i)
	i = i + 1
	e.id = strconv.Itoa(i)
	key, err := as.NewKey("test", "votes", e.id)
	bins := as.BinMap{
		"v": e.value,
	}
	err = client.Put(writePolicy, key, bins)
	panicOnError(err)
	_ = err
	if err == nil {
		ch <- e
		//	fmt.Println(i)

		w.Write([]byte(fmt.Sprintf("Vote")))
	} else {
		w.Write([]byte(fmt.Sprintf("Duplicate")))
	}
}
