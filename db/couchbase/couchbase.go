package main

import (
	"fmt"
	"github.com/couchbaselabs/go-couchbase"
	"net/http"
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
	CacheLimit = 100
	okString   = "OK"
)

var (
	bucket  *couchbase.Bucket
	ch      chan Event
	okBytes     = []byte(okString)
	i       int = 0
)

func worker(c <-chan Event) {
	fmt.Println("started worker !")

	for {
		select {
		case m := <-c:
			send(m)
		}
	}
}

func send(e Event) {
	bucket.Incr("total_votes", 1, 0, 0)
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	// defer client.Close()
	client, err := couchbase.Connect("http://172.31.53.228:8091/")
	if err != nil {
		fmt.Println("Error connecting:  %v", err)
	}
	pool, err2 := client.GetPool("default")
	if err2 != nil {
		fmt.Println("Error getting pool:  %v", err2)
	}

	bucket, err = pool.GetBucket("default")
	if err2 != nil {
		fmt.Println("Error getting bucket:  %v", err2)
	}
	bucket.Set("test", 0, 100)
	bucket.Set("total_votes", 0, 0)
	ch = make(chan Event, 1000)
	time.AfterFunc(1*time.Second, func() {
		for i := 1; i < 72; i++ {
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
	//fmt.Println(e)
	i = i + 1
	e.id = strconv.Itoa(i)
	// set vote lock
	err := bucket.Set(e.id, 0, map[string]interface{}{"vote": e.value})
	_ = err
	//fmt.Println(v)
	ch <- e
	w.Write([]byte(fmt.Sprintf("Vote")))
}
