package main

import (
	"fmt"
	"gopkg.in/redis.v2"
	"net/http"
	//	"net/url"
	"io"
	"runtime"
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
	client  *redis.Client
	ch      chan Event
	okBytes = []byte(okString)
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
	client.Pipelined(func(c *redis.Pipeline) error {
		for index, element := range cache {
			_ = index
			//			fmt.Println(element.id, element.event, element.value)
			//			fmt.Println(index)
			//			c.Set(element.id, element.value)
			//			c.Get(element.id)
			c.Incr("total_votes")
			c.Incr(element.value)
		}
		return nil
	})
}

func vote(e Event) {
	//fmt.Println(e.id)
	v, err := client.Set(e.id, e.value).Result()
	_ = err
	if v == "OK" {
		fmt.Println("fffffff")
		client.Incr("total_votes")
		client.Incr(e.value)
	}
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	// defer client.Close()
	client = redis.NewTCPClient(&redis.Options{
		Addr:     "172.31.53.226:11684",
		PoolSize: 250,
	})
	client.FlushDb()
	ch = make(chan Event, 10000)
	ping := client.Ping()
	set := client.Set("test", "123")
	fmt.Println(ping.Err(), ping.Val())
	fmt.Println(set.Err(), set.Val())
	time.AfterFunc(1*time.Second, func() {
		for i := 1; i < 0; i++ {
			fmt.Println("starting worker %d", i)
			go worker(ch)
		}
	})

	for i := 1; i < 100000000; i++ {
		go func() {
			_, _ = client.Set("test", "i").Result()
			fmt.Println("7777")
		}()
	}
	http.HandleFunc("/vote", voteHandler2)
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

	//	u, err := url.Parse(r.URL.String())
	//	if err != nil {
	//		panic(err)
	//	}

	//	m, _ := url.ParseQuery(u.RawQuery)
	e := Event{id: r.FormValue("u"), event: "vote", value: r.FormValue("v")}
	// e := Event{id: m["u"][0], event: "vote", value: m["v"][0]}
	//fmt.Println(e)

	// set vote lock
	v, err := client.SetNX(e.id, e.value).Result()
	_ = err
	//fmt.Println(v)
	if v {
		ch <- e
		w.Write([]byte(fmt.Sprintf("Vote")))
	} else {
		w.Write([]byte(fmt.Sprintf("Duplicate")))
	}
}

func voteHandler2(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain")

	//	u, err := url.Parse(r.URL.String())
	//	if err != nil {
	//		panic(err)
	//	}

	e := Event{id: r.FormValue("u"), event: "vote", value: r.FormValue("v")}
	go vote(e)
	io.WriteString(w, "OK\n") //	m, _ := url.ParseQuery(u.RawQuery)
	//	w.Write([]byte(okString))
}
