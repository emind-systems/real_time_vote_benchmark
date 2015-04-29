package main

import (
	"fmt"
	"github.com/couchbaselabs/gocb"
	"github.com/couchbaselabs/gocb/gocbcore"
	"github.com/kr/pretty"
	"net/http"
	"runtime"
	"strconv"
	"time"
)

type Event struct {
	id    string
	event string
	value string
}

const (
	CacheLimit = 100
	okString   = "OK"
)

var (
	bucket  *gocb.Bucket
	cluster *gocb.Cluster
	ch      chan Event
	okBytes     = []byte(okString)
	i       int = 0
	CB      *gocb.Bucket
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
	bucket.Counter("total_votes", 1, 0, 0)
}

func main() {
	gocbcore.SetLogger(gocbcore.DefaultStdOutLogger())
	runtime.GOMAXPROCS(runtime.NumCPU())
	cluster, _ = gocb.Connect("http://172.31.31.191")
	bucket, _ = cluster.OpenBucket("default", "")
	bucket.Upsert("test", 0, 100)
	bucket.Upsert("total_votes", 0, 0)
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
	out, err := bucket.Insert(e.id, &e, 0)
	_ = err
	fmt.Println(err)
	fmt.Println(out)
	ch <- e
	w.Write([]byte(fmt.Sprintf("Vote")))
}

// printf warning message
func Check(err error, msg string, args ...interface{}) error {
	if err != nil {
		_, _, line, _ := runtime.Caller(1)
		str := fmt.Sprintf("d: ", line)
		fmt.Errorf(str+msg, args...)
		res := pretty.Formatter(err)
		fmt.Errorf("%# v\n", res)
	}
	return err
}

// print error message and exit program
func Panic(err error, msg string, args ...interface{}) {
	if Check(err, msg, args...) != nil {
		panic(err)
	}
}
