package main

import (
	"fmt"
	"github.com/gocql/gocql"
	"log"
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
	//session *gocql.Session
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
	if err := session.Query(`UPDATE counters SET value=value+1 WHERE id = ?`, e.value).Exec(); err != nil {
		log.Fatal(err)
	}
	if err := session.Query(`UPDATE counters SET value=value+1 WHERE id = ?`, "total_votes").Exec(); err != nil {
		log.Fatal(err)
	}
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	// defer client.Close()
	// connect to the cluster
	cluster := gocql.NewCluster("172.31.53.229")
	cluster.Keyspace = "example"
	//cluster.Consistency = gocql.Quorum
	session, _ := cluster.CreateSession()

	if err := session.Query(`INSERT INTO totals (id,value) VALUES (?, ?)`,
		"total_votes", 0).Exec(); err != nil {
		log.Fatal(err)
	}
	if err := session.Query(`INSERT INTO totals (id,value) VALUES (?, ?)`,
		"y", 0).Exec(); err != nil {
		log.Fatal(err)
	}
	if err := session.Query(`INSERT INTO totals (id,value) VALUES (?, ?)`,
		"n", 0).Exec(); err != nil {
		log.Fatal(err)
	}
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

	if err := session.Query(`INSERT INTO votes (id,value) VALUES (?, ?)`,
		e.id, e.value).Exec(); err != nil {
		log.Fatal(err)
	}
	//fmt.Println(v)
	ch <- e
	w.Write([]byte(fmt.Sprintf("Vote")))
}
