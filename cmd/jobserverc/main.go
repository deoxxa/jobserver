package main

import (
	"fmt"
	"os"
	"time"

	"fknsrs.biz/p/jobserver"
	_ "github.com/mattn/go-sqlite3"
	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	app                 = kingpin.New("jobserverd", "Job server using SQLite as a backend.")
	addr                = app.Flag("addr", "Address of job server.").Default("127.0.0.1:2097").Envar("ADDR").String()
	pingCommand         = app.Command("ping", "Ping the job server.")
	putCommand          = app.Command("put", "Put a job into a queue, or update an existing job.")
	putCommandQueue     = putCommand.Arg("queue", "Queue to put the job into.").Required().String()
	putCommandID        = putCommand.Arg("id", "Identifier for the job.").Required().String()
	putCommandContent   = putCommand.Arg("content", "Content of the job.").Required().String()
	putCommandPriority  = putCommand.Flag("priority", "Priority of the job.").Default("0").Float64()
	putCommandHoldUntil = putCommand.Flag("hold_until", "Hold the job until this time.").String()
	putCommandHoldFor   = putCommand.Flag("hold_for", "Hold the job for this amout of time.").Duration()
	putCommandTTR       = putCommand.Flag("ttr", "Time-to-run for the job.").Default("5m").Duration()
	reserveCommand      = app.Command("reserve", "Try to reserve a job from a queue.")
	reserveCommandQueue = reserveCommand.Arg("queue", "Queue to try to reserve a job from.").Required().String()
	deleteCommand       = app.Command("delete", "Delete a job.")
	deleteCommandID     = deleteCommand.Arg("id", "Identifier of the job to delete.").Required().String()
)

func main() {
	cmd := kingpin.MustParse(app.Parse(os.Args[1:]))

	c, err := jobserver.Dial(*addr)
	if err != nil {
		panic(err)
	}

	switch cmd {
	case pingCommand.FullCommand():
		d, err := c.Ping()
		if err != nil {
			panic(err)
		}
		fmt.Println(d)
	case putCommand.FullCommand():
		holdUntil := time.Now()
		switch {
		case *putCommandHoldUntil != "":
			t, err := time.Parse(time.RFC3339, *putCommandHoldUntil)
			if err != nil {
				panic(err)
			}
			holdUntil = t
		case *putCommandHoldFor != time.Duration(0):
			holdUntil = holdUntil.Add(*putCommandHoldFor)
		}

		a, err := c.Put(*putCommandQueue, *putCommandID, *putCommandContent, *putCommandPriority, holdUntil, *putCommandTTR)
		if err != nil {
			panic(err)
		}
		fmt.Println(a)
	case reserveCommand.FullCommand():
		id, content, err := c.Reserve(*reserveCommandQueue)
		if err != nil {
			if err == jobserver.ErrNoJobs {
				fmt.Println("no jobs")
				return
			}
			panic(err)
		}
		fmt.Println(id)
		fmt.Println(content)
	case deleteCommand.FullCommand():
		existed, err := c.Delete(*deleteCommandID)
		if err != nil {
			panic(err)
		}
		fmt.Printf("existed: %v\n", existed)
	}
}
