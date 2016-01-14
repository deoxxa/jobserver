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
	reserveCommandWait  = reserveCommand.Flag("wait", "Wait for a job to become available.").Bool()
	peekCommand         = app.Command("peek", "Try to peek a job from a queue.")
	peekCommandQueue    = peekCommand.Arg("queue", "Queue to try to peek a job from.").Required().String()
	deleteCommand       = app.Command("delete", "Delete a job.")
	deleteCommandQueue  = deleteCommand.Arg("queue", "Queue from which to delete a job.").Required().String()
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

		if err := c.Put(*putCommandQueue, *putCommandID, *putCommandContent, *putCommandPriority, holdUntil, *putCommandTTR); err != nil {
			panic(err)
		}
	case reserveCommand.FullCommand():
		var j *jobserver.Job
		var err error
		if *reserveCommandWait {
			j, err = c.ReserveWait(*reserveCommandQueue)
		} else {
			j, err = c.Reserve(*reserveCommandQueue)
		}

		if err != nil {
			if err == jobserver.ErrNoJobs {
				fmt.Println("no jobs")
				return
			}
			panic(err)
		}

		fmt.Printf("[%s] %#v %s %s\n", j.Queue, j.Priority, j.TTR, j.ID)
		fmt.Println(j.Content)
	case peekCommand.FullCommand():
		j, err := c.Peek(*peekCommandQueue)
		if err != nil {
			if err == jobserver.ErrNoJobs {
				fmt.Println("no jobs")
				return
			}
			panic(err)
		}

		fmt.Printf("[%s] %#v %s %s\n", j.Queue, j.Priority, j.TTR, j.ID)
		fmt.Println(j.Content)
	case deleteCommand.FullCommand():
		if err := c.Delete(*deleteCommandQueue, *deleteCommandID); err != nil {
			if err == jobserver.ErrNotFound {
				fmt.Println("not found")
				return
			}
			panic(err)
		}
	}
}
