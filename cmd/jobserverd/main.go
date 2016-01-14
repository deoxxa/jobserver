package main

import (
	"bytes"
	"database/sql"
	"fmt"
	"net"
	"os"
	"time"

	"fknsrs.biz/p/jobserver/internal/protocol"
	"github.com/Sirupsen/logrus"
	_ "github.com/mattn/go-sqlite3"
	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	createTableQuery = `create table if not exists "jobs" ("id" text primary key, "queue" text not null, "priority" float not null, "hold_until" integer not null, "ttr" integer, "content" text not null)`
	fetchJobQuery    = `select "queue", "priority", "hold_until", "ttr", "content" from "jobs" where "id" = ?`
	putJobQuery      = `insert into "jobs" ("id", "queue", "priority", "hold_until", "ttr", "content") values (?, ?, ?, ?, ?, ?)`
	getJobQuery      = `select "id", "content" from "jobs" where "queue" = ? and "hold_until" < ? order by "priority" desc limit 1`
	reserveJobQuery  = `update "jobs" set "hold_until" = ? + "ttr" where "id" = ?`
	updateJobQuery   = `update "jobs" set "priority" = ?, "hold_until" = ?, "ttr" = ? where "id" = ?`
	deleteJobQuery   = `delete from "jobs" where "id" = ?`
	listQueuesQuery  = `select distinct "queue" from "jobs"`
	queueStatsQuery  = `select "queue", count(1) as "count" from "jobs" group by "queue"`
)

func withTx(db *sql.DB, f func(tx *sql.Tx) error) error {
	tx, terr := db.Begin()
	if terr != nil {
		return terr
	}

	done := false
	defer func() {
		if !done {
			tx.Rollback()
		}
	}()

	if err := f(tx); err != nil {
		rerr := tx.Rollback()
		done = true
		return rerr
	}

	cerr := tx.Commit()
	done = true
	return cerr
}

func maybePanic(err error) {
	if err != nil {
		panic(err)
	}
}

var (
	app      = kingpin.New("jobserverd", "Job server using SQLite as a backend.")
	dbPath   = app.Flag("db_path", "Path to SQLite database.").Default("jobs.db").Envar("DB_PATH").String()
	addr     = app.Flag("addr", "Address to listen on.").Default(":2097").Envar("ADDR").String()
	logLevel = app.Flag("log_level", "Log level").Default("info").Enum("debug", "info", "warn", "error")
)

func main() {
	kingpin.MustParse(app.Parse(os.Args[1:]))

	ll, lerr := logrus.ParseLevel(*logLevel)
	if lerr != nil {
		panic(lerr)
	}
	logrus.SetLevel(ll)

	logrus.WithFields(logrus.Fields{
		"db_path":   *dbPath,
		"addr":      *addr,
		"log_level": *logLevel,
	}).Info("starting up")

	logrus.WithField("db_path", *dbPath).Debug("opening database")
	db, dberr := sql.Open("sqlite3", *dbPath)
	if dberr != nil {
		panic(dberr)
	}
	defer db.Close()
	logrus.Debug("opened database")

	logrus.Debug("ensuring tables exist")
	if _, err := db.Exec(createTableQuery); err != nil {
		panic(err)
	}
	logrus.Debug("tables created")

	logrus.Debug("opening listening socket")
	s, serr := net.ListenPacket("udp4", *addr)
	if serr != nil {
		panic(serr)
	}
	logrus.Info("listening")

	snum := 1

	for {
		logrus.Debug("waiting for incoming message")

		b := make([]byte, protocol.MessageSize)
		n, r, err := s.ReadFrom(b)
		if err != nil {
			panic(err)
		}

		mnum := snum
		snum++

		l := logrus.WithField("mnum", mnum)

		l = l.WithField("remote", r.String())

		l.WithField("size", n).Debug("got message")

		func() {
			defer func() {
				if e := recover(); e != nil {
					if err, ok := e.(error); ok {
						l.WithField("error", err.Error()).Error("error processing message")
					} else {
						l.WithField("error", e).Error("error processing message")
					}
				} else {
					l.Debug("processed message successfully")
				}
			}()

			m, err := protocol.Parse(bytes.TrimSpace(b[0:n]))
			if err != nil {
				panic(err)
			}

			l = l.WithField("message_type", fmt.Sprintf("%T", m))
			l = l.WithField("message_key", m.GetKey())

			l.Debug("processing message")

			switch m := m.(type) {
			case *protocol.PingMessage:
				if _, err := s.WriteTo(m.Serialise(), r); err != nil {
					panic(err)
				}
			case *protocol.PutMessage:
				maybePanic(withTx(db, func(tx *sql.Tx) error {
					if m.HoldUntil == 0 {
						m.HoldUntil = time.Now().Unix()
					}
					if m.TTR == 0 {
						m.TTR = uint64(time.Hour / time.Second)
					}

					var queue, content string
					var priority float64
					var holdUntil int64
					var ttr uint64
					var found bool

					if err := tx.QueryRow(fetchJobQuery, m.ID).Scan(&queue, &priority, &holdUntil, &ttr, &content); err != nil && err != sql.ErrNoRows {
						return err
					} else if err == nil {
						found = true
					}

					if found == false {
						if _, err := tx.Exec(putJobQuery, m.ID, m.Queue, m.Priority, m.HoldUntil, m.TTR, m.Content); err != nil {
							return err
						}

						d := protocol.Serialise(&protocol.QueuedMessage{Key: m.Key, Action: "created"})
						if _, err := s.WriteTo(d, r); err != nil {
							return err
						}
					} else {
						if m.HoldUntil > holdUntil {
							m.HoldUntil = holdUntil
						}

						if _, err := tx.Exec(updateJobQuery, m.Priority, m.HoldUntil, m.TTR, m.ID); err != nil {
							return err
						}

						d := protocol.Serialise(&protocol.QueuedMessage{Key: m.Key, Action: "updated"})
						if _, err := s.WriteTo(d, r); err != nil {
							return err
						}
					}

					return nil
				}))
			case *protocol.ReserveMessage:
				maybePanic(withTx(db, func(tx *sql.Tx) error {
					var id, content string
					if err := tx.QueryRow(getJobQuery, m.Queue, time.Now().Unix()).Scan(&id, &content); err != nil {
						if err == sql.ErrNoRows {
							d := protocol.Serialise(&protocol.ReserveFailedMessage{Key: m.Key, Reason: "empty"})
							if _, werr := s.WriteTo(d, r); werr != nil {
								return werr
							}

							return nil
						}

						return err
					}

					if _, err := tx.Exec(reserveJobQuery, time.Now().Unix(), id); err != nil {
						return err
					}

					d := protocol.Serialise(&protocol.ReservedMessage{Key: m.Key, ID: id, Content: content})
					if _, err := s.WriteTo(d, r); err != nil {
						return err
					}

					return nil
				}))
			case *protocol.DeleteMessage:
				maybePanic(withTx(db, func(tx *sql.Tx) error {
					qr, err := db.Exec(deleteJobQuery, m.ID)
					if err != nil {
						return err
					}

					n, err := qr.RowsAffected()
					if err != nil {
						return err
					}

					d := protocol.Serialise(&protocol.DeletedMessage{Key: m.Key, Existed: n != 0})
					if _, err := s.WriteTo(d, r); err != nil {
						return err
					}

					return nil
				}))
			}
		}()
	}
}
