package main

import (
	"tpstracker/utils"

	"github.com/DataDog/datadog-go/v5/statsd"
	"github.com/eoscanada/eos-go"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Processor struct {
	Abi    *eos.ABI
	Pool   *pgxpool.Pool
	CRun   bool
	StatsD *statsd.Client
	Timer  *utils.TimeManager
	Rows   [][]interface{}
}

// Create a new Processor
func NewProcessor(pool *pgxpool.Pool, statsd *statsd.Client, timer *utils.TimeManager, rows [][]interface{}) *Processor {
	return &Processor{
		Pool:   pool,
		CRun:   false,
		StatsD: statsd,
		Timer:  timer,
		Rows:   rows,
	}
}
