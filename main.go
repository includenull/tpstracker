package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"os/signal"
	"tpstracker/utils"

	"github.com/DataDog/datadog-go/v5/statsd"
	eos "github.com/eoscanada/eos-go"
	"github.com/eoscanada/eos-go/ship"
	shipclient "github.com/eosswedenorg-go/antelope-ship-client"
	"github.com/golang-migrate/migrate/v4"
	pgxm "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
)

func migrateDatabase(databaseUrl string) error {
	db, err := sql.Open("pgx", databaseUrl)
	if err != nil {
		log.Fatalln("Migrate Open error: ", err)
		return err
	}
	driver, err := pgxm.WithInstance(db, &pgxm.Config{})
	if err != nil {
		log.Fatalln("Migrate Instance error: ", err)
		return err
	}
	m, err := migrate.NewWithDatabaseInstance(
		"file://./migrations",
		"pgx", driver)
	if err != nil {
		log.Fatalln("Migrate migrate error: ", err)
		return err
	}
	if err := m.Up(); err != nil {
		if err != migrate.ErrNoChange {
			log.Fatalln("Migrate up error: ", err)
		}
	}
	return nil
}

func (processor *Processor) InitHandler(abi *eos.ABI) {
	processor.Abi = abi
	log.Println("Server abi: ", abi.Version)
}

func (processor *Processor) ProcessBlock(block *ship.GetBlocksResultV0) {
	// TODO: Implement fork handling

	processor.StatsD.Gauge("current_block", float64(block.ThisBlock.BlockNum), nil, 1)
	processor.StatsD.Gauge("head_block", float64(block.Head.BlockNum), nil, 1)

	if block.ThisBlock.BlockNum%1000 == 0 {
		// fmt.Print("\033[H\033[2J")
		fmt.Printf("Current: %d, Head: %d\n", block.ThisBlock.BlockNum, block.Head.BlockNum)

		// print time behind to console
		remainingTime := processor.Timer.CalculateTimeBehind(block.ThisBlock.BlockNum, block.Head.BlockNum)
		fmt.Printf("Blocks behind: %d (%s)\n", block.Head.BlockNum-block.ThisBlock.BlockNum, remainingTime)
	}

	// calculate blocks per second every 1000 blocks
	if block.ThisBlock.BlockNum%1000 == 0 {
		processor.Timer.EndTimer(block.ThisBlock.BlockNum)
		bps := processor.Timer.CalculateBPS()
		processor.StatsD.Gauge("blocks_per_second", float64(bps), nil, 1)
		fmt.Printf("Blocks per second: %d\n", bps)
		processor.Timer.StartTimer(block.ThisBlock.BlockNum)

		if bps > 0 {
			// print remaining time to console
			remainingTime := processor.Timer.CalculateRemainingTime(block.ThisBlock.BlockNum, block.Head.BlockNum, bps)
			fmt.Printf("Estimated time to sync: %s\n", remainingTime)
		}
	}

	var txCount int = 0
	var axCount int = 0
	var trCount int = 0

	if block.Traces != nil && len(block.Traces.Elem) > 0 {
		// txCount = len(block.Traces.Elem)
		for _, trace := range block.Traces.AsTransactionTracesV0() {
			if trace.Status == eos.TransactionStatusExecuted {
				txCount++

				trCount += len(trace.ActionTraces)
				for _, actionTraceVar := range trace.ActionTraces {
					var act_trace *ship.ActionTraceV1

					if trace_v0, ok := actionTraceVar.Impl.(*ship.ActionTraceV0); ok {
						// convert to v1
						act_trace = &ship.ActionTraceV1{
							ActionOrdinal:        trace_v0.ActionOrdinal,
							CreatorActionOrdinal: trace_v0.CreatorActionOrdinal,
							Receipt:              trace_v0.Receipt,
							Receiver:             trace_v0.Receiver,
							Act:                  trace_v0.Act,
							ContextFree:          trace_v0.ContextFree,
							Elapsed:              trace_v0.Elapsed,
							Console:              trace_v0.Console,
							AccountRamDeltas:     trace_v0.AccountRamDeltas,
							Except:               trace_v0.Except,
							ErrorCode:            trace_v0.ErrorCode,
							ReturnValue:          []byte{},
						}
					} else {
						act_trace = actionTraceVar.Impl.(*ship.ActionTraceV1)
					}
					if act_trace.Act.Account.String() != "eosio" && act_trace.Act.Name.String() != "onblock" {
						// log.Println(act_trace.Act.Account.String() + "::" + act_trace.Act.Name.String())

						if act_trace.CreatorActionOrdinal == 0 {
							axCount++
						}
					}
				}
			}
		}

		if txCount > 0 {
			txCount = txCount - 1
		}

		// for _, trace := range block.Traces.AsTransactionTracesV0() {
		// 	// Actions
		// 	for _, actionTraceVar := range trace.ActionTraces {
		// 		var act_trace *ship.ActionTraceV1

		// 		if trace_v0, ok := actionTraceVar.Impl.(*ship.ActionTraceV0); ok {
		// 			// convert to v1
		// 			act_trace = &ship.ActionTraceV1{
		// 				ActionOrdinal:        trace_v0.ActionOrdinal,
		// 				CreatorActionOrdinal: trace_v0.CreatorActionOrdinal,
		// 				Receipt:              trace_v0.Receipt,
		// 				Receiver:             trace_v0.Receiver,
		// 				Act:                  trace_v0.Act,
		// 				ContextFree:          trace_v0.ContextFree,
		// 				Elapsed:              trace_v0.Elapsed,
		// 				Console:              trace_v0.Console,
		// 				AccountRamDeltas:     trace_v0.AccountRamDeltas,
		// 				Except:               trace_v0.Except,
		// 				ErrorCode:            trace_v0.ErrorCode,
		// 				ReturnValue:          []byte{},
		// 			}
		// 		} else {
		// 			act_trace = actionTraceVar.Impl.(*ship.ActionTraceV1)
		// 		}

		// 		log.Println(act_trace.Act.Account.String() + "::" + act_trace.Act.Name.String())
		// 	}
		// }
	}

	if txCount < 0 {
		txCount = 0
	}

	if axCount < 0 {
		axCount = 0
	}

	// log.Println(fmt.Sprint(block.ThisBlock.BlockNum) + ": " + fmt.Sprint(txCount) + " transactions")

	var parentHash string = ""
	if block.PrevBlock != nil {
		parentHash = block.PrevBlock.BlockID.String()
	}

	if block.ThisBlock.BlockNum > (block.Head.BlockNum-10000) && processor.Rows == nil {
		_, err := processor.Pool.Exec(
			context.Background(),
			"INSERT INTO blocks (id, block_hash, parent_hash, block_time, tx_count, tx_plus_prev_count, ax_count, ax_plus_prev_count) VALUES ($1, $2, $3, $4, $5, GREATEST((SELECT tx_count FROM blocks WHERE id = $1 - 1 LIMIT 1), 0) + $5, $6, GREATEST((SELECT ax_count FROM blocks WHERE id = $1 - 1 LIMIT 1), 0) + $6)",
			block.ThisBlock.BlockNum,
			block.ThisBlock.BlockID.String(),
			parentHash,
			block.Block.Timestamp.Time.UnixMilli(),
			txCount,
			axCount,
		)

		if err != nil {
			log.Fatalf("unable to execute query: %v\n", err)
		}
	} else {
		// Catch up mode
		processor.Rows = append(processor.Rows, []interface{}{block.ThisBlock.BlockNum, block.ThisBlock.BlockID.String(), parentHash, block.Block.Timestamp.Time.UnixMilli(), txCount, axCount})

		if block.ThisBlock.BlockNum%1000 == 0 {
			go processCounts(processor)
		}

		if block.ThisBlock.BlockNum%10000 == 0 {
			if len(processor.Rows) > 0 {
				_, err := processor.Pool.CopyFrom(
					context.Background(),
					pgx.Identifier{"blocks"},
					[]string{"id", "block_hash", "parent_hash", "block_time", "tx_count", "ax_count"},
					pgx.CopyFromRows(processor.Rows),
				)

				if err != nil {
					log.Fatalf("unable to execute query: %v\n", err)
				}
			}

			processor.Rows = nil
		}
	}
}

// Ensure this function can only be run once at a time
func processCounts(processor *Processor) {
	if processor.CRun {
		return
	}
	processor.CRun = true
	_, err := processor.Pool.Exec(
		context.Background(),
		// "UPDATE blocks a SET tx_plus_prev_count = (SELECT tx_count FROM blocks b WHERE b.id = a.id - 1 LIMIT 1) + a.tx_count, ax_plus_prev_count = (SELECT ax_count FROM blocks b WHERE b.id = a.id - 1 LIMIT 1) + a.ax_count WHERE a.id IN (SELECT id FROM blocks WHERE tx_plus_prev_count IS NULL OR ax_plus_prev_count IS NULL ORDER BY id ASC LIMIT)",
		"UPDATE blocks a SET tx_plus_prev_count = (SELECT tx_count FROM blocks b WHERE b.id = a.id - 1 LIMIT 1) + a.tx_count, ax_plus_prev_count = (SELECT ax_count FROM blocks b WHERE b.id = a.id - 1 LIMIT 1) + a.ax_count WHERE a.tx_plus_prev_count IS NULL OR a.ax_plus_prev_count IS NULL ORDER BY id ASC LIMIT",
	)
	processor.CRun = false
	if err != nil {
		log.Fatalf("unable to execute query: %v\n", err)
	}
}

func processStatus(status *ship.GetStatusResultV0) {
	log.Println("-- Status START --")
	log.Println("Head", status.Head.BlockNum, status.Head.BlockID)
	log.Println("ChainStateBeginBlock", status.ChainStateBeginBlock, "ChainStateEndBlock", status.ChainStateEndBlock)
	log.Println("-- Status END --")
}

func main() {
	var startBlock uint32 = 1

	// Load environment variables
	err := godotenv.Load(".env")
	if err != nil {
		log.Fatalln("Error loading .env file", err)
	}

	// Postgres connection
	var pgHost string = os.Getenv("PG_HOST")
	var pgPort string = os.Getenv("PG_PORT")
	var pgUser string = os.Getenv("PG_USER")
	var pgPassword string = os.Getenv("PG_PASSWORD")
	var pgDatabase string = os.Getenv("PG_DATABASE")
	var databaseUrl string = "postgres://" + pgUser + ":" + pgPassword + "@" + pgHost + ":" + pgPort + "/" + pgDatabase

	pool, err := pgxpool.New(context.Background(), databaseUrl)
	if err != nil {
		log.Fatalln("Unable to connect to database", err)
	}
	defer pool.Close()

	// Get starting block from database
	var newStartBlock uint32
	row := pool.QueryRow(context.Background(), "SELECT id FROM blocks ORDER BY id DESC LIMIT 1")
	err = row.Scan(&newStartBlock)
	if err != nil {
		if err != pgx.ErrNoRows {
			pool.Exec(context.Background(), "DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
			migrateDatabase(databaseUrl)
		}
	}

	if newStartBlock > 0 {
		startBlock = newStartBlock + 1
	}

	// StatsD connection
	var statsdHost string = os.Getenv("REDIS_HOST")
	var statsdPort string = os.Getenv("REDIS_PORT")
	var statsdServer string = statsdHost + ":" + statsdPort
	statsd, err := statsd.New(statsdServer)
	if err != nil {
		log.Fatalln("StatsD:", err)
	}

	// Antelope client
	var apiUrl string = os.Getenv("API_URL")
	var apiClient = eos.New(apiUrl)
	_, err = apiClient.GetInfo(context.Background())
	if err != nil {
		log.Fatalln("Failed to get info:", err)
	}

	var rows [][]interface{}

	// Processor
	var processor = NewProcessor(pool, statsd, utils.NewTimeManager(), rows)

	// SHIP client
	var shipUrl string = os.Getenv("SHIP_URL")

	log.Printf("Connecting to ship starting at block: %d\n", startBlock)

	stream := shipclient.NewStream(shipclient.WithStartBlock(startBlock)) // shipclient.WithEndBlock(startBlock+2)
	stream.InitHandler = processor.InitHandler
	stream.BlockHandler = processor.ProcessBlock
	stream.StatusHandler = processStatus
	stream.TraceHandler = func([]*ship.TransactionTraceV0) {}
	stream.TableDeltaHandler = func([]*ship.TableDeltaV0) {}

	// Connect to SHIP client
	err = stream.Connect(shipUrl)
	if err != nil {
		log.Fatalln(err)
	}

	// Request streaming of blocks from ship
	err = stream.SendBlocksRequest()
	if err != nil {
		log.Fatalln(err)
	}

	err = stream.SendStatusRequest()
	if err != nil {
		log.Fatalln(err)
	}

	// Spawn message read loop in another thread.
	go func() {
		// Create interrupt channels.
		interrupt := make(chan os.Signal, 1)

		// Register interrupt channel to receive interrupt messages
		signal.Notify(interrupt, os.Interrupt)

		// Enter event loop in main thread
		//lint:ignore S1000 This works as expected
		for {
			select {
			case <-interrupt:
				log.Println("Interrupt, closing")

				// Cleanly close the connection by sending a close message and then
				// waiting (with timeout) for the server to close the connection.
				err := stream.Shutdown()
				if err != nil {
					log.Println("Failed to close stream: ", err)
				}
				return
			}
		}
	}()

	err = stream.Run()
	log.Println(err)
}
