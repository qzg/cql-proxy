package proxy

import (
	"context"
	"encoding/hex"
	"fmt"
	"math"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/google/uuid"
	"github.com/procyon-projects/chrono"
	"go.uber.org/zap"
)

type metricRow struct {
	select_count uint64
	select_size  uint64
	select_rrus  uint64
	insert_count uint64
	insert_size  uint64
	insert_wrus  uint64
	update_count uint64
	update_size  uint64
	update_wrus  uint64
	delete_count uint64
	delete_size  uint64
	delete_wrus  uint64
	lwt_count    uint64
	lwt_size     uint64
	lwt_rrus     uint64
	lwt_wrus     uint64
	index_wrus   uint64
	writes_size  uint64
	reads_size   uint64
	wrus         uint64
	rrus         uint64
}

func NewMetricRow() *metricRow {
	mr := &metricRow{}

	mr.select_count = 0
	mr.select_size = 0
	mr.select_rrus = 0
	mr.insert_count = 0
	mr.insert_size = 0
	mr.insert_wrus = 0
	mr.update_count = 0
	mr.update_size = 0
	mr.update_wrus = 0
	mr.delete_count = 0
	mr.delete_size = 0
	mr.delete_wrus = 0
	mr.lwt_count = 0
	mr.lwt_size = 0
	mr.lwt_rrus = 0
	mr.lwt_wrus = 0
	mr.index_wrus = 0
	mr.writes_size = 0
	mr.reads_size = 0
	mr.wrus = 0
	mr.rrus = 0

	return mr
}

// sm.stats[timestamp]["keyspace.table"]["select_count"] = 12345
type statsrecord map[time.Time]map[string]*metricRow

// sm.counts[timestamp]["keyspace.table"]["reads|writes|all"][5|9|234...] = 1232455
type counts map[time.Time]map[string]map[string]map[uint64]uint64

// sm.latencies[timestamp]["keyspace.table"][reads|writes] = hdrhistogram.Histogram
type latencies map[time.Time]map[string]map[string]*hdrhistogram.Histogram

type statsManager struct {
	id          string
	config      *runConfig
	Stats       statsrecord
	Counts      counts
	Latencies   latencies
	ctx         context.Context
	MessageFeed chan *RequestResponse
	// MinMaxChan  chan TKMMsg
}

var (
	once sync.Once
	sm   *statsManager
)

var systemTables = []string{"local", "peers", "peers_v2", "schema_keyspaces", "schema_columnfamilies", "schema_columns", "schema_usertypes"}

func SingletonStatsManager(ctx context.Context, config *runConfig, proxy *Proxy) (*statsManager, error) {
	var err error
	if config.TrackUsage {
		once.Do(func() {
			timebucket := time.Now().Truncate(time.Hour)
			sm = &statsManager{
				id:          uuid.NewString(),
				config:      config,
				Stats:       map[time.Time]map[string]*metricRow{},
				Counts:      map[time.Time]map[string]map[string]map[uint64]uint64{},
				Latencies:   map[time.Time]map[string]map[string]*hdrhistogram.Histogram{},
				ctx:         ctx,
				MessageFeed: make(chan *RequestResponse, 1024),
				// MinMaxChan:  make(chan TKMMsg),
			}
			sm.Stats[timebucket] = map[string]*metricRow{}
			sm.Counts[timebucket] = map[string]map[string]map[uint64]uint64{}

			maybeCreateMetricsTable(ctx, config, proxy)
			maybeCreateHistogramsTable(ctx, config, proxy)
			// go trackMinMax(ctx)
			go periodicallyFlush(ctx, config, proxy)
			go listen()
		})
		return sm, err
	} else {
		return sm, nil
	}
}

func listen() {
	fmt.Println("Listening for metrics")
	for {
		select {
		case req := <-sm.MessageFeed:
			handleQuery(req)
		case <-sm.ctx.Done():
			return
		}
	}
}

// track minimum and maximum throughput per second
// func trackMinMax(ctx context.Context) {
// 	var minMaxCounters map[string]map[string]int64
// 	ticker := time.NewTicker(time.Second)
// 	defer ticker.Stop()
// 	for {
// 		select {
// 		case t := <-ticker.C:
// 			// reset the counter every second
// 			fmt.Println("Current time: ", t)

// 			minPerSecondCounter = -1
// 			maxPerSecondCounter = 0
// 		case val := <-sm.MinMaxChan:
// 			mr := getMetricRow(val.timebucket, val.keyspaceTableName)
// 			if val.ns > maxPerSecondCounter {
// 				maxPerSecondCounter = val.ns
// 			}
// 			if minPerSecondCounter == -1 || val.ns < minPerSecondCounter {
// 				minPerSecondCounter = val.ns
// 			}
// 		case <-sm.ctx.Done():
// 			return
// 		}
// 	}
// }

// to do this "correctly" eventually I'll need to be able to:
//   - size the parameters into a write or the results of the write...?
//   - add the size of the writetimes for each non-partition-key field
//   - don't charge for system tables
//
// for latencies, we don't need a histogram; we need specific percentiles:
// 50th, 75th, 90th, 95th, 99th, 99.99, and max, per table, per window
// these are per second measurements.  e.g. what is the average latency across all operations in the reported time window
// Somewhat unrelated, I also need to report on throughput.  Operations per second, averaged over some period of time.  Maybe percentiles make sense
// here too?  That is, during the one hour reporting window the average throughput per second was X; the peak per second was Y
func handleQuery(reqres *RequestResponse) {
	var keyspaceTableName, query string

	reqres.req.client.proxy.logger.Debug("Time Tracker: ", zap.Duration("Response Time", reqres.req.rt))

	switch msg := reqres.req.msg.(type) {
	case *partialExecute:
		if prepare, ok := reqres.req.client.proxy.preparedCache.Load(hex.EncodeToString(msg.queryId)); !ok {
			reqres.req.client.proxy.logger.Error("partialExecute ERROR")
		} else {
			query = string(prepare.PreparedFrame.Body[4:])
		}
	case *partialQuery:
		query = msg.query
	case *partialBatch:
		reqres.req.client.proxy.logger.Debug("partialBatch currently being implemented")
		// recursively call this function with an altered reqres containing each batch member
		hold_queryOrIds := msg.queryOrIds
		for _, batch := range hold_queryOrIds {
			switch batch := batch.(type) {
			case []uint8:
				// prepared statement
				reqres.req.msg = &partialExecute{[]byte(batch)}
			case string:
				// string statement
				reqres.req.msg = &partialQuery{batch}
			}
			handleQuery(reqres)
		}
	case *message.Prepare:
		reqres.req.client.proxy.logger.Info("Preparing Statement:")
		fmt.Println(msg)
	default:
		reqres.req.client.proxy.logger.Error("Unknown message type")
		fmt.Println(msg)
		fmt.Printf("Message of type %T", msg)
	}

	if len(query) > 0 {
		// INSERT INTO keyspace.table (fields...) VALUES (values...)
		// UPDATE keyspace.table [USING] SET field=value, ...
		// SELECT (*|fields|DISTINCT partition) FROM keyspace.table WHERE predicate=value AND ...
		// DELETE [(fields...)] FROM keyspace.table [USING] WHERE predicate=value AND ...
		find_tables := regexp.MustCompile(`(?i)\s(INTO|UPDATE|FROM)\s\s*\\*\"*([a-zA-Z0-9._]*)\\*\"*\s*`)
		sys_re := regexp.MustCompile(`^system`)
		// query = strings.ReplaceAll(query, "\n", " ")
		tables := find_tables.FindStringSubmatch(query)

		if len(tables) == 0 {
			reqres.req.client.proxy.logger.Debug("Encountered an unhandled query/statement", zap.String("query:", query))
			return // TODO: do something more interesting here, like panic
		} else if !strings.Contains(tables[2], ".") {
			// if there's no dot in the table name, prepend the keyspace and a dot to the table name
			keyspace := strings.ReplaceAll(reqres.req.keyspace, "\"", "")
			if keyspace == "" {
				keyspace = strings.ReplaceAll(reqres.req.client.keyspace, "\"", "")
			}
			keyspaceTableName = keyspace + "." + tables[2]
		} else {
			keyspaceTableName = tables[2]
		}

		// if system table tracking is off and the working keyspace name starts with "system", skip
		if !sm.config.UsageTrackSystem && sys_re.MatchString(keyspaceTableName) {
			return
		}

		// round down to the nearest hour
		timebucket := time.Now().Truncate(time.Hour)
		mr := getMetricRow(timebucket, keyspaceTableName)

		if isSystemTable(query) {
			return
		} else if match, _ := regexp.MatchString(`(?i)^\s*SELECT`, query); match {
			handleSELECT(reqres, keyspaceTableName, timebucket, mr)
		} else if match, _ := regexp.MatchString(`(?i)(IF NOT EXISTS|IF EXISTS)`, query); match {
			// this case still needs some way to handle UPDATE ... IF ...
			handleLWT(reqres, keyspaceTableName, timebucket, mr)
		} else if match, _ := regexp.MatchString(`(?i)^\s*INSERT`, query); match {
			handleINSERT(reqres, keyspaceTableName, timebucket, mr)
		} else if match, _ := regexp.MatchString(`(?i)^\s*UPDATE`, query); match {
			handleUPDATE(reqres, keyspaceTableName, timebucket, mr)
		} else if match, _ := regexp.MatchString(`(?i)^\s*DELETE`, query); match {
			handleDELETE(reqres, keyspaceTableName, timebucket, mr)
		}
	}
}

func getMetricRow(timebucket time.Time, keyspaceTableName string) *metricRow {
	mr, ok := sm.Stats[timebucket][keyspaceTableName]
	if !ok {
		if _, ok := sm.Stats[timebucket]; !ok {
			sm.Stats[timebucket] = map[string]*metricRow{}
		}
		mr = NewMetricRow()
		sm.Stats[timebucket][keyspaceTableName] = mr
	}
	return mr
}

func handleSELECT(reqres *RequestResponse, keyspaceTableName string, timebucket time.Time, mr *metricRow) {
	req_size := reqres.req.raw.Header.BodyLength
	res_size := reqres.res.Header.BodyLength
	reqres_size := uint64(req_size) + uint64(res_size)
	rrus := uint64(math.Ceil(float64(reqres_size) / float64(sm.config.UsageRruBytes)))

	mr.select_count++
	mr.select_size = mr.select_size + reqres_size
	mr.select_rrus = mr.select_rrus + rrus

	mr.reads_size = mr.reads_size + reqres_size
	mr.rrus = mr.rrus + rrus

	incrementCounts(timebucket, keyspaceTableName, "reads", rrus)
	recordLatency(timebucket, keyspaceTableName, "read_lat", reqres.req.rt)
}

// LWT is counted according to a regular write but with +1 RRU
func handleLWT(reqres *RequestResponse, keyspaceTableName string, timebucket time.Time, mr *metricRow) {
	req_size := reqres.req.raw.Header.BodyLength
	res_size := reqres.res.Header.BodyLength
	reqres_size := uint64(req_size) + uint64(res_size)
	wrus := uint64(math.Ceil(float64(reqres_size) / float64(sm.config.UsageWruBytes)))

	mr.lwt_count++
	mr.lwt_size = mr.lwt_size + reqres_size
	mr.lwt_wrus = mr.lwt_wrus + wrus
	mr.lwt_rrus = mr.lwt_rrus + 1

	mr.writes_size = mr.writes_size + reqres_size
	mr.wrus = mr.wrus + wrus
	mr.rrus = mr.rrus + 1

	incrementCounts(timebucket, keyspaceTableName, "writes", wrus)
	incrementCounts(timebucket, keyspaceTableName, "reads", 1)
	recordLatency(timebucket, keyspaceTableName, "write_lat", reqres.req.rt)
}

func handleINSERT(reqres *RequestResponse, keyspaceTableName string, timebucket time.Time, mr *metricRow) {
	req_size := reqres.req.raw.Header.BodyLength
	res_size := reqres.res.Header.BodyLength
	reqres_size := uint64(req_size) + uint64(res_size)
	wrus := uint64(math.Ceil(float64(reqres_size) / float64(sm.config.UsageWruBytes)))

	mr.insert_count++
	mr.insert_size = mr.insert_size + reqres_size
	mr.insert_wrus = mr.insert_wrus + wrus

	mr.writes_size = mr.writes_size + reqres_size
	mr.wrus = mr.wrus + wrus

	incrementCounts(timebucket, keyspaceTableName, "writes", wrus)
	recordLatency(timebucket, keyspaceTableName, "write_lat", reqres.req.rt)
}

func handleUPDATE(reqres *RequestResponse, keyspaceTableName string, timebucket time.Time, mr *metricRow) {
	req_size := reqres.req.raw.Header.BodyLength
	res_size := reqres.res.Header.BodyLength
	reqres_size := uint64(req_size) + uint64(res_size)
	wrus := uint64(math.Ceil(float64(reqres_size) / float64(sm.config.UsageWruBytes)))

	mr.update_count++
	mr.update_size = mr.update_size + reqres_size
	mr.update_wrus = mr.update_wrus + wrus

	mr.writes_size = mr.writes_size + reqres_size
	mr.wrus = mr.wrus + wrus

	incrementCounts(timebucket, keyspaceTableName, "writes", wrus)
	recordLatency(timebucket, keyspaceTableName, "write_lat", reqres.req.rt)
}

func handleDELETE(reqres *RequestResponse, keyspaceTableName string, timebucket time.Time, mr *metricRow) {
	req_size := reqres.req.raw.Header.BodyLength
	res_size := reqres.res.Header.BodyLength
	reqres_size := uint64(req_size) + uint64(res_size)
	wrus := uint64(math.Ceil(float64(reqres_size) / float64(sm.config.UsageWruBytes)))

	mr.delete_count++
	mr.delete_size = mr.delete_size + reqres_size
	mr.delete_wrus = mr.delete_wrus + wrus

	mr.writes_size = mr.writes_size + reqres_size
	mr.wrus = mr.wrus + wrus

	incrementCounts(timebucket, keyspaceTableName, "writes", wrus)
	recordLatency(timebucket, keyspaceTableName, "write_lat", reqres.req.rt)
}

func isSystemTable(name string) bool {
	for _, table := range systemTables {
		if name == table {
			return true
		}
	}
	return false
}

func recordLatency(timebucket time.Time, keyspaceTableName string, unitType string, elapsed time.Duration) {
	_, ok := sm.Latencies[timebucket][keyspaceTableName][unitType]
	if !ok {
		if _, ok := sm.Latencies[timebucket]; !ok {
			sm.Latencies[timebucket] = map[string]map[string]*hdrhistogram.Histogram{}
		}
		if _, ok := sm.Latencies[timebucket][keyspaceTableName]; !ok {
			sm.Latencies[timebucket][keyspaceTableName] = map[string]*hdrhistogram.Histogram{}
		}
		if _, ok := sm.Latencies[timebucket][keyspaceTableName][unitType]; !ok {
			sm.Latencies[timebucket][keyspaceTableName][unitType] = hdrhistogram.New(1, 30000000000, 3)
		}
	}
	sm.Latencies[timebucket][keyspaceTableName][unitType].RecordValue(elapsed.Nanoseconds())
}

func incrementCounts(timebucket time.Time, keyspaceTableName string, unitType string, units uint64) {
	// sm.histograms[timestamp]["keyspace.table"]["reads|writes|all"][5|9|234...] = 1232455
	_, ok := sm.Counts[timebucket][keyspaceTableName][unitType][units]
	if !ok {
		if _, ok := sm.Counts[timebucket]; !ok {
			sm.Counts[timebucket] = map[string]map[string]map[uint64]uint64{}
		}
		if _, ok := sm.Counts[timebucket][keyspaceTableName]; !ok {
			sm.Counts[timebucket][keyspaceTableName] = map[string]map[uint64]uint64{}
		}
		if _, ok := sm.Counts[timebucket][keyspaceTableName][unitType]; !ok {
			sm.Counts[timebucket][keyspaceTableName][unitType] = map[uint64]uint64{}
		}
		if _, ok := sm.Counts[timebucket][keyspaceTableName][unitType][units]; !ok {
			sm.Counts[timebucket][keyspaceTableName][unitType][units] = uint64(0)
		}
	}
	sm.Counts[timebucket][keyspaceTableName][unitType][units]++
}

func periodicallyFlush(ctx context.Context, config *runConfig, proxy *Proxy) {
	taskScheduler := chrono.NewDefaultTaskScheduler()

	_, err := taskScheduler.ScheduleWithFixedDelay(func(ctx context.Context) {
		proxy.logger.Debug("Flushing stats")
		flushCurrentStats(ctx, config, proxy)
		flushCurrentCounts(ctx, config, proxy)
		flushCurrentLatencies(ctx, config, proxy)
		purgeOldStats()
	}, time.Duration(config.UsageFlushSeconds)*time.Second)

	if err == nil {
		proxy.logger.Debug("Scheduled periodic flush")
	}
}

func flushCurrentStats(ctx context.Context, config *runConfig, proxy *Proxy) {
	// sm.stats[timestamp]["keyspace.table"]["select_count"] = 12345
	//timebucket := time.Now().Round(time.Hour)
	// TODO Clone stats to operate on a stable view
	statementStart := `BEGIN UNLOGGED BATCH `
	statement := ``
	statementEnd := ` APPLY BATCH;`
	for timebucket, timeEntries := range sm.Stats {
		for table_ref, mr := range timeEntries {
			fragment := fmt.Sprintf(`INSERT INTO %s.%s (
				time_bucket, client_id, table_ref, 
				select_count, select_size, select_rrus, 
				insert_count, insert_size, insert_wrus, 
				update_count, update_size, update_wrus, 
				delete_count, delete_size, delete_wrus, 
				lwt_count, lwt_size, lwt_rrus, lwt_wrus, 
				index_wrus, 
				writes_size, reads_size, wrus, rrus)
			VALUES (
				%v, %s, '%s', 
				%v, %v, %v, 
				%v, %v, %v, 
				%v, %v, %v, 
				%v, %v, %v, 
				%v, %v, %v, %v, 
				%v, 
				%v, %v, %v, %v);`,
				config.UsageKeyspace, config.UsageTable,
				timebucket.UnixMilli(), sm.id, table_ref,
				mr.select_count, mr.select_size, mr.select_rrus,
				mr.insert_count, mr.insert_size, mr.insert_wrus,
				mr.update_count, mr.update_size, mr.update_wrus,
				mr.delete_count, mr.delete_size, mr.delete_wrus,
				mr.lwt_count, mr.lwt_size, mr.lwt_rrus, mr.lwt_wrus,
				mr.index_wrus,
				mr.writes_size, mr.reads_size, mr.wrus, mr.rrus)

			fragment = strings.ReplaceAll(fragment, "\n", " ")
			fragment = strings.ReplaceAll(fragment, "\t", "")
			statement += fragment

		}
	}

	rs, err := proxy.cluster.ExecuteControlQuery(ctx, statementStart+statement+statementEnd)
	if err == nil && rs == nil {
		proxy.logger.Debug("Stats: SAVED")
	} else if err != nil {
		proxy.logger.Error("Error upserting stats!  Tried to execute:")
		proxy.logger.Error(statement)
	}

}

func flushCurrentCounts(ctx context.Context, config *runConfig, proxy *Proxy) {
	statementStart := `BEGIN UNLOGGED BATCH `
	statement := ``
	statementEnd := ` APPLY BATCH;`
	for timebucket, timeEntries := range sm.Counts {
		for table_ref, unit_types := range timeEntries {
			for unit_type, units_group := range unit_types {
				for units, unit_count := range units_group {
					fragment := fmt.Sprintf(`INSERT INTO %s.%s (
						time_bucket, client_id, table_ref, 
						unit_type, units, unit_count) 
					VALUES (
						%v, %s, '%s', 
						'%v', %v, %v);`,
						config.UsageKeyspace, config.UsageHistogramsTable,
						timebucket.UnixMilli(), sm.id, table_ref,
						unit_type, units, unit_count)
					fragment = strings.ReplaceAll(fragment, "\n", "")
					fragment = strings.ReplaceAll(fragment, "\t", "")
					statement += fragment
				}
			}
		}
	}

	rs, err := proxy.cluster.ExecuteControlQuery(ctx, statementStart+statement+statementEnd)
	if err == nil && rs == nil {
		proxy.logger.Debug("Counts: SAVED")
	} else if err != nil {
		proxy.logger.Error("Error upserting counts!  Tried to execute:")
		proxy.logger.Error(statement)
	}

}

func flushCurrentLatencies(ctx context.Context, config *runConfig, proxy *Proxy) {
	statementStart := `BEGIN UNLOGGED BATCH `
	statement := ``
	statementEnd := ` APPLY BATCH;`
	for timebucket, timeEntries := range sm.Latencies {
		for table_ref, unit_types := range timeEntries {
			for unit_type, histogram := range unit_types {
				for units, units_value := range histogram.ValueAtPercentiles([]float64{50.0, 75.0, 95.0, 99.0, 99.9, 99.99, 100.0}) {
					fragment := fmt.Sprintf(`INSERT INTO %s.%s (
						time_bucket, client_id, table_ref, 
						unit_type, units, unit_count) 
					VALUES (
						%v, %s, '%s', 
						'%v', %v, %v);`,
						config.UsageKeyspace, config.UsageHistogramsTable,
						timebucket.UnixMilli(), sm.id, table_ref,
						unit_type, units, units_value)
					fragment = strings.ReplaceAll(fragment, "\n", "")
					fragment = strings.ReplaceAll(fragment, "\t", "")
					statement += fragment
				}
			}
		}
	}

	rs, err := proxy.cluster.ExecuteControlQuery(ctx, statementStart+statement+statementEnd)
	if err == nil && rs == nil {
		proxy.logger.Debug("Latency Histograms: SAVED")
	} else if err != nil {
		proxy.logger.Error("Error upserting latency histograms!  Tried to execute:")
		proxy.logger.Error(statement)
	}

}

// purge in-memory stats older than two hours
func purgeOldStats() {
	three_hours_ago := time.Now().Add(-3 * time.Hour)
	for timebucket, _ := range sm.Stats {
		if timebucket.Before(three_hours_ago) {
			delete(sm.Stats, timebucket)
			delete(sm.Counts, timebucket)
			delete(sm.Latencies, timebucket)
		}
	}
}

// initialize the metrics table if it doesn't exist
func maybeCreateMetricsTable(ctx context.Context, config *runConfig, proxy *Proxy) {
	query := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.%s (
		time_bucket timestamp,
		client_id uuid,
		table_ref text,

		select_count bigint,
		select_size bigint,
		select_rrus bigint,

		insert_count bigint,
		insert_size bigint,
		insert_wrus bigint,

		update_count bigint,
		update_size bigint,
		update_wrus bigint,

		delete_count bigint,
		delete_size bigint,
		delete_wrus bigint,

		lwt_count bigint,
		lwt_size bigint,
		lwt_rrus bigint,
		lwt_wrus bigint,

		index_wrus bigint,

		writes_size bigint,
		reads_size bigint,
		wrus bigint,
		rrus bigint,
		
		PRIMARY KEY ((time_bucket), client_id, table_ref));`, config.UsageKeyspace, config.UsageTable)

	rs, err := proxy.cluster.ExecuteControlQuery(ctx, query)

	if err == nil && rs == nil {
		proxy.logger.Debug("Stats table created or already exists.")
	} else if err != nil {
		proxy.logger.Error("Error initializing usage stats table!")
	}
}

// initialize the metrics table if it doesn't exist
func maybeCreateHistogramsTable(ctx context.Context, config *runConfig, proxy *Proxy) {
	query := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.%s (
		time_bucket timestamp,
		client_id uuid,
		table_ref text,
		unit_type text,
		units float,
		unit_count bigint,

		PRIMARY KEY ((time_bucket), client_id, table_ref, unit_type, units));`, config.UsageKeyspace, config.UsageHistogramsTable)

	rs, err := proxy.cluster.ExecuteControlQuery(ctx, query)

	if err == nil && rs == nil {
		proxy.logger.Debug("Histograms table created or already exists.")
	} else if err != nil {
		proxy.logger.Error("Error initializing histogram stats table!")
	}
}

// /* UUID code borrowed from go-cql (https://github.com/gocql/) */
// type UUID [16]byte

// func MustRandomUUID() UUID {
// 	uuid, err := RandomUUID()
// 	if err != nil {
// 		panic(err)
// 	}
// 	return uuid
// }

// // RandomUUID generates a totally random UUID (version 4) as described in
// // RFC 4122.
// func RandomUUID() (UUID, error) {
// 	var u UUID
// 	_, err := io.ReadFull(rand.Reader, u[:])
// 	if err != nil {
// 		return u, err
// 	}
// 	u[6] &= 0x0F // clear version
// 	u[6] |= 0x40 // set version to 4 (random uuid)
// 	u[8] &= 0x3F // clear variant
// 	u[8] |= 0x80 // set to IETF variant
// 	return u, nil
// }

// reqres.req.client.proxy.logger.Info("Response Size:", zap.Int32("message size in bytes", reqres.req.raw.Header.BodyLength))
// reqres.req.client.proxy.logger.Info("handling query", zap.String("Query", query))
