
USE YOUR_KEYSPACE_NAME_HERE

CREATE TABLE IF NOT EXISTS astra_usage_stats (
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
	
	PRIMARY KEY ((time_bucket), client_id, table_ref));
	
	
CREATE TABLE IF NOT EXISTS astra_usage_histograms (
	time_bucket timestamp,
	client_id uuid,
	table_ref text,
	unit_type text,
	units float,
	unit_count bigint,

	PRIMARY KEY ((time_bucket), client_id, table_ref, unit_type, units));