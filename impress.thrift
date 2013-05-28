namespace py impress_thrift

service ImpressCache {
	oneway void add(
		1: string site,
		2: list<string> objkeys,
		3: string data,
	),

	string get(
		1: string site,
		2: list<string> objkeys,
	),

	/**
	 * Returns the unix time that the server has been running since
	 */
	i64 aliveSince(),

	/**
	 * Gets the value of a single counter
	 */
	i64 getCounter(1: string key),

	/**
	 * Gets the counters for this service
	 */
	map<string, i64> getCounters(),

	/**
	 * Sets an option
	 */
	void setOption(1: string key, 2: string value),

	/**
	 * Gets an option
	 */
	string getOption(1: string key),

	/**
	 * Gets all options
	 */
	map<string, string> getOptions(),
}
