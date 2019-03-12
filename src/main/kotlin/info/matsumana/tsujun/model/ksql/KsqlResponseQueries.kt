package info.matsumana.tsujun.model.ksql

data class KsqlResponseQueries(val statementText: String, val queries: Array<KsqlResponseQueriesInner>)

// TODO add sinks
data class KsqlResponseQueriesInner(val id: String, val kafkaTopic: String, val queryString: String)

