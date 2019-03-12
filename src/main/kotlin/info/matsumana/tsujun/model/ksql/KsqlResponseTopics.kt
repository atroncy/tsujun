package info.matsumana.tsujun.model.ksql

data class KsqlResponseTopics(val statementText: String, val topics: Array<KsqlResponseTopicsInner>)

data class KsqlResponseTopicsInner(val name: String, val registered: Boolean, val replicaInfo: Array<Int>, val consumerCount: Long, val consumerGroupCount: Long)
