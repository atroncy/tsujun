package info.matsumana.tsujun.model.ksql

data class KsqlResponseStreams(val statementText: String, val streams: Array<KsqlResponseStreamsInner>)

data class KsqlResponseStreamsInner(val name: String, val topic: String, val format: String)
