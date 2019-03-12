package info.matsumana.tsujun.model.ksql

data class KsqlRequest(val ksql: String, val streamsProperties: Map<String, String> = HashMap<String, String>())
