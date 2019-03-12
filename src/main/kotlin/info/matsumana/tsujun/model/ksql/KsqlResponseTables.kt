package info.matsumana.tsujun.model.ksql

data class KsqlResponseTables(val statementText: String, val tables: Array<KsqlResponseTablesInner>)

data class KsqlResponseTablesInner(val name: String, val topic: String, val format: String, val isWindowed: Boolean)
