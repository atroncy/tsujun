package info.matsumana.tsujun.model.ksql

data class KsqlResponseDescribe(val statementText: String, val sourceDescription: KsqlResponseDescribeInner)

data class KsqlResponseDescribeInner(val name: String, val fields: Array<KsqlResponseDescribeInnerFields>)

data class KsqlResponseDescribeInnerFields(val name: String, val schema: KsqlResponseDescribeInnerFieldSchema)

data class KsqlResponseDescribeInnerFieldSchema(val type: String)
