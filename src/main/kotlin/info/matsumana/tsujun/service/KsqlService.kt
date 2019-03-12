package info.matsumana.tsujun.service

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.fasterxml.jackson.module.kotlin.readValue
import info.matsumana.tsujun.config.KsqlServerConfig
import info.matsumana.tsujun.exception.KsqlException
import info.matsumana.tsujun.model.Request
import info.matsumana.tsujun.model.ResponseTable
import info.matsumana.tsujun.model.ksql.*
import info.matsumana.tsujun.model.statementsResponseTable
import org.slf4j.LoggerFactory
import org.springframework.http.HttpStatus
import org.springframework.stereotype.Service
import org.springframework.web.reactive.function.client.WebClient
import org.springframework.web.reactive.function.client.WebClientResponseException
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.io.IOException
import java.util.*
import io.netty.handler.timeout.WriteTimeoutHandler
import io.netty.handler.timeout.ReadTimeoutHandler
import org.springframework.http.client.reactive.ReactorClientHttpConnector
import reactor.netty.http.client.HttpClient


@Service
class KsqlService(private val ksqlServerConfig: KsqlServerConfig) {

    companion object {
        private val logger = LoggerFactory.getLogger(this::class.java.enclosingClass)
        private val mapper = ObjectMapper().registerModule(KotlinModule())
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        private val TYPE_COMMAND_DESCRIBE = "DESCRIBE"
        private val TYPE_COMMAND_ = "DESCRIBE"
        private val REGEX_SELECT = Regex("""^SELECT\s+?.*""", RegexOption.IGNORE_CASE)
        private val REGEX_DESCRIBE = Regex("""^DESCRIBE\s+?.*""", RegexOption.IGNORE_CASE)
        private val REGEX_QUERIES = Regex("""^(LIST|SHOW)\s+?QUERIES(\s|;)*""", RegexOption.IGNORE_CASE)
        private val REGEX_STREAMS = Regex("""^(LIST|SHOW)\s+?STREAMS(\s|;)*""", RegexOption.IGNORE_CASE)
        private val REGEX_TABLES = Regex("""^(LIST|SHOW)\s+?TABLES(\s|;)*""", RegexOption.IGNORE_CASE)
        private val REGEX_TOPICS = Regex("""^(LIST|SHOW)\s+?TOPICS(\s|;)*""", RegexOption.IGNORE_CASE)
        private val emptyResponseSelect = KsqlResponseSelect(KsqlResponseSelectColumns(arrayOf()))
        private val emptyResponseQueries =
                arrayOf(KsqlResponseQueries("", arrayOf(KsqlResponseQueriesInner("", "", ""))))
        private val emptyResponseStreams =
                arrayOf(KsqlResponseStreams("", arrayOf(KsqlResponseStreamsInner("", "", ""))))
        private val emptyResponseTables =
                arrayOf(KsqlResponseTables("", arrayOf(KsqlResponseTablesInner("", "", "", false))))
        private val emptyResponseTopics =
                arrayOf(KsqlResponseTopics("", arrayOf(KsqlResponseTopicsInner("", false, emptyArray(), 0, 0))))
        private val emptyResponseDescribe =
                arrayOf(KsqlResponseDescribe("", KsqlResponseDescribeInner("", arrayOf())))
    }

    fun execute(request: Request): Flux<ResponseTable> {
        logger.debug("KSQL server={}", ksqlServerConfig)

        // see aslo:
        // https://github.com/confluentinc/ksql/blob/master/ksql-parser/src/main/antlr4/io/confluent/ksql/parser/SqlBase.g4
        val sql = request.sql.trimStart().trimEnd()
        if (REGEX_SELECT.matches(sql)) {
            return select(request)
        } else if (REGEX_DESCRIBE.matches(sql)) {
            return describe(request)
        } else if (REGEX_QUERIES.matches(sql)) {
            return queries(request)
        } else if (REGEX_STREAMS.matches(sql)) {
            return streams(request)
        } else if (REGEX_TABLES.matches(sql)) {
            return tables(request)
        } else if (REGEX_TOPICS.matches(sql)) {
            return topics(request)
        } else {
            val rawMessage = """Currently, Tsūjun supports only the following syntax.
                |
                | - SELECT
                | - DESCRIBE
                | - (LIST | SHOW) QUERIES
                | - (LIST | SHOW) STREAMS
                | - (LIST | SHOW) TABLES
                | - (LIST | SHOW) TOPICS
            """.trimMargin()
            val message = mapper.writeValueAsString(KsqlResponseErrorMessage(rawMessage, emptyList()))
            throw KsqlException(request.sequence, request.sql, HttpStatus.BAD_REQUEST.value(), message)
        }
    }

    private fun command(request: Request): Flux<ResponseTable> {
        return commandClient(request)
                .flatMap { commandsResult ->

                    val responseTables = mutableListOf<ResponseTable>()
                    for (jsonNode in commandsResult) {
                        val typeCommand = jsonNode.path("@type").asText()
                        if (typeCommand.isEmpty()) {
                            continue
                        }
                        when(typeCommand) {
                            TYPE_COMMAND_DESCRIBE -> responseTables.addAll(handleDescribe(request, jsonNode))
                        }

                    }
                    Flux.fromIterable(responseTables)
                }
    }

    private fun handleDescribe(request: Request, json: JsonNode): List<ResponseTable> {
        val res = mapper.convertValue<KsqlResponseDescribe>(json, KsqlResponseDescribe::class.java)
        val headerTable = ResponseTable(sequence = request.sequence,
                sql = request.sql,
                mode = 1,
                data = arrayOf("Name :", res.sourceDescription.name))

        val headerFields = ResponseTable(sequence = request.sequence,
                sql = request.sql,
                mode = 1,
                data = arrayOf("Field ", "Type"))

        val data = res.sourceDescription.fields.map { (name, schema) ->
            ResponseTable(sequence = request.sequence,
                    sql = request.sql,
                    mode = 1,
                    data = arrayOf(name, schema.type))
        }

        val list = mutableListOf(statementsResponseTable(request, res.statementText), headerTable, headerFields)
        list.addAll(data)
        return list
    }

    private fun select(request: Request): Flux<ResponseTable> {
        // In the WebClient, sometimes the response gets cut off in the middle of json.
        // For example, as in the following log.
        // So, temporarily save the failed data to a variable, combine and use the next response.
        //
        // 2017-12-28 20:54:27.832 DEBUG 70874 --- [ctor-http-nio-5] i.matsumana.tsujun.service.KsqlService   : {"r
        // 2017-12-28 20:54:27.833 DEBUG 70874 --- [ctor-http-nio-5] i.matsumana.tsujun.service.KsqlService   : ow":{"columns":["Page_27",0]},"errorMessage":null}
        val previousFailed = ArrayDeque<String>()
        val streamProperties = mapOf("ksql.streams.auto.offset.reset" to "earliest")

        // TODO handle query stream limit timeout?.

        val httpClient = HttpClient.create().tcpConfiguration { client ->
            client.doOnConnected({
              it.addHandlerLast(ReadTimeoutHandler(10)).addHandlerLast(WriteTimeoutHandler(10))
            })
        }


        return WebClient.builder()
                .clientConnector(ReactorClientHttpConnector(httpClient))
                .baseUrl(ksqlServerConfig.server)
                .build()
                .post()
                .uri("/query")
                .body(Mono.just(KsqlRequest(request.sql, streamProperties)), KsqlRequest::class.java)
                .retrieve()
                .bodyToFlux(String::class.java)
                .doOnError { e ->
                    handleException(e, request)
                }
                .map { orgString ->
                    logger.debug(orgString)

                    val s = orgString.trim()
                    if (s.isEmpty()) {
                        emptyResponseSelect
                    } else {
                        try {
                            mapper.readValue<KsqlResponseSelect>(s)
                        } catch (ignore: IOException) {
                            if (previousFailed.isEmpty()) {
                                previousFailed.addFirst(s)
                                emptyResponseSelect
                            } else {
                                try {
                                    val completeJson = previousFailed.removeFirst() + s
                                    mapper.readValue<KsqlResponseSelect>(completeJson)
                                } catch (ignore: IOException) {
                                    emptyResponseSelect
                                }
                            }
                        }
                    }
                }
                .filter { (row) -> !row.columns.isEmpty() }
                .map { (row) ->
                    ResponseTable(sequence = request.sequence,
                            sql = request.sql,
                            mode = 1,
                            data = row.columns)
                }
    }

    private fun describe(request: Request): Flux<ResponseTable> {
        return generateWebClientKsql(request)
                .map { orgString ->
                    logger.debug(orgString)

                    val s = orgString.trim()
                    if (s.isEmpty()) {
                        emptyResponseDescribe
                    } else {
                        try {
                            mapper.readValue<Array<KsqlResponseDescribe>>(s)
                        } catch (ignore: IOException) {
                            emptyResponseDescribe
                        }
                    }
                }
                .map { res -> res[0] }
                .flatMap { res ->


                    val headerTable = ResponseTable(sequence = request.sequence,
                            sql = request.sql,
                            mode = 1,
                            data = arrayOf("Name :", res.sourceDescription.name))

                    val headerFields = ResponseTable(sequence = request.sequence,
                            sql = request.sql,
                            mode = 1,
                            data = arrayOf("Field ", "Type"))

                    val data = res.sourceDescription.fields.map { (name, schema) ->
                        ResponseTable(sequence = request.sequence,
                                sql = request.sql,
                                mode = 1,
                                data = arrayOf(name, schema.type))
                    }

                    val list = mutableListOf(headerTable, headerFields)
                    list.addAll(data)
                    Flux.fromIterable(list)
                }
    }

    private fun queries(request: Request): Flux<ResponseTable> {
        return generateWebClientKsql(request)
                .map { orgString ->
                    logger.debug(orgString)

                    val s = orgString.trim()
                    if (s.isEmpty()) {
                        emptyResponseQueries
                    } else {
                        try {
                            mapper.readValue<Array<KsqlResponseQueries>>(s)
                        } catch (ignore: IOException) {
                            emptyResponseQueries
                        }
                    }
                }
                .map { res -> res[0] }
                .filter { res -> !res.queries.isEmpty() }
                .flatMap { res ->
                    val header = ResponseTable(sequence = request.sequence,
                            sql = request.sql,
                            mode = 1,
                            data = arrayOf("ID", "Kafka Topic", "Query String"))

                    val data = res.queries.map { (id, kafkaTopic, queryString) ->
                        ResponseTable(sequence = request.sequence,
                                sql = request.sql,
                                mode = 1,
                                data = arrayOf(id, kafkaTopic, queryString))
                    }

                    val list = mutableListOf(header)
                    list.addAll(data)
                    Flux.fromIterable(list)
                }
    }

    private fun streams(request: Request): Flux<ResponseTable> {
        return generateWebClientKsql(request)
                .map { orgString ->
                    logger.debug(orgString)

                    val s = orgString.trim()
                    if (s.isEmpty()) {
                        emptyResponseStreams
                    } else {
                        try {
                            mapper.readValue<Array<KsqlResponseStreams>>(s)
                        } catch (ignore: IOException) {
                            emptyResponseStreams
                        }
                    }
                }
                .map { res -> res[0] }
                .filter { res -> !res.streams.isEmpty() }
                .flatMap { res ->
                    val header = ResponseTable(sequence = request.sequence,
                            sql = request.sql,
                            mode = 1,
                            data = arrayOf("Stream Name", "Ksql Topic", "Format"))

                    val data = res.streams.map { (name, topic, format) ->
                        ResponseTable(sequence = request.sequence,
                                sql = request.sql,
                                mode = 1,
                                data = arrayOf(name, topic, format))
                    }

                    val list = mutableListOf(header)
                    list.addAll(data)
                    Flux.fromIterable(list)
                }
    }

    private fun tables(request: Request): Flux<ResponseTable> {
        return generateWebClientKsql(request)
                .map { orgString ->
                    logger.debug(orgString)

                    val s = orgString.trim()
                    if (s.isEmpty()) {
                        emptyResponseTables
                    } else {
                        try {
                            mapper.readValue<Array<KsqlResponseTables>>(s)
                        } catch (ignore: IOException) {
                            emptyResponseTables
                        }
                    }
                }
                .map { res -> res[0] }
                .filter { res -> !res.tables.isEmpty() }
                .flatMap { res ->
                    val header = ResponseTable(sequence = request.sequence,
                            sql = request.sql,
                            mode = 1,
                            data = arrayOf("Stream Name", "Ksql Topic", "Format", "Windowed"))

                    val data = res.tables.map { (name, topic, format, isWindowed) ->
                        ResponseTable(sequence = request.sequence,
                                sql = request.sql,
                                mode = 1,
                                data = arrayOf(name, topic, format, isWindowed))
                    }

                    val list = mutableListOf(header)
                    list.addAll(data)
                    Flux.fromIterable(list)
                }
    }

    private fun topics(request: Request): Flux<ResponseTable> {
        return generateWebClientKsql(request)
                .map { orgString ->
                    logger.debug(orgString)

                    val s = orgString.trim()
                    if (s.isEmpty()) {
                        emptyResponseTopics
                    } else {
                        try {
                            mapper.readValue<Array<KsqlResponseTopics>>(s)
                        } catch (ignore: IOException) {
                            emptyResponseTopics
                        }
                    }
                }
                .map { res -> res[0] }
                .filter { res -> !res.topics.isEmpty() }
                .flatMap { res ->
                    val header = ResponseTable(sequence = request.sequence,
                            sql = request.sql,
                            mode = 1,
                            data = arrayOf("Kafka Topic", "Registered", "Partitions", "Partitions Replicas", "Consumer", "ConsumerGroups"))

                    val data = res.topics.map { (name, registered, replicaInfo, consumerCount, consumerGroupCount) ->
                        ResponseTable(sequence = request.sequence,
                                sql = request.sql,
                                mode = 1,
                                data = arrayOf(name, registered, replicaInfo.size, replicaInfo.max().toString(), consumerCount, consumerGroupCount))
                    }

                    val list = mutableListOf(header)
                    list.addAll(data)
                    Flux.fromIterable(list)
                }
    }

    private fun generateWebClientKsql(request: Request): Flux<String> {
        return WebClient.create(ksqlServerConfig.server)
                .post()
                .uri("/ksql")
                .body(Mono.just(KsqlRequest(request.sql)), KsqlRequest::class.java)
                .retrieve()
                .bodyToFlux(String::class.java)
                .doOnError { e ->
                    handleException(e, request)
                }
    }

    private fun commandClient(request: Request): Flux<ArrayNode> {
        return WebClient.create(ksqlServerConfig.server)
                .post()
                .uri("/ksql")
                .body(Mono.just(KsqlRequest(request.sql)), KsqlRequest::class.java)
                .retrieve()
                .bodyToFlux(ArrayNode::class.java)
                .doOnError { e ->
                    handleException(e, request)
                }
    }

    private fun handleException(e: Throwable, request: Request) {
        logger.info("WebClient Error", e)

        if (e is WebClientResponseException) {
            throw KsqlException(request.sequence, request.sql, e.statusCode.value(), e.responseBodyAsString)
        } else {
            throw e
        }
    }
}
