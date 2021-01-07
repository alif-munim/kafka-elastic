package com.github.alifmunim;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.RestClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class FlinkStream {

    private static final String READ_TOPIC = "student-1";

    public static void main(String[] args) throws Exception {
        Logger logger = LoggerFactory.getLogger(FlinkStream.class.getName());

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("zookeeper.connect", "localhost:2181");
        props.put("group.id", "student-group-1");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "latest");

        DataStreamSource<String> student = env.addSource(new FlinkKafkaConsumer<>(
                READ_TOPIC,
                new SimpleStringSchema(),
                props)).setParallelism(1);
        student.print();
        logger.info("student:" + student);
        List<HttpHost> esHttphost = new ArrayList<>();
        esHttphost.add(new HttpHost("127.0.0.1", 9200, "http"));

        ElasticsearchSink.Builder<String> esSinkBuilder = new ElasticsearchSink.Builder<>(
                esHttphost,
                new ElasticsearchSinkFunction<String>() {

                    public IndexRequest createIndexRequest(String element) {
                        Map<String, String> json = new HashMap<>();
                        json.put("data", element);
                        logger.info("data:" + element);

                        return Requests.indexRequest()
                                .index("index-student")
                                .type("student")
                                .source(json);
                    }

                    @Override
                    public void process(String element, RuntimeContext ctx, RequestIndexer indexer) {
                        indexer.add(createIndexRequest(element));
                    }
                }
        );

        esSinkBuilder.setBulkFlushMaxActions(1);
        esSinkBuilder.setRestClientFactory(restClientBuilder -> {
            restClientBuilder.setDefaultHeaders(new BasicHeader[]{new BasicHeader("Content-Type","application/json")});
            restClientBuilder.setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                @Override
                public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {

                    // elastic search username and password
                    CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
                    credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials("elastic", "changeme"));

                    return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                }
            });
        });

//        esSinkBuilder.setFailureHandler(new ElasticSearchSinkUtil.RetryRejectedExecutionFailureHandler());

        student.addSink(esSinkBuilder.build());
        env.execute("Kafka as source, elastic search as sinker");
    }
}
