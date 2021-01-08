package com.github.alifmunim;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ActionRequestFailureHandler;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.ExceptionUtils;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.xcontent.XContentType;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class FlinkStream {

    private static final String READ_TOPIC = "twitter_tweets";

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
        logger.info("tweet:" + student);
        List<HttpHost> esHttphost = new ArrayList<>();
        esHttphost.add(new HttpHost("127.0.0.1", 9200, "http"));

        ElasticsearchSink.Builder<String> esSinkBuilder = new ElasticsearchSink.Builder<>(
                esHttphost,
                new ElasticsearchSinkFunction<String>() {

                    public IndexRequest createIndexRequest(String element) {
//                        Map<String, String> json = new HashMap<>();
//                        json.put("data", element);
//                        logger.info("data:" + element);

                        String jsonExtract = extractJsonInfo(element);

                        return Requests.indexRequest()
                                .index("twitter37")
                                .type("tweet")
                                .source(jsonExtract, XContentType.JSON);
                    }

                    @Override
                    public void process(String element, RuntimeContext ctx, RequestIndexer indexer) {
                        indexer.add(createIndexRequest(element));
                    }
                }
        );
        
        esSinkBuilder.setBulkFlushMaxActions(3000);
        esSinkBuilder.setBulkFlushInterval(500);

        esSinkBuilder.setRestClientFactory(restClientBuilder -> {
            restClientBuilder.setDefaultHeaders(new BasicHeader[]{new BasicHeader("Content-Type","application/json")});
        });

        esSinkBuilder.setFailureHandler(new ActionRequestFailureHandler() {
            @Override
            public void onFailure(ActionRequest action, Throwable failure, int restStatusCode, RequestIndexer indexer) throws Throwable {
                if (ExceptionUtils.findThrowable(failure, EsRejectedExecutionException.class).isPresent()) {
                    // full queue; re-add document for indexing
                    indexer.add(action);
                    System.out.println("Failed");
                } else if (ExceptionUtils.findThrowable(failure, ElasticsearchParseException.class).isPresent()) {
                    // malformed document; simply drop request without failing sink
                } else {
                    // for all other failures, fail the sink
                    // here the failure is simply rethrown, but users can also choose to throw custom exceptions
                    throw failure;
                }
            }
        });

        student.addSink(esSinkBuilder.build());
        env.execute("Kafka as source, elastic search as sinker");
    }

    public static String extractJsonInfo(String jsonString) {
        JSONObject obj = new JSONObject(jsonString);

        String text = obj.getString("text");
        String user = obj.getJSONObject("user").getString("screen_name");
        String date = obj.getString("created_at");

        JSONArray hashtags = obj.getJSONObject("entities").getJSONArray("hashtags");
        JSONArray user_mentions = obj.getJSONObject("entities").getJSONArray("user_mentions");
        JSONArray urls = obj.getJSONObject("entities").getJSONArray("urls");

        int quote_count = obj.getInt("quote_count");
        int reply_count = obj.getInt("reply_count");
        int retweet_count = obj.getInt("retweet_count");
        int favorite_count = obj.getInt("favorite_count");

        boolean is_quote_status = obj.getBoolean("is_quote_status");
        boolean favorited = obj.getBoolean("favorited");
        boolean retweeted = obj.getBoolean("retweeted");

        JSONObject objRetweetNew = new JSONObject();
        JSONObject objRetweetQuotedNew = new JSONObject();

        if (obj.has("retweeted_status")) {
            JSONObject objRetweet = obj.getJSONObject("retweeted_status");

            String retweeted_text = objRetweet.getString("text");
            String retweeted_user = objRetweet.getJSONObject("user").getString("screen_name");
            String retweeted_date = objRetweet.getString("created_at");

            JSONArray retweeted_hashtags = objRetweet.getJSONObject("entities").getJSONArray("hashtags");
            JSONArray retweeted_user_mentions = objRetweet.getJSONObject("entities").getJSONArray("user_mentions");
            JSONArray retweeted_urls = objRetweet.getJSONObject("entities").getJSONArray("urls");

            int retweeted_quote_count = objRetweet.getInt("quote_count");
            int retweeted_reply_count = objRetweet.getInt("reply_count");
            int retweeted_retweet_count = objRetweet.getInt("retweet_count");
            int retweeted_favorite_count = objRetweet.getInt("favorite_count");

            boolean retweeted_is_quote_status = objRetweet.getBoolean("is_quote_status");
            boolean retweeted_favorited = objRetweet.getBoolean("favorited");
            boolean retweeted_retweeted = objRetweet.getBoolean("retweeted");

            objRetweetNew.put("text", retweeted_text)
                    .put("user", retweeted_user)
                    .put("date", retweeted_date)
                    .put("hashtags", retweeted_hashtags)
                    .put("user_mentions", retweeted_user_mentions)
                    .put("urls", retweeted_urls)
                    .put("quote_count", retweeted_quote_count)
                    .put("reply_count", retweeted_reply_count)
                    .put("retweet_count", retweeted_retweet_count)
                    .put("favorite_count", retweeted_favorite_count)
                    .put("is_quote_status", retweeted_is_quote_status)
                    .put("favorited", retweeted_favorited)
                    .put("retweeted", retweeted_retweeted);

            if (obj.has("quoted_status")) {
                JSONObject objQuoted = objRetweet.getJSONObject("quoted_status");

                String quoted_text = objQuoted.getString("text");
                String quoted_user = objQuoted.getJSONObject("user").getString("screen_name");
                String quoted_date = objQuoted.getString("created_at");

                JSONArray quoted_hashtags = objQuoted.getJSONObject("entities").getJSONArray("hashtags");
                JSONArray quoted_user_mentions = objQuoted.getJSONObject("entities").getJSONArray("user_mentions");
                JSONArray quoted_urls = objQuoted.getJSONObject("entities").getJSONArray("urls");

                int quoted_quote_count = objQuoted.getInt("quote_count");
                int quoted_reply_count = objQuoted.getInt("reply_count");
                int quoted_retweet_count = objQuoted.getInt("retweet_count");
                int quoted_favorite_count = objQuoted.getInt("favorite_count");

                boolean quoted_is_quote_status = objQuoted.getBoolean("is_quote_status");
                boolean quoted_favorited = objQuoted.getBoolean("favorited");
                boolean quoted_retweeted = objQuoted.getBoolean("retweeted");

                objRetweetQuotedNew.put("text", quoted_text)
                        .put("user", quoted_user)
                        .put("date", quoted_date)
                        .put("hashtags", quoted_hashtags)
                        .put("user_mentions", quoted_user_mentions)
                        .put("urls", quoted_urls)
                        .put("quote_count", quoted_quote_count)
                        .put("reply_count", quoted_reply_count)
                        .put("retweet_count", quoted_retweet_count)
                        .put("favorite_count", quoted_favorite_count)
                        .put("is_quote_status", quoted_is_quote_status)
                        .put("favorited", quoted_favorited)
                        .put("retweeted", quoted_retweeted);
            }
        }

        JSONObject objNew = new JSONObject()
                .put("text", text)
                .put("user", user)
                .put("date", date)
                .put("hashtags", hashtags)
                .put("user_mentions", user_mentions)
                .put("urls", urls)
                .put("quote_count", quote_count)
                .put("reply_count", reply_count)
                .put("retweet_count", retweet_count)
                .put("favorite_count", favorite_count)
                .put("is_quote_status", is_quote_status)
                .put("favorited", favorited)
                .put("retweeted", retweeted);

        if (obj.has("retweeted_status")) {
            objNew.put("retweeted_status", objRetweetNew);

            if (obj.has("quoted_status")) {
                objNew.getJSONObject("retweeted_status").put("quoted_status", objRetweetQuotedNew);
            }
        }

        String newJsonString = objNew.toString();

        return newJsonString;
    }
}
