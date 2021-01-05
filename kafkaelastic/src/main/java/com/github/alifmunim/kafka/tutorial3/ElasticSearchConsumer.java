package com.github.alifmunim.kafka.tutorial3;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ElasticSearchConsumer {

    public static RestHighLevelClient createClient() {

        //https://pqx3bvo6cu:8crmt0j69q@odl-performance-8359437335.us-east-1.bonsaisearch.net:443

        String hostname = "localhost";
        String username = "";
        String password = "";

        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(username, password));

        RestClientBuilder builder = RestClient.builder(
                new HttpHost(hostname, 9200, "http")
        ).setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
            @Override
            public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
                return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
            }
        });

        RestHighLevelClient client = new RestHighLevelClient(builder);
        return client;
    }

    public static KafkaConsumer<String, String> createConsumer(String topic) {
        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "kafka-demo-elasticsearch";

        // Create consumer configs
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        // Create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList(topic));
        return consumer;
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

        if (obj.getJSONObject("retweeted_status") != null) {
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

            if (obj.getBoolean("is_quote_status")) {
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

        if (obj.getJSONObject("retweeted_status") != null) {
            objNew.put("retweeted_status", objRetweetNew);

            if (obj.getBoolean("is_quote_status")) {
                objNew.getJSONObject("retweeted_status").put("quoted_status", objRetweetQuotedNew);
            }
        }

        String newJsonString = objNew.toString();

        return newJsonString;
    }

    public static void main(String[] args) throws IOException {
        Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());

        RestHighLevelClient client = createClient();

        // Create consumer
        KafkaConsumer<String, String> consumer = createConsumer("twitter_tweets");

        int maxTweets = 10;
        int numTweets = 0;

        boolean done = false;
        // Poll for new data
        while (!done) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            // Insert data into elasticsearch
            for (ConsumerRecord<String, String> record : records) {
                // Create a JSON string
                String jsonString = record.value();
                String jsonExtract = extractJsonInfo(jsonString);

                // Create an index request
                IndexRequest indexRequest = new IndexRequest(
                        "twitter06"
                ).source(jsonExtract, XContentType.JSON);

                // Send index request and get ID from response
                IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
                String id = indexResponse.getId();
                logger.info(id);

//                try {
//                    Thread.sleep(250);
//                } catch(InterruptedException e) {
//                    e.printStackTrace();
//                }

                numTweets += 1;

                if (numTweets >= maxTweets) {
                    break;
                }
            }

            if (numTweets >= maxTweets) {
                System.out.printf("Indexed %d documents\n", maxTweets);
                done = true;
                client.close();
            }

        }

//      client.close();
    }

}
