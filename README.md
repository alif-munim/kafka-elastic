# kafka-elastic

A simple ETL pipeline implementations that collect twitter data from Kafka and post to Elasticsearch.

### setup
1. Git clone this repository
2. Import the project into IntelliJ IDEA

### twitterproducer
This producer is used in both the **kafkaelastic** and **esflink** pipelines.

1. Create a `resources` folder at the same level as the `src` folder of the **twitterproducer** module.
1. Inside of the `resources` folder, create a file named `config.properties`.
1. Enter the twitter client configuration properties (consumerKey, consumerSecret, token, secret) inside `config.properties`.
1. In `TwitterProducer.java`, configure the number of tweets you'd like to produce by setting the `maxTweets` variable.
1. Run the `main()` method of `TwitterProducer.java` and begin producing tweets to the **twitter_tweets** topic.

### kafkaelastic
1. Start your local kafka.
1. Start your local elasticsearch.
1. Ensure **twitterproducer** has finished producing tweets to the **twitter_tweets** topic.
1. In `ElasticSearchConsumer.java`, configure the number of tweets you'd like to consume by setting the `maxTweets` variable.
1. Run the `main()` method of `ElasticSearchConsumer.java`.
1. To view your data, navigate to http://localhost:9200/[your-index]/_search?pretty=true.
1. To view statistics about your index, navigate to http://localhost:9200/[your-index]/_stats?pretty=true.
