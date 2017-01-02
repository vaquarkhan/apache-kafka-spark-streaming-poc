package vaquar.khan.poc;

import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import java.util.*;
import java.util.regex.Pattern;

public class KafkaSparkServiceApplication {
    static final Logger log = LogManager.getLogger(KafkaSparkServiceApplication.class.getName());
    private static String brokers = "localhost:9092";
    private static String topics = "test";
    private static Duration batchInterval = Durations.seconds(2);

    public static void main(String[] args) {
        log.debug("Starting consumer...");
        kafkaConsumer();
    }

    public static void kafkaConsumer() {
        SparkConf sparkConf = new SparkConf().setAppName("KafkaWordCount").setMaster("local[*]");
        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, batchInterval);
        JavaPairInputDStream<String, String> stream = setupDirectStream(streamingContext);
        JavaPairDStream<String, Integer> wordCounts = processStream(stream);
        wordCounts.print();

        streamingContext.start();
        streamingContext.awaitTermination();
    }

    private static JavaPairInputDStream<String, String> setupDirectStream(JavaStreamingContext streamingContext) {
        Set<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));
        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", brokers);

        return createKafkaDirectStream(streamingContext, topicsSet, kafkaParams);
    }

    private static JavaPairDStream<String, Integer> processStream(JavaPairInputDStream<String, String> messages) {
        JavaDStream<String> lines = extractLines(messages);
        JavaDStream<String> words = splitLines(lines);
        return countWords(words);
    }

    private static JavaPairInputDStream<String, String> createKafkaDirectStream(JavaStreamingContext jssc, Set<String> topicsSet, Map<String, String> kafkaParams) {
        return KafkaUtils.createDirectStream(
                jssc,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                kafkaParams,
                topicsSet
        );
    }

    private static JavaPairDStream<String, Integer> countWords(JavaDStream<String> words) {
        return words.mapToPair(
            new PairFunction<String, String, Integer>() {
                @Override
                public Tuple2<String, Integer> call(String s) {
                    log.debug("Preparing to reduce... {}", s);
                    return new Tuple2<>(s, 1);
                }
            }).reduceByKey((i1, i2) -> i1 + i2);
    }

    private static JavaDStream<String> splitLines(JavaDStream<String> lines) {
        Pattern SPACE = Pattern.compile(" ");
        return lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String line) {
                log.debug("Splitting line... {}", line);
                return Arrays.asList(SPACE.split(line));
            }
        });
    }

    private static JavaDStream<String> extractLines(JavaPairInputDStream<String, String> messages) {
        return messages.map(new Function<Tuple2<String, String>, String>() {
            @Override
            public String call(Tuple2<String, String> tuple2) {
                log.debug("Getting line... {}", tuple2);
                return tuple2._2();
            }
        });
    }
}
