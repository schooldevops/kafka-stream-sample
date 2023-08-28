package com.schooldevops.kafka.stream.aggregatingwordcntdemo.topology;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.WindowStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Arrays;
import java.util.regex.Pattern;

@Component
public class WordCountProcessor {
    private static final Serde<String> STRING_SERDE = Serdes.String();

//    @Autowired
//    public void buildPipeline(StreamsBuilder streamsBuilder) {
//        KStream<String, String> messageStream = streamsBuilder
//                .stream("input-topic", Consumed.with(STRING_SERDE, STRING_SERDE));
//
//        KTable<String, Long> wordCounts = messageStream
//                .mapValues((ValueMapper<String, String>) String::toLowerCase)
//                .flatMapValues(value -> Arrays.asList(value.split("\\W+")))
//                .groupBy((key, word) -> word, Grouped.with(STRING_SERDE, STRING_SERDE))
//                .count(Materialized.as("counts"));
//
//        wordCounts.toStream().to("output-topic");
//    }

//    @Autowired
//    public Topology getTopology(StreamsBuilder builder) {
//        final Serde<String> stringSerde = Serdes.String();
//        final Serde<Long> longSerde = Serdes.Long();
//
////        final StreamsBuilder builder = new StreamsBuilder();
//        final KStream<String, String> textLines = builder.stream("input-topic");
//
//        final Pattern pattern = Pattern.compile("\\W+", Pattern.UNICODE_CHARACTER_CLASS);
//        final KTable<String, Long> wordCounts = textLines
//                .flatMapValues(value -> Arrays.asList(pattern.split(value.toLowerCase())))
//                .groupBy((key, word) -> word)
//                .count(Materialized.as("counts"));
//
//        wordCounts.toStream().to("output-topic", Produced.with(stringSerde, longSerde));
//        return builder.build();
//    }


    @Autowired
    public Topology getTopology(StreamsBuilder builder) {
        Consumed<String, String> pulseConsumerOptions =
                Consumed.with(Serdes.String(), Serdes.String());

        KStream<String, String>
                textLines = builder.stream("input-topic", pulseConsumerOptions);
        final Pattern pattern = Pattern.compile("\\W+", Pattern.UNICODE_CHARACTER_CLASS);

        TimeWindows tumblingWindow =
                TimeWindows.of(Duration.ofSeconds(60)).grace(Duration.ofSeconds(5));

        KTable<Windowed<String>, Long> wordCounts =
                textLines
                        .flatMapValues(value -> Arrays.asList(pattern.split(value.toLowerCase())))
                        .groupBy((key, word) -> word)
                        .windowedBy(tumblingWindow)
                        .count(Materialized.as("output-store"))
//                        .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded().shutDownWhenFull()))
                ;

        return builder.build();
    }

//    @Autowired
//    public Topology getTopology(StreamsBuilder builder) {
//        final Serde<String> stringSerde = Serdes.String();
//        final Serde<Long> longSerde = Serdes.Long();
//
//        KStream<String, String>
//                textLines = builder.stream("input-topic", Consumed.with(Serdes.String(), Serdes.String()));
//        final Pattern pattern = Pattern.compile("\\W+", Pattern.UNICODE_CHARACTER_CLASS);
//
//        final KGroupedStream<String, String> groupedByWord = textLines
//                .flatMapValues(value -> Arrays.asList(pattern.split(value.toLowerCase())))
//                .groupBy((key, word) -> word);
//        // Create a State Store for with the all time word count
//        groupedByWord.count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("word-count")
//                .withValueSerde(Serdes.Long()));
//        // Create a Windowed State Store that contains the word count for every
//        // 1 minute
//        KTable<Windowed<String>, Long> windowTable = groupedByWord.windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(60)))
//                .count(Materialized.<String, Long, WindowStore<Bytes, byte[]>>as("timed-word-count")
//                        .withValueSerde(Serdes.Long()));
//
//        windowTable.toStream((windowedRegion, count) -> windowedRegion.toString())
//                .to("output-topic", Produced.with(Serdes.String(), Serdes.Long()));
//        return builder.build();
//    }
}
