package com.schooldevops.kafka.stream.aggregatingwordcntdemo.controller;

import com.schooldevops.kafka.stream.aggregatingwordcntdemo.producer.KafkaProducer;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.kstream.Window;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.*;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@AllArgsConstructor
@RestController
public class WordCountController {

    private final StreamsBuilderFactoryBean factoryBean;

    private final KafkaProducer kafkaProducer;

    @GetMapping("/count-word/{word}/{latestSeconds}")
    public String getWordCount(@PathVariable("word") String word, @PathVariable("latestSeconds") Long latestSeconds) {
        List<Map<String, Object>> bpms = new ArrayList<>();

        Instant fromTime = Instant.ofEpochMilli(System.currentTimeMillis() - (latestSeconds * 1000));
        Instant toTime = Instant.now();

        KafkaStreams kafkaStreams =  factoryBean.getKafkaStreams();
        ReadOnlyWindowStore<String, Long> store = kafkaStreams.store(
                StoreQueryParameters.fromNameAndType("output-store", QueryableStoreTypes.windowStore()));
        WindowStoreIterator<Long> range = store.fetch(word, fromTime, toTime);
        while (range.hasNext()) {
            Map<String, Object> bpm = new HashMap<>();
            KeyValue<Long, Long> next = range.next();
            Long timestamp = next.key;
            Long count = next.value;
            bpm.put("timestamp", Instant.ofEpochMilli(timestamp).toString());
            bpm.put("count", count);
            bpms.add(bpm);
        }
        // close the iterator to avoid memory leaks
        range.close();

        return bpms.toString();
    }

    @GetMapping("/count-range/{latestSeconds}")
    public String getWordCountAll(@PathVariable("latestSeconds") Long latestSeconds) {
        List<Map<String, Object>> bpms = new ArrayList<>();

        Instant fromTime = Instant.ofEpochMilli(System.currentTimeMillis() - (latestSeconds * 1000));
        Instant toTime = Instant.now();

        KafkaStreams kafkaStreams =  factoryBean.getKafkaStreams();

        ReadOnlyWindowStore<String, Long> store = kafkaStreams.store(
                StoreQueryParameters.fromNameAndType("output-store", QueryableStoreTypes.windowStore()));
        KeyValueIterator<Windowed<String>, Long> range = store.fetchAll(fromTime, toTime);
        while (range.hasNext()) {
            Map<String, Object> bpm = new HashMap<>();
            KeyValue<Windowed<String>, Long> next = range.next();
            String key = next.key.key();
            Window window = next.key.window();
            Long start = window.start();
            Long end = window.end();
            Long count = next.value;
            bpm.put("key", key);
            bpm.put("start", Instant.ofEpochMilli(start).toString());
            bpm.put("end", Instant.ofEpochMilli(end).toString());
            bpm.put("count", count);
            bpms.add(bpm);
        }
        // close the iterator to avoid memory leaks
        range.close();
        // return a JSON response
        return bpms.toString();
    }


    @GetMapping("/count-all")
    public String getWordCountAll2() {
        Map<String, Long> bpm = new HashMap<>();

        KafkaStreams kafkaStreams =  factoryBean.getKafkaStreams();
        ReadOnlyWindowStore<String, Long> store = kafkaStreams.store(
                StoreQueryParameters.fromNameAndType("output-store", QueryableStoreTypes.windowStore()));

        KeyValueIterator<Windowed<String>, Long> range = store.all();
        while (range.hasNext()) {
            KeyValue<Windowed<String>, Long> next = range.next();
            Windowed<String> key = next.key;
            Long value = next.value;
            bpm.put(key.toString(), value);

            log.info("------------------- Key {}, Value {}", key.toString(), value);
        }
        range.close();
        return bpm.toString();
    }

    @PostMapping("/message")
    public void addMessage(@RequestBody String message) {
        kafkaProducer.sendMessage(message);
    }
}
