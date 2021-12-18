package ru.curs.homework.configuration;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;

import java.util.HashMap;
import java.util.Map;

import static ru.curs.homework.configuration.TopologyConfiguration.*;

@Configuration
public class KafkaConfiguration {
    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration getStreamsConfig() {
        Map<String, Object> props = new HashMap<>();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "homework-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
        props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
                         LogAndContinueExceptionHandler.class);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 200);
        KafkaStreamsConfiguration streamsConfig = new KafkaStreamsConfiguration(props);
        return streamsConfig;
    }

    @Bean
    public NewTopic betAmountsForBettor() {
        return new NewTopic(BETTOR_AMOUNTS, 10, (short) 1);
    }

    @Bean
    public NewTopic betAmountsForTeam() {
        return new NewTopic(TEAM_AMOUNTS, 10, (short) 1);
    }

    @Bean
    public NewTopic possibleFraudTopic() {
        return new NewTopic(POSSIBLE_FRAUDS, 10, (short) 1);
    }

}
