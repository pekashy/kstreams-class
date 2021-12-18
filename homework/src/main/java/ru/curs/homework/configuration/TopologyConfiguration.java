package ru.curs.homework.configuration;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.Stores;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonSerde;
import ru.curs.counting.model.*;
import ru.curs.homework.transformers.SumOfBetsForBettorTransformer;
import ru.curs.homework.transformers.SumOfBetsForTeamTransformer;
import ru.curs.homework.transformers.WinBetsTransformer;


import java.time.Duration;

import static ru.curs.counting.model.TopicNames.*;
import static ru.curs.homework.transformers.SumOfBetsForBettorTransformer.BETTORS_BET_SUM_STORE_NAME;
import static ru.curs.homework.transformers.SumOfBetsForTeamTransformer.TEAMS_BET_SUM_STORE_NAME;
import static ru.curs.homework.transformers.WinBetsTransformer.MATCH_SCORE_SUM_STORE_NAME;

@Configuration
@RequiredArgsConstructor
public class TopologyConfiguration {
    public static final String BETTOR_AMOUNTS = "bettor-amounts";
    public static final String TEAM_AMOUNTS = "team-amounts";
    public static final String POSSIBLE_FRAUDS = "possible-frauds";


    private void produceBetSumForPlayer(StreamsBuilder streamsBuilder, KStream<String, Bet> betInput) {
        /*
        Approach with no transform. First implemented this, but topology seemed much nastier to me.

        val groupedByBettorBets = betInput.groupBy((key, value) -> value.getBettor(),
                Serialized.with(Serdes.String(), new JsonSerde<>(Bet.class)));
        KTable<String, Long> sumOfBetsByBettor = groupedByBettorBets.aggregate(() -> 0L,
                (bettorName, bet, total) -> total + bet.getAmount(), Materialized.with(Serdes.String(), Serdes.Long()));
        sumOfBetsByBettor.toStream().to(BETTOR_AMOUNTS);
        */
        streamsBuilder.addStateStore(Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(BETTORS_BET_SUM_STORE_NAME),
                Serdes.String(),
                Serdes.Long()));
        KStream<String, Long> sumOfBetsByBettor = betInput
                .transform(SumOfBetsForBettorTransformer::new, BETTORS_BET_SUM_STORE_NAME);
        sumOfBetsByBettor.to(BETTOR_AMOUNTS, Produced.with(Serdes.String(), Serdes.Long()));
    }

    private void produceBetSumForTeam(StreamsBuilder streamsBuilder, KStream<String, Bet> betInput) {
        streamsBuilder.addStateStore(Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(TEAMS_BET_SUM_STORE_NAME),
                Serdes.String(),
                Serdes.Long()));
        KStream<String, Long> sumOfBetsByTeam = betInput.filter((key, bet) -> bet.getOutcome() != Outcome.D)
                .transform(SumOfBetsForTeamTransformer::new, TEAMS_BET_SUM_STORE_NAME);
        sumOfBetsByTeam.to(TEAM_AMOUNTS, Produced.with(Serdes.String(), Serdes.Long()));
    }

    public void produceFrauds(StreamsBuilder streamsBuilder, KStream<String, Bet> betInput,
                              KStream<String, EventScore> eventInput) {

        streamsBuilder.addStateStore(Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(MATCH_SCORE_SUM_STORE_NAME),
                Serdes.String(),
                new JsonSerde<>(Score.class)
        ));

        final KStream<String, Bet> winBets = eventInput.transform(WinBetsTransformer::new,
                MATCH_SCORE_SUM_STORE_NAME);

        KStream<String, Fraud> fraudsForUser = betInput.join(winBets, (bet, win) ->
                        new Fraud(bet.getBettor(), bet.getMatch(),
                                bet.getOutcome(), bet.getAmount(), bet.getOdds(),
                                win.getTimestamp() - bet.getTimestamp()),
                JoinWindows.of(Duration.ofSeconds(1)).before(Duration.ZERO),
                StreamJoined.with(
                        Serdes.String(), /* key */
                        new JsonSerde<>(Bet.class),   /* left value */
                        new JsonSerde<>(Bet.class))  /* right value */
        ).selectKey((match, fraud) -> fraud.getBettor());

        fraudsForUser.to(POSSIBLE_FRAUDS, Produced.with(Serdes.String(), new JsonSerde<>(Fraud.class)));
    }

    @Bean
    public Topology createTopology(StreamsBuilder streamsBuilder) {

        final KStream<String, Bet> betInput = streamsBuilder.
                stream(BET_TOPIC,
                        Consumed.with(Serdes.String(),
                                new JsonSerde<>(Bet.class))
                                .withTimestampExtractor((record, previousTimestamp) ->
                                        ((Bet) record.value()).getTimestamp())
                );

        final KStream<String, EventScore> eventInput = streamsBuilder.
                stream(EVENT_SCORE_TOPIC,
                        Consumed.with(Serdes.String(),
                                        new JsonSerde<>(EventScore.class))
                                .withTimestampExtractor((record, previousTimestamp) ->
                                        ((EventScore) record.value()).getTimestamp())
                );

        produceBetSumForPlayer(streamsBuilder, betInput);
        produceBetSumForTeam(streamsBuilder, betInput);
        produceFrauds(streamsBuilder, betInput, eventInput);

        Topology topology = streamsBuilder.build();
        System.out.println("========================================");
        System.out.println(topology.describe());
        System.out.println("========================================");
        return topology;
    }
}
