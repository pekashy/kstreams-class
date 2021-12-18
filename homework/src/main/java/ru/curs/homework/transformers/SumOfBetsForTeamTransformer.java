package ru.curs.homework.transformers;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import ru.curs.counting.model.Bet;
import ru.curs.counting.model.Outcome;

import java.util.Optional;



public class SumOfBetsForTeamTransformer implements Transformer<String, Bet, KeyValue<String, Long>> {
    private KeyValueStore<String, Long> stateStore;
    public static final String TEAMS_BET_SUM_STORE_NAME = "teamsBetSumStore";

    @Override
    public void init(ProcessorContext processorContext) {
        this.stateStore = processorContext.getStateStore(TEAMS_BET_SUM_STORE_NAME);
    }

    @Override
    public KeyValue<String, Long> transform(String match, Bet bet) {
        String wonTeam = getWonTeam(bet);
        long teamBetSum = Optional
                .ofNullable(stateStore.get(wonTeam))
                .orElse(0L);
        teamBetSum += bet.getAmount();
        stateStore.put(wonTeam, teamBetSum);
        return KeyValue.pair(wonTeam, teamBetSum);
    }

    private String getWonTeam(Bet bet) {
        Outcome matchOutcome = bet.getOutcome();

        String match = bet.getMatch();
        String[] teams = match.split("-");
        if (matchOutcome == Outcome.A) {
            return teams[1];
        }
        return teams[0];
    }

    @Override
    public void close() { }

};

