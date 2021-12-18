package ru.curs.homework.transformers;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import ru.curs.counting.model.Bet;

import java.util.Optional;



public class SumOfBetsForBettorTransformer implements Transformer<String, Bet, KeyValue<String, Long>> {
    private KeyValueStore<String, Long> stateStore;
    public static final String BETTORS_BET_SUM_STORE_NAME = "bettorsBetSumStore";

    @Override
    public void init(ProcessorContext processorContext) {
        this.stateStore = processorContext.getStateStore(BETTORS_BET_SUM_STORE_NAME);
    }

    @Override
    public KeyValue<String, Long> transform(String match, Bet bet) {
        String bettor = bet.getBettor();
        long bettorBetSum = Optional
                .ofNullable(stateStore.get(bettor))
                .orElse(0L);
        bettorBetSum += bet.getAmount();
        stateStore.put(bettor, bettorBetSum);
        return KeyValue.pair(bettor, bettorBetSum);
    }

    @Override
    public void close() { }

};

