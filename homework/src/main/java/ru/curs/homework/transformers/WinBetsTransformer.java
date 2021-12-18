package ru.curs.homework.transformers;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import ru.curs.counting.model.*;


public class WinBetsTransformer implements Transformer<String, EventScore, KeyValue<String, Bet>> {
    private KeyValueStore<String, Score> stateStore;
    public static final String MATCH_SCORE_SUM_STORE_NAME = "matchScoresStore";

    @Override
    public void init(ProcessorContext processorContext) {
        this.stateStore = processorContext.getStateStore(MATCH_SCORE_SUM_STORE_NAME);
    }

    @Override
    public KeyValue<String, Bet> transform(String match, EventScore event) {
        Score currScore = stateStore.get(match);
        if (currScore == null) {
            currScore = new Score();
        }
        Outcome out = event.getScore().getAway() > currScore.getAway() ? Outcome.A : Outcome.H;
        stateStore.put(match, event.getScore());
        return new KeyValue<>(String.format("%s:%s", match, out),
                new Bet(null, match, out, -1, -1, event.getTimestamp()));
    }

    @Override
    public void close() { }

};

