package ai.tracewell.stream;

import ai.tracewell.stream.model.AiEvent;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/** Keyed by event_id: lets the first copy through and drops resends. State expires after `ttl`. */
public class Deduper extends KeyedProcessFunction<String, AiEvent, AiEvent> {
    private final Duration ttl;
    private transient ValueState<Boolean> seen;
    private transient Counter duplicates;

    public Deduper(Duration ttl) {
        this.ttl = ttl;
    }

    @Override
    public void open(Configuration parameters) {
        ValueStateDescriptor<Boolean> desc = new ValueStateDescriptor<>("seen", Boolean.class);
        desc.enableTimeToLive(StateTtlConfig.newBuilder(ttl)
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build());
        seen = getRuntimeContext().getState(desc);
        duplicates = getRuntimeContext().getMetricGroup().counter("events_deduped");
    }

    @Override
    public void processElement(AiEvent e, Context ctx, Collector<AiEvent> out) throws Exception {
        if (seen.value() != null) {
            duplicates.inc();
            return;
        }
        seen.update(true);
        out.collect(e);
    }
}
