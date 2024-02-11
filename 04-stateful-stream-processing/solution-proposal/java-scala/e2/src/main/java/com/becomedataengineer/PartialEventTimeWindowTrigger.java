package com.becomedataengineer;

import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class PartialEventTimeWindowTrigger extends Trigger<Object, TimeWindow> {
    private static final Logger LOG = LoggerFactory.getLogger(PartialEventTimeWindowTrigger.class);
    private static final long PARTIAL_RESULTS_MILLIS_THRESHOLD = TimeUnit.MINUTES.toMillis(3L);
    private final EventTimeTrigger eventTimeTrigger = EventTimeTrigger.create();

    @Override
    public TriggerResult onElement(Object element, long watermarkTime, TimeWindow window,
                                   TriggerContext triggerContext) throws Exception {
        // let's check now what are the partial results we should emit
        long remainingMillisBeforeWindowEnd = window.maxTimestamp() - watermarkTime;
        if (remainingMillisBeforeWindowEnd <= PARTIAL_RESULTS_MILLIS_THRESHOLD) {
            LOG.info("Generating partial results because the difference is "+remainingMillisBeforeWindowEnd+ " ms");
            return TriggerResult.FIRE;
        }
        return eventTimeTrigger.onElement(element, watermarkTime, window, triggerContext);
    }

    @Override
    public TriggerResult onProcessingTime(long watermarkTime, TimeWindow window, TriggerContext triggerContext) throws Exception {
        return eventTimeTrigger.onProcessingTime(watermarkTime, window, triggerContext);
    }

    @Override
    public TriggerResult onEventTime(long watermarkTime, TimeWindow window, TriggerContext triggerContext) {
        LOG.info("Triggering on the event time");
        return eventTimeTrigger.onEventTime(watermarkTime, window, triggerContext);
    }

    @Override
    public void clear(TimeWindow window, TriggerContext triggerContext) throws Exception {
        eventTimeTrigger.clear(window, triggerContext);
    }
}
