package flinkWatermarkDemo;

import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.Window;

public class MyTrigger extends Trigger {
    @Override
    public TriggerResult onElement(Object o, long l, Window window, TriggerContext triggerContext) throws Exception {
        return null;
    }

    @Override
    public TriggerResult onProcessingTime(long l, Window window, TriggerContext triggerContext) throws Exception {
        return null;
    }

    @Override
    public TriggerResult onEventTime(long l, Window window, TriggerContext triggerContext) throws Exception {
        return null;
    }

    @Override
    public void clear(Window window, TriggerContext triggerContext) throws Exception {

    }
}
