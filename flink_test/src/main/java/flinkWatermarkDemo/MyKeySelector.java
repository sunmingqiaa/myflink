package flinkWatermarkDemo;

import org.apache.flink.api.java.functions.KeySelector;

public class MyKeySelector implements KeySelector {
    @Override
    public Object getKey(Object o) throws Exception {
        return null;
    }
}
