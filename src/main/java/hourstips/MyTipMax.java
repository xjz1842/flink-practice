package hourstips;

import com.learn.flink.ridesandfares.TaxiFare;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class MyTipMax extends ProcessWindowFunction<TaxiFare,                  // 输入类型
        Tuple3<Long, Long, Float>,  // 输出类型
        Long,                       // 键类型
        TimeWindow> {
    @Override
    public void process(
            Long key,
            Context context,
            Iterable<TaxiFare> events,
            Collector<Tuple3<Long, Long, Float>> out) {

        float total = 0;
        for(TaxiFare taxiFare : events){
            total = taxiFare.tip;
        }
        out.collect(Tuple3.of(key, context.window().getEnd(), total));
    }
}
