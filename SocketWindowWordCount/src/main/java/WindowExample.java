import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class WindowExample {
    public static void main (String[] args) throws Exception{
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final DataStream<String> text = env.socketTextStream("localhost", 9000);

        DataStream<SocketWindowWordCount.WordWithCount> windowCounts = text.flatMap(new FlatMapFunction<String, SocketWindowWordCount.WordWithCount>() {
            @Override
            public void flatMap(String value, Collector<SocketWindowWordCount.WordWithCount> out) throws Exception {
                for (String word: value.split("\\s")) {
                    out.collect(new SocketWindowWordCount.WordWithCount(word, 1L));
                }
            }
        });

        KeyedStream<SocketWindowWordCount.WordWithCount, Tuple> keyValue = windowCounts.keyBy("word");

        // tumbling window : Calculates wordcount for each 15 seconds
        WindowedStream<SocketWindowWordCount.WordWithCount, Tuple, TimeWindow> tumblingWindow = keyValue.timeWindow(Time.seconds(15));

        // sliding window : Calculate wordcount for last 5 seconds
        WindowedStream<SocketWindowWordCount.WordWithCount, Tuple, TimeWindow> slidingWindow = keyValue.timeWindow(Time.seconds(15), Time.seconds(5));

        // count window : Calculate for every 5 records (aggregated records)
        WindowedStream<SocketWindowWordCount.WordWithCount, Tuple, GlobalWindow> countWindow = keyValue.countWindow(5);

        // WITH SUM

        tumblingWindow.sum("count").name("tumblingWindow").print();
//        slidingWindow.sum("count").name("slidingWindow").print();
//        countWindow.sum("count").name("countWindow").print();

        // WITH REDUCE

        tumblingWindow.reduce(new ReduceFunction<SocketWindowWordCount.WordWithCount>() {
            @Override
            public SocketWindowWordCount.WordWithCount reduce(SocketWindowWordCount.WordWithCount value1, SocketWindowWordCount.WordWithCount value2) throws Exception {
                return new SocketWindowWordCount.WordWithCount(value1.word, value1.count + value2.count);
            }
        }).name("tumblingWindow").print();
//
//        slidingWindow.reduce(new ReduceFunction<SocketWindowWordCount.WordWithCount>() {
//            @Override
//            public SocketWindowWordCount.WordWithCount reduce(SocketWindowWordCount.WordWithCount value1, SocketWindowWordCount.WordWithCount value2) throws Exception {
//                return new SocketWindowWordCount.WordWithCount(value1.word, value1.count + value2.count);
//            }
//        }).name("slidingWindow").print();
//
//        countWindow.reduce(new ReduceFunction<SocketWindowWordCount.WordWithCount>() {
//            @Override
//            public SocketWindowWordCount.WordWithCount reduce(SocketWindowWordCount.WordWithCount value1, SocketWindowWordCount.WordWithCount value2) throws Exception {
//                return new SocketWindowWordCount.WordWithCount(value1.word, value1.count + value2.count);
//            }
//        }).name("countWindow").print();

        env.execute("WindowExample");
    }
}
