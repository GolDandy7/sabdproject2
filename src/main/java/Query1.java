import entity.NYBusLog;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

public class Query1 {

    private static final int WINDOW_SIZE = 1;      // hours
    //private static final int WINDOW_SIZE = 24 * 7;  // hours

    public static void run(DataStream<NYBusLog> stream) throws Exception {
        DataStream<NYBusLog> timestampedAndWatermarked = stream
                .assignTimestampsAndWatermarks
                        (new BoundedOutOfOrdernessTimestampExtractor<NYBusLog>(Time.seconds(1)) {
                            @Override
                            public long extractTimestamp(NYBusLog logIntegerTuple2) {
                                return logIntegerTuple2.getDateOccuredOn();
                            }
                        });

        // somma del delay per boro
        DataStream<String> chart = timestampedAndWatermarked
                .keyBy(NYBusLog::getBoro)
                .timeWindow(Time.hours(WINDOW_SIZE))
                .aggregate(new SumAggregator(), new KeyBinder())
                .timeWindowAll(Time.hours(WINDOW_SIZE))
                .process(new ChartProcessAllWindowFunction());

        chart.print();
        //forse vuole il TextoOutputFormat
        chart.writeAsText(String.format("output"+ "querypinco_%d.out",WINDOW_SIZE),
                FileSystem.WriteMode.OVERWRITE).setParallelism(1);
    }

    private static class SumAggregator implements AggregateFunction<NYBusLog, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(NYBusLog value, Long accumulator) {
            return accumulator + value.getDelay();
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a + b;
        }
    }

    private static class KeyBinder
            extends ProcessWindowFunction<Long, Tuple2<String, Long>, String, TimeWindow> {

        @Override
        public void process(String key,
                            Context context,
                            Iterable<Long> counts,
                            Collector<Tuple2<String, Long>> out) {
            Long count = counts.iterator().next();
            out.collect(new Tuple2<>(key, count));
        }
    }

    private static class ChartProcessAllWindowFunction
            extends ProcessAllWindowFunction<Tuple2<String, Long>, String, TimeWindow> {

        @Override
        public void process(Context context, Iterable<Tuple2<String, Long>> iterable, Collector<String> collector) {
            List<Tuple2<String, Long>> counts = new ArrayList<>();
            for (Tuple2<String, Long> t : iterable)
                counts.add(t);
            counts.sort((a, b) -> new Long(b.f1 - a.f1).intValue());

            /*
            LocalDateTime startDate = LocalDateTime.ofEpochSecond(
                    context.window().getStart() / 1000, 0, ZoneOffset.UTC);
            LocalDateTime endDate = LocalDateTime.ofEpochSecond(
                    context.window().getEnd() / 1000, 0, ZoneOffset.UTC);
            StringBuilder result = new StringBuilder(startDate.toString() + " " + endDate.toString() + ": ");
             */
            StringBuilder result = new StringBuilder(Long.toString(context.window().getStart() / 1000));

            int size = counts.size();
            for (int i = 0; i < 3 && i < size; i++)
                result.append(", ").append(counts.get(i).f0).append(", ").append(counts.get(i).f1);

            collector.collect(result.toString());
        }

    }
}
