package longRidesExercise;


import com.learn.flink.ridesandfares.ExerciseBase;
import com.learn.flink.ridesandfares.TaxiRide;
import com.learn.flink.ridesandfares.TaxiRideGenerator;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * The "Long Ride Alerts" exercise of the Flink training in the docs.
 *
 * <p>The goal for this exercise is to emit START events for taxi rides that have not been matched
 * by an END event during the first 2 hours of the ride.
 */
public class LongRidesExercise extends ExerciseBase {


    /**
     * Main method.
     *
     * @throws Exception which occurs during job execution.
     */
    public static void main(String[] args) throws Exception {

        // set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(ExerciseBase.parallelism);

        // start the data generator
        DataStream<TaxiRide> rides = env.addSource(rideSourceOrTest(new TaxiRideGenerator()));

        DataStream<TaxiRide> longRides = rides
                .keyBy((TaxiRide ride) -> ride.rideId)
                .process(new MatchFunction());

        printOrTest(longRides);

        env.execute("Long Taxi Rides");
    }

    public static class MatchFunction extends KeyedProcessFunction<Long, TaxiRide, TaxiRide> {

        private ValueState<TaxiRide> rideState;

        @Override
        public void open(Configuration config) throws Exception {
            ValueStateDescriptor<TaxiRide> valueStateDescriptor = new ValueStateDescriptor<>("ride.event", TaxiRide.class);
            rideState = getRuntimeContext().getState(valueStateDescriptor);
        }

        @Override
        public void processElement(TaxiRide ride, Context context, Collector<TaxiRide> out) throws Exception {
            TimerService timerService = context.timerService();
            TaxiRide previousEventRide = rideState.value();

            if (previousEventRide == null) {
                rideState.update(ride);
                if (ride.isStart) {
                    context.timerService().registerEventTimeTimer(getTimerTime(ride));
                }
            } else {
                if (!ride.isStart) {
                    context.timerService().deleteEventTimeTimer(getTimerTime(previousEventRide));
                }
                rideState.clear();
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext context, Collector<TaxiRide> out) throws Exception {
            out.collect(rideState.value());
            rideState.clear();
        }

        private long getTimerTime(TaxiRide ride) {
            return ride.startTime.plusSeconds(120 * 60).toEpochMilli();
        }
    }

}
