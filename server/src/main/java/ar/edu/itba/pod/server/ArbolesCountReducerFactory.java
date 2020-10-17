package ar.edu.itba.pod.server;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

/**
 * ReducerFactory <Key in, Value in, Value out>
 */
public class ArbolesCountReducerFactory implements ReducerFactory <String, Long, Long> {

    @Override
    public Reducer<Long, Long> newReducer(String key) {
        return new ArbolesCountReducer();
    }

    /**
     * Reducer <Value in, Value out>
     */
    private class ArbolesCountReducer extends Reducer <Long, Long>{
        private volatile long sum;

        @Override
        public void beginReduce () {
            sum = 0;
        }
        @Override
        public void reduce(Long value) {
            sum += value;
        }

        @Override
        public Long finalizeReduce() {
            return sum;
        }

    }
}
