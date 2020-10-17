package ar.edu.itba.pod.server;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

import java.util.Arrays;
import java.util.StringTokenizer;

/**
 * Mapper<Key in, Value in, Key out, Value out>
 */
public class TokenizerMapper implements Mapper<String,String[],String,Long> {
    private static final Long ONE = 1L;
    @Override
    public void map(String s, String[] strings, Context<String, Long> context) {
        StringTokenizer tokenizer = new StringTokenizer(s);
        while (tokenizer.hasMoreElements()){
            context.emit(tokenizer.nextToken(), ONE);
        }
    }
}
