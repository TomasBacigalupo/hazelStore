package ar.edu.itba.pod.server;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import com.sun.xml.internal.xsom.impl.scd.Iterators;

import java.io.*;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class Cluster {
    public static void main(String[] args) throws IOException {
        HazelcastInstance hz = Hazelcast.newHazelcastInstance();
        Map<String, String> datos = hz.getMap("materias");
        datos.put("72.42", "POD");
        System.out.println( String.format("%d Datos en el cluster", datos.size() ));
        for (String key : datos.keySet()) {
            System.out.println(String.format( "Datos con key %s= %s", key, datos.get(key)));
        }

        IMap<String, String[]> arboles = hz.getMap("arboles");
        //cargar datos del csv
        BufferedReader br;
        String SEPARATOR = ",";

        ClassLoader classLoader = Cluster.class.getClassLoader();
        InputStream is = classLoader.getResourceAsStream("arbolado-publico-lineal-2017-2018.csv");
        InputStreamReader inputStreamReader = new InputStreamReader(is);
        br = new BufferedReader(inputStreamReader);

        try{
            String line = br.readLine();
            while (null!=line) {
                String [] fields = line.split(SEPARATOR);
                //System.out.println(Arrays.toString(fields));
                line = br.readLine();
                arboles.put(fields[11], fields);
            }

        } catch (Exception e) {
            System.out.println(e.getCause());
        } finally {
            if (null!=br) {
                br.close();
            }
        }

        System.out.println(arboles.size());


        JobTracker t = hz.getJobTracker("tree-count");

        KeyValueSource<String, String[]> source = KeyValueSource.fromMap(arboles);

        //<key in, value in>
        Job<String, String[]> job = t.newJob(source);
        ICompletableFuture<Map<String,Long>> future = job
                .mapper(new TokenizerMapper())
                .reducer(new ArbolesCountReducerFactory())
                .submit();

        // Wait and retrieve the result
        try {
            Map<String, Long> result = future.get();
            result.forEach((k, v)-> System.out.println("arbol: "+ k.toString() +" cantidad: "+v));
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }



    }
}
