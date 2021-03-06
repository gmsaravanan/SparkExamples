package SparkProj;

/**
 * Created by mohasa02 on 11/5/2015.
 */


import org.apache.spark.api.java.function.Function;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;


public final class SparkD {
    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) throws Exception {

        if (args.length < 1) {
            System.err.println("Usage: com.nielsen.perfengg.SparkD <file>");
            System.exit(1);
        }

        SparkConf sparkConf = new SparkConf().setAppName("SparkExamples").setMaster("local[4]")
                                             .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
                                             .set("spark.kryo.registrator", "com.nielsen.perfengg.SparkExRegistrator");

        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        JavaRDD<String> lines = ctx.textFile(args[0], 1).persist(StorageLevel.DISK_ONLY());

        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String s) {
                return Arrays.asList(SPACE.split(s));
            }
        });

        JavaPairRDD<String, Integer> ones = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) {
                return new Tuple2<String, Integer>(s, 1);
            }
        }).persist(StorageLevel.DISK_ONLY());

        System.out.println("Reduce By Key counts");
        JavaPairRDD<String, Integer> counts = ones.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer i1, Integer i2) {
                return i1 + i2;
            }
        }).persist(StorageLevel.DISK_ONLY());
        List<Tuple2<String, Integer>> output = counts.collect();
        for (Tuple2<?,?> tuple : output) {
            System.out.println(tuple._1() + ": " + tuple._2());
        }

        //GroupbyKey

        //AggregateByKey
       ctx.stop();
    }

}
