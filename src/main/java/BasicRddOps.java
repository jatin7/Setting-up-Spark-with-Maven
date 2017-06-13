import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.List;

import static java.util.Arrays.asList;

/**
 * Created by kedar on 6/12/17.
 */
public class BasicRddOps {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("basic").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // map
        JavaRDD<Integer> rdd = sc.parallelize(asList(1, 2, 3, 4));
        rdd.map(x -> x*x).foreach(x1 -> System.out.println(x1));

        // flatmap
        List<Integer> r1 = asList(1, 2, 3);
        List<Integer> r2 = asList(3, 4, 5);
        List<Integer> r3 = asList(5, 6, 7);
        JavaRDD<List<Integer>> lists = sc.parallelize(asList(r1, r2, r3));
        lists.flatMap(list -> list.iterator()).foreach(x -> System.out.println(x));

        // set operations
        JavaRDD<String> list1 = sc.parallelize(asList("foo", "bar", "bar", "quux"));
        JavaRDD<String> list2 = sc.parallelize(asList("baz", "bar", "bar", "quux", "foobar"));
        list1.distinct().foreach(x -> System.out.println(x));
        list2.distinct().foreach(x -> System.out.println(x));
        list1.union(list2).foreach(x -> System.out.println(x));
        list1.intersection(list2).foreach(x -> System.out.println(x));
        list2.intersection(list1).foreach(x -> System.out.println(x));
        list1.subtract(list2).foreach(x -> System.out.println(x));
        list2.subtract(list1).foreach(x -> System.out.println(x));
    }
}
