import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

/**
 * <p>
 *     Given flights to/from Boston do something interesting with it.
 * </p>
 * Created by kedar on 6/12/17.
 */
public class Flights {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("airline").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> inputRDD = sc.textFile(args[0]);
        String city = args[1].toLowerCase();
        Function<String, Boolean> boston = x -> x.toLowerCase().contains(city);
        JavaRDD<String> bostonFlights = inputRDD.filter(boston);
        System.out.println();
        System.out.println();
        System.out.println();
        System.out.println("in 2006, there were " + bostonFlights.count() + " flights to/from: " + city);
        System.out.println();
        System.out.println();
        System.out.println();

        System.out.println("first few flights:");
        bostonFlights.take(10).forEach(System.out::println);
    }
}
