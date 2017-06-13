import org.apache.spark.*;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;


/**
 * Created by kedar on 6/7/17.
 */
public class TransformRDD {
    public static void main(String[] args) {
	    SparkConf conf = new SparkConf().setAppName("Transformrdd").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> inputRDD = sc.textFile(args[0]);
        Function<String, Boolean> error = x -> x.contains("jpg");
        JavaRDD<String> errorsRDD = inputRDD.filter(error);
        System.out.println();
        System.out.println();
        System.out.println();
        System.out.println("number of lines with 'jpg': " + errorsRDD.count());
        System.out.println();
        System.out.println();
        System.out.println();
    }
}
