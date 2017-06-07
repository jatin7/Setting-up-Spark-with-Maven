import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;


/**
 * Created by kedar on 6/7/17.
 */
public class TransformRDD {
    public static void main(String[] args) {
        JavaSparkContext sc = new JavaSparkContext();
        JavaRDD<String> inputRDD = sc.textFile("log.txt");
        Function<String, Boolean> error = x -> x.contains("error");
        JavaRDD<String> errorsRDD = inputRDD.filter(error);
        System.out.println("number of errors: " + errorsRDD.count());
    }
}
