import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.SparkConf;
import java.util.ArrayList;
import java.util.Set;

/**
 package model;

 /**
 * Created by wushuang on 16-7-11.
 */
public class main {
    public static void main(String[] args){
        SparkConf conf = new SparkConf().setAppName("Simple Application");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        Set<String> paths = Sets.newHashSet();
        paths.add("/home/wushuang/Desktop/*");
        paths.add("/home/wushuang/Downloads/*");


        String[] a = Iterables.toArray(paths,String.class);

        System.out.print(a);

        sqlContext.read().parquet(a);


    }
}
