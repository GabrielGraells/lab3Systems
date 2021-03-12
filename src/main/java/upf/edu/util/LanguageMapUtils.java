package upf.edu.util;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.util.Arrays;

public class LanguageMapUtils {

    public static JavaPairRDD<String, String> buildLanguageMap(JavaRDD<String> lines) {

        JavaPairRDD<String, String>  languageMap= lines
                .map(s -> Arrays.asList(s.split("\t")))
                .filter(list -> (!list.get(1).isEmpty() && !list.get(2).isEmpty()))
                .mapToPair(line -> new Tuple2<String, String>(line.get(1), line.get(2)));

        return languageMap;// IMPLEMENT ME
    }
}
