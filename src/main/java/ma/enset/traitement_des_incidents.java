package ma.enset;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class traitement_des_incidents {
    public static void main(String[] args) {
        SparkSession ss = SparkSession.builder().appName("traitement_des_incidents")
                .master("local[*]").getOrCreate();

        Dataset<Row> df = ss.read().option("header", true).option("inferSchema", "true").csv("incidents.csv");
        df.createOrReplaceTempView("incidents");

        Dataset<Row> dfSQl = ss.sql("SELECT service,count(*) as nbre_incidents" +
                " FROM incidents GROUP BY service");
        dfSQl.show();

        Dataset<Row> dfanne = ss.sql("SELECT YEAR(TO_DATE(date)) AS annee, COUNT(*) AS nbre_incidents " +
                "FROM incidents GROUP BY annee ORDER BY nbre_incidents DESC LIMIT 2");
        dfanne.show();
    }
}
