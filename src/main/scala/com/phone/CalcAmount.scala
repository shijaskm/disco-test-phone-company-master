package com.phone
import org.apache.spark.sql.expressions.Window
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.sql.SparkSession
import spark.implicits._


object CalcAmount extends App {

def CalcAmt(args: Array[String]) {
        //val filepath = args(0)
        val filepath = "/user/hadoop/calls.txt" // hdfs location
        val conf = new SparkConf().setAppName("CalcAmount").setMaster("local")
        val sc = new SparkContext(conf)

		val data = sc.textFile(filepath).map(line => line.split(" ")).map(userRecord => (userRecord(0), userRecord(1), userRecord(2)))
 
 
        val df = data.toDF


        val df1 = df.withColumn("custid", col("_1")).withColumn("number", col("_2")).withColumn("dur", col("_3")).drop("_1","_2","_3")

       val df2=df1.withColumn("duration",substring(col("dur"),1,2).cast("Int")*60*60+substring(col("dur"),4,2).cast("Int")*60+substring(col("dur"),7,2).cast("Int")).drop("dur")

       val dataDur = df2.withColumn("amount",   when(col("duration") > 180, (col("duration")-180)*0.03+180*0.05).otherwise(col("duration")*0.05))


      val rankByGrpName = Window.partitionBy(col("custid")).orderBy(col("amount").desc)
 
      val dataFinal = dataDur.withColumn("rank",rank over rankByGrpName).filter(col("rank") !== 1).drop("rank")  
 

      dataFinal.groupBy("custid").agg(sum("amount")).show()
		
		
        context.stop()
  }



}
