package com.pjw

import org.apache.spark
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object dataAnalysis_190701 {
  def main(args: Array[String]): Unit = {
    // 1. data 불러와서 rdd 변환
    var rawFile = "pro_promotion.csv"

    var pro_promotion =
      spark.read.format("csv").
        option("header", "true").
        option("Delimiter", ",").
        load("c:/spark/bin/data/" + rawFile)

    var pro_promotion_rdd = pro_promotion.rdd

    //  데이터 불러온 후 컬럼 인덱싱 처리 한 번 해줘
    var pp_columns = pro_promotion.columns
    var pp_regionsegNo = pp_columns.indexOf("regionseg")
    var pp_salesidNo = pp_columns.indexOf("salesid")
    var pp_productgroupNo = pp_columns.indexOf("productgroup")
    var pp_itemNo = pp_columns.indexOf("item")
    var pp_targetweekNo = pp_columns.indexOf("targetweek")
    var pp_planweekNo = pp_columns.indexOf("planweek")
    var pp_map_priceNo = pp_columns.indexOf("map_price")
    var pp_irNo = pp_columns.indexOf("ir")
    var pp_pmapNo = pp_columns.indexOf("pmap")
    var pp_pmap10No = pp_columns.indexOf("pmap10")
    var pp_pro_percentNo = pp_columns.indexOf("pro_percent")


    // 2. planweek 기준 201601전에 있는 targetweek의 데이터를 전부 제거.
    var filtered_pro_promotion_rdd = pro_promotion_rdd.filter(x => {
     ((x.getString(pp_map_priceNo).toInt != 0) && (x.getString(pp_targetweekNo) >= x.getString(pp_planweekNo)))
     })

    filtered_pro_promotion_rdd.collect.foreach(println)

    // 3. pro_actual_sales rdd 생성
    var rawFile1 = "pro_actual_sales.csv"

    var pro_actual_sales =
      spark.read.format("csv").
        option("header", "true").
        option("Delimiter", ",").
        load("c:/spark/bin/data/" + rawFile1)

    var pro_actual_sales_rdd = pro_actual_sales.rdd

    //  데이터 불러온 후 컬럼 인덱싱 처리 한 번 해줘
    var pas_columns = pro_actual_sales.columns
    var pas_regionSeg1No = pas_columns.indexOf("regionSeg1")
    var pas_productSeg1No = pas_columns.indexOf("productSeg1")
    var pas_productSeg2No = pas_columns.indexOf("productSeg2")
    var pas_regionSeg2No = pas_columns.indexOf("regionSeg2")
    var pas_regionSeg3No = pas_columns.indexOf("regionSeg3")
    var pas_productSeg3No = pas_columns.indexOf("productSeg3")
    var pas_yearweekNo = pas_columns.indexOf("yearweek")
    var pas_yearNo = pas_columns.indexOf("year")
    var pas_weekNo = pas_columns.indexOf("week")
    var pas_qtyNo = pas_columns.indexOf("qty")

    // 4. rdd -> df
    var filtered_pro_promotion = spark.createDataFrame(filtered_pro_promotion_rdd,
      StructType(
        Seq(
         StructField("regionseg",StringType),
         StructField("salesid",StringType),
         StructField("productgroup",StringType),
         StructField("item",StringType),
         StructField("targetweek",StringType),
         StructField("planweek",StringType),
         StructField("map_price",StringType),
         StructField("ir",StringType),
         StructField("pmap",StringType),
         StructField("pmap10",StringType),
         StructField("pro_percent",StringType)
        )))

    filtered_pro_promotion.
      coalesce(1).
      write.format("csv").
      mode("overwrite").
      option("header", "true").
      save("c:/filtered_pro_promotion.csv")

    // 5. join with sql
    filtered_pro_promotion.createOrReplaceTempView("filtered_pro_promotion")
    pro_actual_sales.createOrReplaceTempView("pro_actual_sales")

    var joinResultDf = spark.sql(
      """select
        |    a.*,
        |    b.qty
        |from filtered_pro_promotion a
        |left join pro_actual_sales b
        |on a.regionseg = b.regionseg1
        |and a.salesid = b.regionseg2
        |and a.productgroup = b.productseg2
        |and a.item = b.productseg3
        |and a.TARGETWEEK = b.yearweek
      """)

    joinResultDf.fisrt

    joinResultDf.
      coalesce(1).
      write.format("csv").
      mode("overwrite").
      option("header", "true").
      save("c:/reftJoinResultDf.csv")

    // 결론. 실패 : join을 반대로 하여 qty의 손실이 생김
  }
}
