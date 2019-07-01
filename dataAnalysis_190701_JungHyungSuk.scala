package com.haiteam

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object dataAnalysis_190701_JungHyungSuk {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().
      setAppName("DataLoading").
      setMaster("local[*]")
    var sc = new SparkContext(conf)
    val spark = new SQLContext(sc)

    import spark.implicits._

    //sales파일 불러오는 코드
    var salesFile = "pro_actual_sales.csv"
    var salesDf=
      spark.read.format("csv").
        option("header","true").
        option("Delimiter",",").
        load("C:/spark_orgin_2.2.0/bin/data/"+salesFile)


    print(salesDf.show(2))

    //컬럼값 소문자로 변환 후 index 정의
    var salesColums = salesDf.columns.map(x=>{x.toLowerCase()})
    var regionidno1 = salesColums.indexOf("regionseg1")
    var productno1 = salesColums.indexOf("productseg1")
    var productno2 = salesColums.indexOf("productseg2")
    var regionidno2 = salesColums.indexOf("regionseg2")
    var regionidno3 = salesColums.indexOf("regionseg3")
    var productno3 = salesColums.indexOf("productseg3")
    var yearweekno = salesColums.indexOf("yearweek")
    var yearno = salesColums.indexOf("year")
    var weekno = salesColums.indexOf("week")
    var qtyno = salesColums.indexOf("qty")

    //promotion파일 불러오기
    var promotionFile = "pro_promotion.csv"

    var promotionDf=
      spark.read.format("csv").
        option("header","true").
        option("Delimiter",",").
        load("C:/spark_orgin_2.2.0/bin/data/"+promotionFile)

    print(promotionDf.show(2))

    var promotionColums = promotionDf.columns.map(x=>{x.toLowerCase()})
    var regionidno = promotionColums.indexOf("regionseg")
    var salesidno = promotionColums.indexOf("salesid")
    var productgroup2 = promotionColums.indexOf("productgroup")
    var itemno = promotionColums.indexOf("item")
    var targetweekno3 = promotionColums.indexOf("targetweek")
    var planweekno = promotionColums.indexOf("planwee")
    var map_priceno = promotionColums.indexOf("map_price")
    var irno = promotionColums.indexOf("ir")
    var pmapno = promotionColums.indexOf("pmap")
    var pmap10no = promotionColums.indexOf("pmap10")
    var pro_percentno = promotionColums.indexOf("pro_percent")

    //rdd변환
    var salesRdd = salesDf.rdd
    var promotionRdd = promotionDf.rdd

    //planWeek의 최소값을 추출
    var minPlanWeek = promotionRdd.map(x=>{x.getString(planweekno).toInt}).min()


    //targetWeek가 planWeek의 최솟값보다 크거나 같은 값과 map_price가 0이 아닌값 추출
    var filteredPromotion = promotionRdd.filter(x=>{
      var check = false
      var targetWeek = x.getString(targetweekno3)
      var map_price = x.getString(map_priceno)
      if(targetWeek.toInt >= minPlanWeek && map_price.toInt != 0){
        check = true
      }
      check
    })

    //데이터프레임으로 변환하기 위해서는 filteredPromotion의 Row로 정렬되있는 것을 Map을 통해 풀어주어야 한다.
    var filterMap = filteredPromotion.map(x=>{
      (x.getString(regionidno),
        x.getString(salesidno),
        x.getString(productgroup2),
        x.getString(itemno),
        x.getString(targetweekno3),
        x.getString(planweekno),
        x.getString(map_priceno),
        x.getString(irno),
        x.getString(pmapno),
        x.getString(pmap10no),
        x.getString(pro_percentno))
    })

    //DF 컬럼 정의
    var testDf = filterMap.toDF("REGIONSEG","SALESID","PRODUCTGROUP","ITEM","TARGETWEEK","PLANWEEK","MAP_PRICE","IR","PMAP","PMAP10","PRO_PERCENT")

    //테이블 생성
    testDf.createOrReplaceTempView("filterPromotion")

    salesDf.createOrReplaceTempView("salesData")

    //spark sql을 통해 leftJoin 실행
    var leftJoinData = spark.sql("""SELECT A.*,B.MAP_PRICE,B.IR,B.PMAP,B.PMAP10,B.PRO_PERCENT FROM salesData A left join filterPromotion B ON A.regionseg1 = B.REGIONSEG AND A.productseg2 = B.PRODUCTGROUP AND A.regionseg2 = B.SALESID AND A.productseg3 = ITEM and A.yearweek = B.TARGETWEEK""")

    //완성된 dataFrame을 해당 경로에 csv로 던진다.
    leftJoinData.
      coalesce(1).
      write.format("csv").
      mode("overwrite").
      option("header", "true").
      save("c:/spark/bin/data/pro_actual_sales_season.csv")

  }
}
