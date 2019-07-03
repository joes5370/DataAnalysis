package com.haiteam

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object 문제원형_0702_TEST {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().
      setAppName("DataLoading").
      setMaster("local[*]")
    var sc = new SparkContext(conf)
    val spark = new SQLContext(sc)
    import spark.implicits._

    var salesFile = "pro_actual_sales.csv"
    // 절대경로 입력
    var salesDf =
      spark.read.format("csv").
        option("header", "true").
        option("Delimiter", ",").
        load("C:/spark_orgin_2.2.0/bin/data/" + salesFile)

    // 데이터 확인 (3)
    print(salesDf.show(2))

    var salesColums = salesDf.columns.map(x => {
      x.toLowerCase()
    })
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

    var promotionFile = "pro_promotion.csv"

    var promotionDf =
      spark.read.format("csv").
        option("header", "true").
        option("Delimiter", ",").
        load("C:/spark_orgin_2.2.0/bin/data/" + promotionFile)

    print(promotionDf.show(2))

    var promotionColums = promotionDf.columns.map(x => {
      x.toLowerCase()
    })
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

    var promotionRdd = promotionDf.rdd
    var minPlanWeek = promotionRdd.map(x => {
      x.getString(planweekno).toInt
    }).min()

    var filteredPromotion = promotionRdd.filter(x => {
      var check = false
      var targetWeek = x.getString(targetweekno3)
      var map_price = x.getString(map_priceno)
      if (targetWeek.toInt >= minPlanWeek) {
        check = true
      }
      check
    })

    //디버깅코드 NaN check!!!!!!!!!!!
    //    var processRdd2 = filteredPromotion.groupBy(x => {
    //      (x.getString(productgroup2), x.getString(itemno))
    //    })
    //
    //    var x = processRdd2.filter(x=>{
    //      var chaeck = false
    //      if( (x._1._1 == "PG01") &&
    //        (x._1._2 == "ITEM0466")){
    //        chaeck=true
    //      }
    //      chaeck
    //    }).first()


    var processRdd = filteredPromotion.groupBy(x => {
      (x.getString(productgroup2), x.getString(itemno))
    }).flatMap(x => {
      var key = x._1
      var data = x._2

      var mapPrice = data.map(x => {
        x.getString(map_priceno)
      }).toArray

      var mapSum = data.map(x => {
        x.getString(map_priceno).toDouble
      }).sum

      var count_nz = data.filter(x => {
        var checkValid = false
        if (x.getString(map_priceno).toInt > 0) {
          checkValid = true
        }
        checkValid
      }).size

      var new_mapPrice =
        if (mapPrice.contains("0")) {
          if (count_nz != 0) {
            mapSum / count_nz
          } else {
            0
          }
        } else {
          mapPrice(0)
        }

      var result = data.map(x => {
        var pmap = if (new_mapPrice == 0) {
          0
        } else {
          new_mapPrice.toString.toDouble - x.getString(irno).toDouble
        }
        var pmap10 = if (pmap == 0) {
          0
        } else {
          pmap * 0.9
        }
        var pro_percent = if (pmap10 == 0) {
          0
        } else {
          1 - (pmap10 / new_mapPrice.toString.toDouble)
        }
        var ir = if (new_mapPrice == 0) {
          0
        } else {
          x.getString(irno)
        }

        (x.getString(regionidno),
          x.getString(salesidno),
          x.getString(productgroup2),
          x.getString(itemno),
          x.getString(targetweekno3),
          x.getString(planweekno),
          new_mapPrice.toString.toDouble,
          ir.toString.toDouble,
          pmap,
          math.round(pmap10),
          pro_percent)
      })
      result
    })

    var resultMap = processRdd.map(x => {
      (x._1, x._2, x._3, x._4, x._5, x._6, x._7, x._8, x._9, x._10, x._11)
    })


    var testDf = resultMap.toDF("REGIONSEG", "SALESID", "PRODUCTGROUP", "ITEM", "TARGETWEEK", "PLANWEEK", "MAP_PRICE", "IR", "PMAP", "PMAP10", "PRO_PERCENT")

    testDf.createOrReplaceTempView("filterPromotion")

    salesDf.createOrReplaceTempView("salesData")

    var leftJoinData = spark.sql("""SELECT A.*,B.MAP_PRICE,B.IR,B.PMAP,B.PMAP10,B.PRO_PERCENT FROM salesData A left join filterPromotion B ON A.regionseg1 = B.REGIONSEG AND A.productseg2 = B.PRODUCTGROUP AND A.regionseg2 = B.SALESID AND A.productseg3 = ITEM and A.yearweek = B.TARGETWEEK""")

    leftJoinData.
      coalesce(1). // 파일개수
      write.format("csv"). // 저장포맷
      mode("overwrite"). // 저장모드 append/overwrite
      option("header", "true"). // 헤더 유/무
      save("c:/spark/bin/data/leftJoin.csv") // 저장파일명

    //    testDf.
    //      coalesce(1). // 파일개수
    //      write.format("csv").  // 저장포맷
    //      mode("overwrite"). // 저장모드 append/overwrite
    //      option("header", "true"). // 헤더 유/무
    //      save("c:/spark/bin/data/refine_pro_promotion.csv") // 저장파일명



    var leftJoinFile = "sales_promotion_leftjoinData.csv"

    var leftJoinDf =
      spark.read.format("csv").
        option("header", "true").
        option("Delimiter", ",").
        load("C:/spark_orgin_2.2.0/bin/data/" + leftJoinFile)

    print(leftJoinDf.show(2))

    var leftJoinRdd = leftJoinDf.rdd

    var reftJoinColums = leftJoinDf.columns.map(x => {
      x.toLowerCase()
    })
    var regionidno = reftJoinColums.indexOf("regionseg1")
    var productno = reftJoinColums.indexOf("productseg1")
    var productno2 = reftJoinColums.indexOf("productseg2")
    var regionidno2 = reftJoinColums.indexOf("regionseg2")
    var regionidno3 = reftJoinColums.indexOf("regionseg3")
    var productno3 = reftJoinColums.indexOf("productseg3")
    var yearweekno = reftJoinColums.indexOf("yearweek")
    var yearno = reftJoinColums.indexOf("year")
    var weekno = reftJoinColums.indexOf("week")
    var qtyno = reftJoinColums.indexOf("qty")
    var map_priceno = reftJoinColums.indexOf("map_price")
    var irno = reftJoinColums.indexOf("ir")
    var pmapno = reftJoinColums.indexOf("pmap")
    var pmap10no = reftJoinColums.indexOf("pmap10")
    var pro_percentno = reftJoinColums.indexOf("pro_percent")


    //null값에 0으로 채우기
    var fullDataRdd = leftJoinRdd.map(x => {
      var map_price = x.getString(map_priceno)
      var ir = x.getString(irno)
      var pmap = x.getString(pmapno)
      var pmap10 = x.getString(pmap10no)
      var new_pro_percent = x.getString(pro_percentno)
      var promotionCheck = "Y"

      if (map_price == null) {
        map_price = "0"
      }

      if (ir == null) {
        ir = "0"
      }

      if (pmap == null) {
        pmap = "0"
      }

      if (pmap10 == null) {
        pmap10 = "0"
      }

      if (new_pro_percent == null) {
        new_pro_percent = "0"
      }

      if (map_price == "0"){
        promotionCheck = "N"
      }

      (x.getString(regionidno),
        x.getString(productno2),
        x.getString(regionidno2),
        x.getString(regionidno3),
        x.getString(productno3),
        x.getString(yearweekno),
        x.getString(yearno),
        x.getString(weekno),
        x.getString(qtyno),
        map_price,
        ir,
        pmap,
        pmap10,
        new_pro_percent,
        promotionCheck)
    })

    var testDf = fullDataRdd.toDF("REGIONSEG1", "PRODUCTSEG2", "REGIONSEG2", "REGIONSEG3", "PRODUCTSEG3", "YEARWEEK", "YEAR", "WEEK", "QTY", "MAP_PRICE", "IR", "PMAP", "PMAP10", "PRO_PERCENT","PROMOTION_CHECK")

    testDf.
      coalesce(1). // 파일개수
      write.format("csv"). // 저장포맷
      mode("overwrite"). // 저장모드 append/overwrite
      option("header", "true"). // 헤더 유/무
      save("c:/spark/bin/data/testAll.csv") // 저장파일명


    var testFile = "testAllData.csv"

    var testAllDf =
      spark.read.format("csv").
        option("header", "true").
        option("Delimiter", ",").
        load("C:/spark_orgin_2.2.0/bin/data/" + testFile)

    print(testAllDf.show(2))

    var testColums = testAllDf.columns.map(x => {
      x.toLowerCase()
    })
    var regionidno = testColums.indexOf("regionseg1")
    var productno2 = testColums.indexOf("productseg2")
    var regionidno2 = testColums.indexOf("regionseg2")
    var regionidno3 = testColums.indexOf("regionseg3")
    var productno3 = testColums.indexOf("productseg3")
    var yearweekno = testColums.indexOf("yearweek")
    var yearno = testColums.indexOf("year")
    var weekno = testColums.indexOf("week")
    var qtyno = testColums.indexOf("qty")
    var map_priceno = testColums.indexOf("map_price")
    var irno = testColums.indexOf("ir")
    var pmapno = testColums.indexOf("pmap")
    var pmap10no = testColums.indexOf("pmap10")
    var pro_percentno = testColums.indexOf("pro_percent")
    var promotionCheck = testColums.indexOf("promotion_check")

    var testAllRdd = testAllDf.rdd


    //빠진 주차 구하기
    def postWeek(inputYearWeek: String, gapWeek: Int): String = {
      var currYear = inputYearWeek.substring(0, 4).toInt
      var currWeek = inputYearWeek.substring(4, 6).toInt

      val calendar = Calendar.getInstance();
      calendar.setMinimalDaysInFirstWeek(4);
      calendar.setFirstDayOfWeek(Calendar.MONDAY);

      var dateFormat = new SimpleDateFormat("yyyyMMdd");

      calendar.setTime(dateFormat.parse(currYear + "1231"));

      var maxWeek = calendar.getActualMaximum(Calendar.WEEK_OF_YEAR)

      var conversion = currWeek + gapWeek
      if (maxWeek < conversion) {
        while (maxWeek < conversion) {
          currWeek = conversion - maxWeek
          currYear = currYear + 1
          calendar.setTime(dateFormat.parse(currYear + "1231"));
          maxWeek = calendar.getActualMaximum(Calendar.WEEK_OF_YEAR)
          conversion = currWeek
        }
        return currYear.toString() + "%02d".format((currWeek))
      } else {
        return currYear.toString() + "%02d".format((currWeek + gapWeek))
      } // end of if
    }


    var testAllMap = testAllRdd.groupBy(x => {
      (x.getString(regionidno),
        x.getString(productno2),
        x.getString(regionidno2),
        x.getString(regionidno3),
        x.getString(productno3))
    }).flatMap(x => {
      var key = x._1
      var data = x._2
      var yearweek = data.map(x => x.getString(yearweekno)).toArray.sorted
      var yearweekMax = data.map(x => x.getString(yearweekno)).max
      var yearweekMin = data.map(x => x.getString(yearweekno)).min

      var i = 1
      var tempYearweek = Array(yearweekMin)
      while (tempYearweek.last < yearweekMax) {
        tempYearweek ++= Array(postWeek(yearweekMin.toString, i))
        i = i + 1
      }
      var conversionArray = tempYearweek.diff(yearweek)

      val tmpMap = conversionArray.map(x => {
        val year = x.substring(0, 4)
        val week = x.substring(4, 6)
        val yearweek = year+week
        val qty = 0
        val map_price = 0
        val ir = 0
        val pmap = 0d
        val pmap10 = 0d
        val pro_percent = 0d
        val promotion_check = "N"
        (key._1, key._2, key._3, key._4, key._5, yearweek, year, week, qty.toString, map_price.toString,ir.toString, pmap.toString, pmap10.toString, pro_percent.toString,promotion_check.toString)
      })

      var resultMap = data.map(x=>{
        (x.getString(regionidno),
          x.getString(productno2),
          x.getString(regionidno2),
          x.getString(regionidno3),
          x.getString(productno3),
          x.getString(yearweekno),
          x.getString(yearno),
          x.getString(weekno),
          x.getString(qtyno),
          x.getString(map_priceno),
          x.getString(irno),
          x.getString(pmapno),
          x.getString(pmap10no),
          x.getString(pro_percentno),
          x.getString(promotionCheck)
        )
      })
      tmpMap ++ resultMap
    })

    var sortedData = testAllMap.sortBy(x=>(x._2,x._4,x._5,x._6))


    var sortAllDf = sortedData.toDF("REGIONSEG1", "PRODUCTSEG2", "REGIONSEG2", "REGIONSEG3", "PRODUCTSEG3", "YEARWEEK", "YEAR", "WEEK", "QTY", "MAP_PRICE", "IR", "PMAP", "PMAP10", "PRO_PERCENT","PROMOTION_CHECK")

    sortAllDf.
      coalesce(1). // 파일개수
      write.format("csv"). // 저장포맷
      mode("overwrite"). // 저장모드 append/overwrite
      option("header", "true"). // 헤더 유/무
      save("c:/spark/bin/data/test.csv") // 저장파일명




  }
}
