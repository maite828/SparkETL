package com.spark.etl.workflows.components.transformers
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame
import com.spark.etl.utils.{TableColumnConstants=>TC}
import org.apache.spark.sql.functions._

class StoreSalesTransformer extends TransformTrait {
  override def transform(paramsMap: Map[String, Any], dataFrameMap:
  Map[String, DataFrame]): Map[String, DataFrame] = {

    val log: Logger = Logger.getLogger(this.getClass.getName)

    val salesDF = dataFrameMap("salesDF")
    val storeDF = dataFrameMap("storeDF")

    val salesStrExpr = storeDF.col(TC.strNbr) === salesDF.col(TC.strNbr)

    val result = salesDF.groupBy(col(TC.strNbr))
      .agg(sum(TC.saleAmt).alias(TC.saleAmt))
      .join(storeDF, salesStrExpr, "inner")
      .select(storeDF(TC.strNbr), col(TC.strNm), col(TC.saleAmt))

    result.show(10)


    Map("strSalesDF" -> result)

  }
}
