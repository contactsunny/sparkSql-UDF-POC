package com.contactsunny.poc.sparkSqlUdfPoc.utils;

import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF4;
import org.apache.spark.sql.types.DataTypes;

import static com.contactsunny.poc.sparkSqlUdfPoc.config.CustomConstants.COLUMN_DOUBLE_UDF_NAME;
import static com.contactsunny.poc.sparkSqlUdfPoc.config.CustomConstants.COLUMN_UPPERCASE_UDF_NAME;

public class UDFUtil {

    private SQLContext sqlContext;

    public UDFUtil(SQLContext _sqlContext) {
        this.sqlContext = _sqlContext;
    }

    public void registerColumnDoubleUdf() {

        this.sqlContext.udf().register(COLUMN_DOUBLE_UDF_NAME, (UDF1<String, Integer>)
            (columnValue) -> {

                return Integer.parseInt(columnValue) * 2;

            }, DataTypes.IntegerType);
    }

    public void registerColumnUppercaseUdf() {

        this.sqlContext.udf().register(COLUMN_UPPERCASE_UDF_NAME, (UDF1<String, String>)
            (columnValue) -> {

                return columnValue.toUpperCase();

            }, DataTypes.StringType);
    }
}
