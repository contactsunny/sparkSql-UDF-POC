package com.contactsunny.poc.sparkSqlUdfPoc.utils;

import com.contactsunny.poc.sparkSqlUdfPoc.domain.FileInputLine;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

public class FileUtil {

    private SparkSession sparkSession;

    public FileUtil(SparkSession _sparkSession) {
        this.sparkSession = _sparkSession;
    }

    public Dataset<FileInputLine> getDatasetFromFile(String filePath) {

        Dataset<FileInputLine> fileDataSet = this.sparkSession.read().option("header", "true").csv(filePath)
                .as(Encoders.bean(FileInputLine.class));

        return fileDataSet;
    }
}
