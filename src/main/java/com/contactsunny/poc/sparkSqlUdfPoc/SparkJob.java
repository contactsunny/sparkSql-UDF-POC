package com.contactsunny.poc.sparkSqlUdfPoc;

import com.contactsunny.poc.sparkSqlUdfPoc.domain.FileInputLine;
import com.contactsunny.poc.sparkSqlUdfPoc.exceptions.ValidationException;
import com.contactsunny.poc.sparkSqlUdfPoc.utils.FileUtil;
import com.contactsunny.poc.sparkSqlUdfPoc.utils.UDFUtil;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import static com.contactsunny.poc.sparkSqlUdfPoc.config.CustomConstants.*;
import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;

class SparkJob {

    private String[] args;

    private final Logger logger = Logger.getLogger(SparkJob.class);

    private String sparkMaster, inputFilePath;

    private JavaSparkContext javaSparkContext;
    private SQLContext sqlContext;
    private SparkSession sparkSession;

    private UDFUtil udfUtil;
    private FileUtil fileUtil;

    SparkJob(String[] _args) {
        this.args = _args;
    }

    void startJob() throws ValidationException, IOException {

        /*
        Validate to check if we have all the required arguments.
         */
        validateArguments();

        /*
        Load all the properties from the .properties file and initialize the instance variables.
         */
        loadProperties();

        /*
        Register the required UDFs.
         */
        registerUdfs();

        /*
        Get the dataset from the input file.
         */
        Dataset<FileInputLine> inputFileDataset = fileUtil.getDatasetFromFile(inputFilePath);

        inputFileDataset.show();

        Dataset<Row> doubledColumnDataset = inputFileDataset.withColumn(DOUBLED_COLUMN_NAME,
                callUDF(COLUMN_DOUBLE_UDF_NAME, col(NUMBER_COLUMN_NAME)));

        doubledColumnDataset.show();

        Dataset<Row> upperCaseColumnDataset = doubledColumnDataset.withColumn(UPPSERCASE_NAME_COLUMN_NAME,
                callUDF(COLUMN_UPPERCASE_UDF_NAME, col(NAME_COLUMN_NAME)));

        upperCaseColumnDataset.show();
    }

    private void loadProperties() throws IOException {
        Properties properties = new Properties();
        String propFileName = "application.properties";

        InputStream inputStream = App.class.getClassLoader().getResourceAsStream(propFileName);

        try {
            properties.load(inputStream);
        } catch (IOException e) {
            logger.error(e.getMessage());
            throw e;
        }

        initialize(properties);
    }

    private void registerUdfs() {

        this.udfUtil.registerColumnDoubleUdf();
        this.udfUtil.registerColumnUppercaseUdf();
    }

    private void initialize(Properties properties) {

        sparkMaster = properties.getProperty("spark.master");

        javaSparkContext = createJavaSparkContext();
        sqlContext = new SQLContext(javaSparkContext);
        sparkSession = sqlContext.sparkSession();

        inputFilePath = this.args[0];

        udfUtil = new UDFUtil(sqlContext);
        fileUtil = new FileUtil(sparkSession);
    }

    private void validateArguments() throws ValidationException {

        if (args.length < 1) {
            logger.error("Invalid arguments.");
            logger.error("1. Input file path.");
            logger.error("Example: java -jar <jarFileName.jar> /path/to/input/file");

            throw new ValidationException("Invalid arguments, check help text for instructions.");
        }
    }

    private JavaSparkContext createJavaSparkContext() {

        /* Create the SparkSession.
         * If config arguments are passed from the command line using --conf,
         * parse args for the values to set.
         */

        SparkConf conf = new SparkConf().setAppName("SparkSql-UDF-POC").setMaster(sparkMaster);

        return new JavaSparkContext(conf);
    }
}
