package com.contactsunny.poc.sparkSqlUdfPoc;

import com.contactsunny.poc.sparkSqlUdfPoc.exceptions.ValidationException;
import org.apache.log4j.Logger;

import java.io.IOException;

public class App {

    public static void main(String[] args) {

        SparkJob sparkJob = new SparkJob(args);
        try {
            sparkJob.startJob();
        } catch (ValidationException | IOException e) {
            e.printStackTrace();
        }

    }
}
