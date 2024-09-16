package org.example.services;

import com.google.cloud.bigquery.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.runners.direct.DirectOptions;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Wait;
import org.apache.beam.sdk.values.PCollection;

import java.io.IOException;
import java.io.Serializable;
import java.io.StringReader;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.IntStream;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

@Slf4j
public class FileToBQ {

    /**
     * bq rm -f -t DS1.SALES_DETAILS
     * <p>
     * below is the bq command to create the table
     * <p>
     * bq mk --table \
     * --project_id=jason-hsbc \
     * --dataset_id=DS1 \
     * SALES_DETAILS \
     * Order_ID:STRING,Product:STRING,Quantity_Ordered:INTEGER,Price_Each:FLOAT,Order_Date:String,Purchase_Address:STRING
     */

    private static final String CSV_HEADER = "Order_ID,Product,Quantity_Ordered,Price_Each,Order_Date,Purchase_Address";
    private static final String BQ_PROJECT_ID = "jason-hsbc";
    private static final String BQ_DS_ID = "DS1";

    //private final BigQuery BQ_CLIENT = BigQueryOptions.newBuilder().setProjectId(BQ_PROJECT_ID).build().getService();

    public interface MyPipelineOptions extends PipelineOptions {
        @Description("Path of the file to read from")
        @Default.String("gs://linkedin_learning_56/charter02/input/Sales_April_2019.csv")
        /**
         * The ValueProvider interface in Apache Beam is used to provide dynamic values at runtime.
         * It allows you to retrieve parameter values from external sources during pipeline execution,
         * rather than hardcoding them during pipeline compilation.
         */
        ValueProvider<String> getInputFile();

        void setInputFile(ValueProvider<String> value);


        @Description("Path of the file to write to")
        @Default.String("SALES_DETAILS")
        ValueProvider<String> getTargetTableName();

        void setTargetTableName(ValueProvider<String> value);


    }

    public void process(String[] args) {

        // create Pipeline Options based on the parameters
        MyPipelineOptions pipelineOptions =
                PipelineOptionsFactory.fromArgs(args).withValidation().as(MyPipelineOptions.class);

        // Let the local machine also simulate single worker execution
        if (pipelineOptions.getRunner().equals(DirectRunner.class)) {
            // pipelineOptions.as(DirectOptions.class).setTargetParallelism(1);
        }

        // Create pipeline based on the options
        Pipeline p = Pipeline.create(pipelineOptions);


        PCollection<String> input = p.apply("Read Lines from file", TextIO.read().from(pipelineOptions.getInputFile()));

        PCollection<String> allLines = input
                .apply("Remove header", ParDo.of(new FilterOutHeader(CSV_HEADER)))
                .apply("Print some info1", ParDo.of(new PrintInfoFn1()));

        PCollection<String> processedLines = allLines
                .apply("wait..", Wait.on(allLines))
                .apply("Print some info2", ParDo.of(new PrintInfoFn2()));

        processedLines.apply("Write TO BQ", ParDo.of(new WriteToBQ(pipelineOptions.getTargetTableName().get())));

        p.run();

    }

    static class PrintInfoFn1 extends DoFn<String, String> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            String row = c.element();
            log.info("print some info1: content of this row is".concat(row));
            c.output(row);
        }

    }

    static class FilterOutHeader extends DoFn<String, String> {

        private final String header;

        public FilterOutHeader(String header) {
            this.header = header;
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            String row = c.element();
            if (!row.isEmpty() && !row.equals(this.header)) {
                c.output(row);
            } else {
                log.info("Removed Header: ".concat(row));
            }
        }

    }

    static class PrintInfoFn2 extends DoFn<String, String> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            String row = c.element();
            log.info("print some info2: content of this row is".concat(row));
            c.output(row);
        }
    }

    static class WriteToBQ extends DoFn<String, String> {

        private final String tableName;
        private transient BigQuery bigQuery;
        private transient List<Map<String, Object>> batchRows;
        private final int batchSize = 20;
        private final TableId table;

        public WriteToBQ(String tableName) {
            this.tableName = tableName;
            table = TableId.of(BQ_PROJECT_ID, BQ_DS_ID, tableName);
        }

        /**
         * we cannot create the common db connection obj outside the DoFn , to avoid below exception
         * Caused by: java.io.NotSerializableException: com.google.cloud.bigquery.BigQueryImpl
         */
        @Setup
        public void setup() {
            // create BQ for every worker
            log.info("setting up bq client......!!!!!");
            bigQuery = BigQueryOptions.getDefaultInstance().getService();
            batchRows = new Vector<>();

        }

        @ProcessElement
        public void processElement(@Element String row, OutputReceiver<String> receiver) {
            log.info("WriteToBQ - print some info3: content of this row is".concat(row));
            Map<String, Object> rowData = builtRowHashMap(row);
            batchRows.add(rowData);
            if (batchRows.size() >= batchSize) {
                executeBatchInsert();
            }
        }

        private void executeBatchInsert() {
            //TableId table = TableId.of(BQ_PROJECT_ID, BQ_DS_ID, tableName);
            InsertAllRequest.Builder requestBuilder = InsertAllRequest.newBuilder(table);

            for (Map<String, Object> rowData : batchRows) {
                requestBuilder.addRow(rowData);
            }

            InsertAllResponse response = bigQuery.insertAll(requestBuilder.build());

            if (response.hasErrors()) {
                // 处理插入错误
                log.error("Error occurred while writing to BigQuery: " + response.getInsertErrors());
            } else {
                // 数据行成功写入
                log.info("Data rows written to BigQuery successfully");
            }

            batchRows.clear();
        }

        @FinishBundle
        public void finishBundle() {
            if (!batchRows.isEmpty()) {
                executeBatchInsert();
            }
        }

        private Map<String, Object> builtRowHashMap(String row) {
            Map<String, Object> rowData = new HashMap<>();
            String[] strArr = splitCSVRow(row);
            String[] headerArr = CSV_HEADER.split(",");

            IntStream.range(0, strArr.length)
                    .forEach(i -> rowData.put(headerArr[i], strArr[i]));
            return rowData;
        }

        public static String[] splitCSVRow(String rowData) {

            CSVParser parser = null;
            try {
                parser = CSVParser.parse(new StringReader(rowData), CSVFormat.DEFAULT);
            } catch (IOException e) {
                log.error("failed convert row data to csv, row data:".concat(rowData), e);
                throw new RuntimeException(e);
            }
            CSVRecord record = parser.iterator().next();

            List<String> dataList = new ArrayList<>();
            for (String field : record) {
                dataList.add(field);
            }

            return dataList.toArray(new String[0]);
        }

    }
}
