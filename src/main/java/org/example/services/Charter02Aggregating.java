package org.example.services;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.TableId;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;

import java.io.PipedInputStream;
import java.util.Arrays;
import java.util.List;

@Slf4j
public class Charter02Aggregating {

    private static final String CSV_HEADER = "Order_ID,Product,Quantity_Ordered,Price_Each,Order_Date,Purchase_Address";
    private static final String BQ_PROJECT_ID = "jason-hsbc";
    private static final String BQ_DS_ID = "DS1";

    public interface AggregationPipelineOptions extends PipelineOptions {
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
        @Default.String("gs://linkedin_learning_56/charter02/output/output_sales_details")
        ValueProvider<String> getOutputFile();
        void setOutputFile(ValueProvider<String> value);


    }

    public void process(String[] args){

        // create Pipeline Options based on the parameters
        AggregationPipelineOptions pipelineOptions =
                PipelineOptionsFactory.fromArgs(args).withValidation().as(AggregationPipelineOptions.class);

        // Create pipeline based on the options
        Pipeline p = Pipeline.create(pipelineOptions);

        PCollection<String> input = p.apply("Read Lines from file", TextIO.read().from(pipelineOptions.getInputFile()));

        PCollection<String> allLines = input
                .apply("Remove header", ParDo.of(new FilterOutHeader(CSV_HEADER)))
                .apply("Print some info1", ParDo.of(new PrintInfoFn1()));

        PCollection<String> processedLines = allLines
                .apply("wait..", Wait.on(allLines))
                .apply("Print some info2", ParDo.of(new PrintInfoFn2()))
                ;

        processedLines.apply("Print some info3", ParDo.of(new writeToBQ()));

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
        public FilterOutHeader(String header){
            this.header = header;
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            String row = c.element();
            if (!row.isEmpty() && !row.equals(this.header)) {
                c.output(row);
            }else {
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

    static class writeToBQ extends DoFn<String, String> {
        @ProcessElement
        public void processElement(@Element String row, OutputReceiver<String> receiver) {
            log.info("print some info3: content of this row is".concat(row));
            receiver.output(row);
        }
    }
}
