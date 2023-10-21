package org.example.services;

import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;

import java.io.PipedInputStream;

@Slf4j
public class Charter02Aggregating {

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

        p.apply("Read Lines from file", TextIO.read().from(pipelineOptions.getInputFile()))
                .apply("Print some info", ParDo.of(new PrintInfoFn()));

        p.run();

    }

    static class PrintInfoFn extends DoFn<String, String> {

        @ProcessElement
        public void processElement(ProcessContext c) {
            String row = c.element();
            log.info("content of this row is".concat(row));
            c.output(row);
        }

    }
}
