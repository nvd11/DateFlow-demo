package org.example.services;

import java.lang.Double;

import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;

public class Charter02 {

    private static final String CSV_HEADER = "Order_ID,Product,Quantity_Ordered,Price_Each,Order_Date,Purchase_Address";

    public void process(String[] args) {

        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        //options.setRunner(DirectRunner.class);

        runProductDetails(options);
    }

    static void runProductDetails(PipelineOptions options) {

        Pipeline p = Pipeline.create(options);

        p.apply("ReadLines", TextIO.read().from("gs://linkedin_learning_56/charter02/input/Sales_April_2019.csv"))
                .apply(ParDo.of(new FilterHeaderFn(CSV_HEADER)))
                .apply("ExtractSalesDetails", ParDo.of(new ExtractSalesDetailsFn()))
                .apply("WriteSalesDetails", TextIO.write().to("gs://linkedin_learning_56/charter02/output/get_sales_details")
                        .withNumShards(1) //without it the job will auto split shards
                        .withHeader("Product,Total_Price,Order_Date"));

        p.run().waitUntilFinish();
    }

    private static class FilterHeaderFn extends DoFn<String, String> {

        private static final long serialVersionUID = 1L;
        private final String header;

        public FilterHeaderFn(String header) {
            this.header = header;
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            String row = c.element();

            if (!row.isEmpty() && !row.equals(this.header)) {
                c.output(row);
            }
        }
    }

    static class ExtractSalesDetailsFn extends DoFn<String, String> {

        @ProcessElement
        public void processElement(@Element String element, OutputReceiver<String> receiver) {

            String[] field = element.split(",");

            String productDetails = field[1] + "," +
                    Double.toString(Integer.parseInt(field[2]) * Double.parseDouble(field[3])) + "," +  field[4];

            receiver.output(productDetails);
        }
    }
}
