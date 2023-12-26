package org.example.services;

import java.lang.Double;

import lombok.extern.slf4j.Slf4j;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;

@Slf4j
public class Charter02 {

    private static final String CSV_HEADER = "Order_ID,Product,Quantity_Ordered,Price_Each,Order_Date,Purchase_Address";

    public interface StoreSalesAvgOptions extends PipelineOptions {

        @Description("Path of the file to read from")
        @Default.String("gs://linkedin_learning_56/charter02/input/Sales_April_2019.csv" +
                "")
        String getInputFile();

        void setInputFile(String value);

        @Description("Path of the file to write to")
        @Default.String("gs://linkedin_learning_56/charter02/output/")
        String getOutput();

        void setOutput(String value);
    }



    public void process(String[] args) {

        StoreSalesAvgOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(StoreSalesAvgOptions.class);

        //options.setRunner(DirectRunner.class);

        runProductDetails(options);
    }

    static void runProductDetails(StoreSalesAvgOptions options) {

        Pipeline p = Pipeline.create(options);

        p.apply("ReadLines", TextIO.read().from(options.getInputFile()))
                .apply(ParDo.of(new FilterHeaderFn(CSV_HEADER)))
                .apply("ExtractSalesDetails", ParDo.of(new ExtractSalesDetailsFn()))
                .apply("WriteSalesDetails", TextIO.write().to(options.getOutput())
                        .withNumShards(1) //without it the job will auto split shards
                        .withHeader("Product,Total_Price,Order_Date"));

        p.run();
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
            log.info("processing FilterHeaderFn..".concat(row.toString().substring(1,10)));

            if (!row.isEmpty() && !row.equals(this.header)) {
                c.output(row);
            }
        }
    }

    static class ExtractSalesDetailsFn extends DoFn<String, String> {

        @ProcessElement
        public void processElement(@Element String element, OutputReceiver<String> receiver) {
            String[] field = element.split(",");
            log.info("processing ExtractSalesDetailsFn..");

            String productDetails = field[1] + "," +
                    Double.toString(Integer.parseInt(field[2]) * Double.parseDouble(field[3])) + "," +  field[4];

            receiver.output(productDetails);
        }
    }
}
