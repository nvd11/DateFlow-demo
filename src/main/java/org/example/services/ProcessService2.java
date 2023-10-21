
package org.example.services;

import lombok.extern.slf4j.Slf4j;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;


@Slf4j
public class ProcessService2 {


    public interface PubSubToGcsOptions extends PipelineOptions, StreamingOptions {
        @Description("The Cloud Pub/Sub topic to read from.")
        @Validation.Required
        String getInputTopic();

        void setInputTopic(String value);

        @Description("Output file's window size in number of minutes.")
        @Default.Integer(1)
        Integer getWindowSize();

        void setWindowSize(Integer value);

        @Description("Path of the output file including its filename prefix.")
        @Validation.Required
        String getOutput();

        void setOutput(String value);
    }

    public void process() {
        log.info("processing2...");

        String[] args = {"--project=jason-hsbc", "--region=europe-west2",
                "--inputTopic=projects/jason-hsbc/topics/TopicA", "--windowSize=1", "--output=gs://jason-hsbc-dataflow/tmp", "--runner=DataflowRunner"};

        int numShards = 1;
        PubSubToGcsOptions options =
                PipelineOptionsFactory.fromArgs(args).withValidation().as(PubSubToGcsOptions.class);
        options.setStreaming(true);
        options.setTempLocation("gs://jason-hsbc-dataflow/tmp");
        Pipeline pipeline = Pipeline.create(options);
        pipeline
                // 1) Read string messages from a Pub/Sub topic.
                .apply("Read PubSub Messages", PubsubIO.readStrings().fromTopic(options.getInputTopic()))
                // 2) Group the messages into fixed-sized minute intervals.
                .apply(Window.into(FixedWindows.of(Duration.standardMinutes(options.getWindowSize()))));

        // Execute the pipeline and wait until it finishes running.
        pipeline.run().waitUntilFinish();

        log.info("processing2... end!");
    }


}
