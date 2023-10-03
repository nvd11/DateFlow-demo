
package org.example.services;

import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;

@Slf4j
public class ProcessService3 {

    public void process() {
        log.info("processing3...");

        DataflowPipelineOptions options = PipelineOptionsFactory.fromArgs("").as(DataflowPipelineOptions.class);
        options.setJobName("dataflow-exam3");
        options.setProject("jason-hsbc");
        options.setRegion("europe-west1");
        options.setTempLocation("gs://jason-hsbc-dataflow/tmp");
        options.setSubnetwork("regions/europe-west1/subnetworks/subnet-1");
        options.setNumWorkers(1);
        options.setNumberOfWorkerHarnessThreads(2);

        //options.setGcpCredential(new File(...));
        options.setRunner(DataflowRunner.class);

        log.info(getCurrentAccountName());

        Pipeline pipeline = Pipeline.create(options);

        PCollection<String> message = pipeline.apply("Read Pub/Sub Messages", PubsubIO.readStrings().fromSubscription("projects/jason-hsbc/subscriptions/SubscriptionA1"));

        PCollection<KV<String, String>> combinedMsg = message.apply("Extract", ParDo.of(new ExtractMessageAttributeFn()));

        pipeline.run().waitUntilFinish();

        log.info("processing3... end!");
    }
    private String getDeployedAccount(Credentials credentials) {
        if (credentials instanceof GoogleCredentials) {
            return ((GoogleCredentials) credentials).toString();
        }
        return "Unknown";
    }

    public static String getCurrentAccountName() {
        try {
            GoogleCredentials credentials = GoogleCredentials.getApplicationDefault();
            return credentials.toString();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "Unknown";
    }

    static class ExtractMessageAttributeFn extends DoFn<String, KV<String, String>> {
        @ProcessElement
        public void processElement(ProcessContext processContext){
            String msg = processContext.element();

            log.info("msg is: ".concat(msg));
        }


    }

}
