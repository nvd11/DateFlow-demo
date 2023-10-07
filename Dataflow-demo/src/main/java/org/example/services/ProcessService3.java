
package org.example.services;

import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.runners.dataflow.options.DataflowWorkerLoggingOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;
import org.codehaus.jackson.map.util.JSONPObject;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.json.JSONArray;
import org.json.JSONObject;

import java.nio.charset.StandardCharsets;

@Slf4j
public class ProcessService3 {

    private String bucketName = "jason-hsbc-raw";
    private String projectId = "jason-hsbc";

    public void process() {
        log.info("processing3...");

        DataflowPipelineOptions options = PipelineOptionsFactory.fromArgs("").as(DataflowPipelineOptions.class);
        options.setJobName("dataflow-exam3");
        options.setProject(this.projectId);
        options.setRegion("europe-west1");
        options.setTempLocation("gs://jason-hsbc-dataflow/tmp");
        options.setSubnetwork("regions/europe-west1/subnetworks/subnet-1");
        options.setNumWorkers(1);
        options.setDefaultWorkerLogLevel(DataflowWorkerLoggingOptions.Level.INFO);
        options.setNumberOfWorkerHarnessThreads(2);

        //options.setGcpCredential(new File(...));
        options.setRunner(DataflowRunner.class);

        log.info(getCurrentAccountName());

        Pipeline pipeline = Pipeline.create(options);

        /**
         * * The effect of using the @UnknownKeyFor annotation is to tell the Apache Beam framework that the PCollection does not need to be grouped or associated based on a specific key.
         * As a result, Apache Beam can perform more efficient parallel computations and optimizations for operations that do not require key associations.
         *
         * * When the @NonNull annotation is applied to a PCollection element type, it indicates that elements in the PCollection are not allowed to be null.
         * This means that when processing data streams, the Apache Beam framework checks elements for non-nullability and issues warnings or errors at compile time to avoid potential null pointer exceptions.

            the @Initialized annotation informs the compiler that the variable has been properly initialized by marking it at variable declaration time to help detect possible null pointer exceptions
               and improve the reliability and readability of the code. However, it should be noted that the @Initialized annotation is only an auxiliary tool, and correct logic and design still need to be ensured in the actual programming process.

         */
        PCollection<@UnknownKeyFor @NonNull @Initialized PubsubMessage> message = pipeline.apply("Read Pub/Sub Messages", PubsubIO.readMessagesWithAttributesAndMessageId().fromSubscription("projects/jason-hsbc/subscriptions/SubscriptionA1"));

        PCollection<KV<String, String>> combinedMsg = message.apply("Extract", ParDo.of(new ExtractMessageAttributeFn()))
                        .apply("appying windowing", Window.into(FixedWindows.of(Duration.standardMinutes(1))))
                        .apply("Group by fileName", GroupByKey.create())
                                .apply("Combine Message",ParDo.of(new CombinedMessagesFn()));

        combinedMsg.apply("Write to GCS", ParDo.of(new WriteToGCSFn(this.bucketName,this.projectId)));

        // pipeline.run().waitUntilFinish();
        pipeline.run();

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

    static class ExtractMessageAttributeFn extends DoFn<PubsubMessage, KV<String, String>> {
        @ProcessElement
        public void processElement(ProcessContext processContext){
            PubsubMessage msg = processContext.element();

            String messageId = msg.getMessageId();
            log.info("msg id is: ".concat(messageId));

            Instant publishTime = processContext.timestamp();

            String tableName = msg.getAttribute("tableName");
            String transferDate = msg.getAttribute("transferDate");
            Integer recordCount = Integer.valueOf(msg.getAttribute("recordCount"));
            String partialOfMsg = msg.getAttribute("partialOfMsg").replace("/","_");

            log.info("table Name is : ".concat(tableName));

            JSONObject jsonObject = new JSONObject();
            jsonObject.put("messageId", messageId);
            jsonObject.put("timestamp", publishTime);

            // create a json object for attributes
            JSONObject jsonAttributes = new JSONObject();
            jsonAttributes.put("tableName", tableName);
            jsonAttributes.put("transferDate", transferDate);
            jsonAttributes.put("recordCount", recordCount);
            jsonAttributes.put("partialOfMsg", partialOfMsg);

            // create a json array
            JSONArray attributesArray = new JSONArray();
            attributesArray.put(jsonAttributes);

            //put attributes array into jsonObject
            jsonObject.put("attributes", attributesArray);

            //put the data into jsonObject
            String msgData = new String(msg.getPayload(), StandardCharsets.UTF_8);
            jsonObject.put("data", msgData);

            log.info("json object: \n".concat(jsonObject.toString()));

            String fileName = String.format("%s-%s-%s-%s.json", tableName, transferDate, partialOfMsg, recordCount);
            log.info("file name is: ".concat(fileName));

            processContext.output(KV.of(fileName, jsonObject.toString()));

        }
    }

    static class CombinedMessagesFn extends DoFn<KV<String, Iterable<String>>, KV<String, String>> {
        @ProcessElement
        public void processElement(ProcessContext processContext){
            KV<String, Iterable<String>> ele = processContext.element();

            String data = String.format(",%n%s", ele.getValue());
            KV<String, String> kv = KV.of(ele.getKey(), data);

            log.info("combined data is: ".concat(data));
            processContext.output(kv);
            log.info("CombinedMessagesFn done..");
        }

    }

    static class WriteToGCSFn extends DoFn<KV<String, String>, String> {
        String bucketName;
        String projectId;

        public WriteToGCSFn(String bucketName, String projectId){
            this.bucketName = bucketName;
            this.projectId = projectId;
        }

        @ProcessElement
        public void processElement(ProcessContext processContext){
            KV<String, String> ele = processContext.element();
            String outputFilename = ele.getKey();
            String data = ele.getValue();

            //Initialize GCS client
            Storage storage = StorageOptions.newBuilder().setProjectId(this.projectId).build().getService();

            String objectName = "landing/".concat(outputFilename);
            log.info("Object Name: ".concat(objectName));

            Blob blob = storage.get(bucketName, objectName);
            if (blob == null) { //file does not exist
                log.info("creating new files...: ".concat(outputFilename));

                BlobInfo blobInfo = BlobInfo.newBuilder(bucketName, objectName).build();
                storage.create(blobInfo, data.getBytes());

                log.info("created new file..");
            }


            log.info("WriteToGCSFn done!");
        }


    }


}
