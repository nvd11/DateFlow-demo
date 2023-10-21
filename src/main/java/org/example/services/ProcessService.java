package org.example.services;

import lombok.extern.slf4j.Slf4j;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.PCollection;

import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.example.util.DateTimeUtil;
import org.joda.time.Duration;

import java.time.LocalDateTime;

@Slf4j
public class ProcessService {
    public void process(){
        log.info("processing...");



        DataflowPipelineOptions options = PipelineOptionsFactory.fromArgs("").as(DataflowPipelineOptions.class);
        options.setJobName("dataflow-exam2");
        options.setProject("jason-hsbc");
        options.setRegion("europe-west1");
        options.setTempLocation("gs://jason-hsbc-dataflow/tmp");
        options.setSubnetwork("regions/europe-west1/subnetworks/subnet-1");
        options.setNumWorkers(1);
        options.setNumberOfWorkerHarnessThreads(2);

        //options.setGcpCredential(new File(...));
        options.setRunner(DataflowRunner.class);

        Pipeline pipeline = Pipeline.create(options);

        PCollection<String> messages = pipeline
                .apply("Read Pub/Sub Messages", PubsubIO.readStrings().fromSubscription("projects/jason-hsbc/subscriptions/SubscriptionA1"))
                .apply("Window into GlobalWindow", Window.<String>into(new GlobalWindows())
                        .triggering(Repeatedly.forever(AfterPane.elementCountAtLeast(1)))
                        .withAllowedLateness(Duration.ZERO)
                        .accumulatingFiredPanes());;
        // 处理消息并ACK
        // 将消息保存到GCS Bucket中
        messages.apply("Save to GCS",
                TextIO.write().withWindowedWrites().to("gs://jason-hsbc-raw/raw/").withoutSharding()
                        .withNumShards(1).withSuffix(DateTimeUtil.getLocalDTStr(LocalDateTime.now()).concat(".txt"))
                        );



        log.info("message: ".concat(messages.toString()));

        log.info("processing1... end!");
        pipeline.run();
        System.out.println("processing1... end!");

    }



    // 定义文件命名策略
    private static FileIO.Write.FileNaming fileNaming() {
        return new FileIO.Write.FileNaming() {
            @Override
            public String getFilename(BoundedWindow window, PaneInfo pane, int numShards, int shardIndex, Compression compression) {
                return "testfile.json";
            }

        };
    }

}
