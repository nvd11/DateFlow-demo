package org.example;

import lombok.extern.slf4j.Slf4j;
import org.example.config.EnvConfig;
import org.example.services.Charter02;
import org.example.services.ProcessService2;
import org.example.services.ProcessService;
import org.example.services.ProcessService3;

import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;


@Slf4j
public class Main {

    private static String project_id = "jason-hsbc";
    private static String region = "europe-west2";


    public static void main(String[] args) throws UnknownHostException {
        log.info("main()...");

        EnvConfig.envSetup();

        RunCharter02withDirectRunner();

        // new Charter02().process(args);
    }



    private static void RunCharter02withDirectRunner(){
        String args[] = Arrays.asList(
                "--runner=DirectRunner"
                                        ).toArray(new String[1]);
        new Charter02().process(args);
    }
    private static void RunCharter02withDataflow(){
        List<String> list = Arrays.asList(
                "--project=".concat(project_id),
                "--stagingLocation=gs://jason-hsbc-dataflow/staging/",
                "--subnetwork=regions/europe-west2/subnetworks/subnet-west2",
                "--tempLocation=gs://jason-hsbc-dataflow/tmp",
                "--runner=DataflowRunner",
                "--region=".concat(region)
        );

        new Charter02().process(list.toArray(new String[list.size()]));
    }



}