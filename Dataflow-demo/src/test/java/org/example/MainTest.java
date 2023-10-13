package org.example;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;


@Slf4j
class MainTest {

    @Test
    void testMainWithDirectJava() {
        log.info("main testing..");

        List<String> list= Arrays.asList(
                "--runner=DirectRunner",
                "--inputFile=gs://linkedin_learning_56/charter02/input/Sales_April_2019.csv"
        );
        Main.main(list.toArray(new String[list.size()]));

        Assertions.assertTrue(true);

    }

    @Test
    void testMainWithDataflowRunner() {
        log.info("main testing..");


        List<String> list= Arrays.asList(
                "--project=jason-hsbc",
                "--inputFile=gs://linkedin_learning_56/charter02/input/Sales_April_2019.csv",
                "--stagingLocation=gs://jason-hsbc-dataflow/staging/",
                "--subnetwork=regions/europe-west1/subnetworks/subnet-1",
                "--tempLocation=gs://jason-hsbc-dataflow/tmp",
                "--runner=DataflowRunner",
                "--region=europe-west1"

        );
        Main.main(list.toArray(new String[list.size()]));

        Assertions.assertTrue(true);

    }
}