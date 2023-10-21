package org.example;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;


@Slf4j
class MainTest {

    @Test
    @Disabled
    void testMainWithDirectJava() throws UnknownHostException {
        log.info("main testing..");

        List<String> list= Arrays.asList(
                "--runner=DirectRunner",
                "--inputFile=gs://linkedin_learning_56/charter02/input/Sales_April_2019.csv"
        );
        Main.main(list.toArray(new String[list.size()]));

        Assertions.assertTrue(true);

    }

    @Test
    @Disabled
    void testMainWithDataflowRunner() throws UnknownHostException {
        log.info("main testing..");


        List<String> list= Arrays.asList(
                "--project=jason-hsbc",
                "--inputFile=gs://linkedin_learning_56/charter02/input/Sales_April_2019.csv",
                "--stagingLocation=gs://jason-hsbc-dataflow/staging/",
                "--subnetwork=regions/europe-west2/subnetworks/subnet-west2",
                "--tempLocation=gs://jason-hsbc-dataflow/tmp",
                // "--templateLocation=gs://jason-hsbc-dataflow/templates/template_test_template",
                "--runner=DataflowRunner",
                "--region=europe-west2"

        );
        Main.main(list.toArray(new String[list.size()]));

        Assertions.assertTrue(true);

    }
}