package org.example.services;

import lombok.extern.slf4j.Slf4j;
import org.example.config.EnvConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@Slf4j
class FileToBQTest {

    @BeforeAll
    public static void init() {
        try {
            EnvConfig.envSetup();
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }

    }

    @Test
    void process() {
        log.info("FileToBQ testing..");

        List<String> list = Arrays.asList(
                "--runner=DirectRunner",
                "--inputFile=gs://linkedin_learning_56/charter02/input/Sales_April_2019.csv",
                "--targetTableName=SALES_DETAILS"
        );
        new FileToBQ().process(list.toArray(new String[list.size()]));

        Assertions.assertTrue(true);
    }

}