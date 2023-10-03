package org.example;

import lombok.extern.slf4j.Slf4j;
import org.example.services.ProcessService2;
import org.example.services.ProcessService;
import org.example.services.ProcessService3;


@Slf4j
public class Main {
    public static void main(String[] args) {
        System.out.println("Hello world!");
        log.info("main()...");

        System.setProperty("http.proxyHost", "10.0.1.223");
        System.setProperty("http.proxyPort", "7887");
        System.setProperty("https.proxyHost", "10.0.1.223");
        System.setProperty("https.proxyPort", "7890");

        new ProcessService3().process();
    }
}