package org.example.config;

import lombok.extern.slf4j.Slf4j;

import java.net.InetAddress;
import java.net.UnknownHostException;

@Slf4j
public class EnvConfig {
    public static void envSetup() throws UnknownHostException {


        InetAddress localHost = InetAddress.getLocalHost();
        String hostname = localHost.getHostName();
        log.info("current hostname is: ".concat(hostname));

        //skip proxy setup for gcp vm
        if (!hostname.contains("instance-")){
            System.setProperty("http.proxyHost", "10.0.1.223");
            System.setProperty("http.proxyPort", "7887");
            System.setProperty("https.proxyHost", "10.0.1.223");
            System.setProperty("https.proxyPort", "7890");
            //System.setProperty("GOOGLE_APPLICATION_CREDENTIALS", "/home/gateman/keys/terraform-jason-hsbc-key.json");
            //System.setProperty("GOOGLE_CLOUD_KEYFILE_JSON", "/home/gateman/keys/terraform-jason-hsbc-key.json");
        }

    }

}
