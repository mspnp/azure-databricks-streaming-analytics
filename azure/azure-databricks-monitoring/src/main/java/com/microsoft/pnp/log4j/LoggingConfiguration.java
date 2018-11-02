package com.microsoft.pnp.log4j;

import org.apache.log4j.PropertyConfigurator;

import java.io.InputStream;

public class LoggingConfiguration {
    public static void configure(String configFilename) {
        PropertyConfigurator.configure(configFilename);
    }

    public static void configure(InputStream inputStream) {
        PropertyConfigurator.configure(inputStream);
    }
}
