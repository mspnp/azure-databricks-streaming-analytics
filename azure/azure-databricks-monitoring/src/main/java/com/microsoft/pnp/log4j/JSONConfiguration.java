package com.microsoft.pnp.log4j;

import com.fasterxml.jackson.databind.ObjectMapper;

public interface JSONConfiguration {
    void configure(ObjectMapper objectMapper);
}
