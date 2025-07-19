package com.shtisu.etl.parser;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.shtisu.etl.model.OpenMeteoResponse;

import java.io.IOException;

public class OpenMeteoApiJsonParser {

    private static final ObjectMapper MAPPER  = new ObjectMapper()
            .setPropertyNamingStrategy(new PropertyNamingStrategies.SnakeCaseStrategy())
            .registerModule(new JavaTimeModule())
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    public static OpenMeteoResponse parse(String json) throws IOException {
        return MAPPER.readValue(json,OpenMeteoResponse.class );
    }

}
