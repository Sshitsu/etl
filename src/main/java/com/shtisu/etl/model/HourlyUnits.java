package com.shtisu.etl.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
/**
 * Вспомогательная модель
 */
@Data
public class HourlyUnits {
    @JsonProperty("time")
    private String time;
    @JsonProperty("temperature_2m")
    private String temperature2m;
    @JsonProperty("relative_humidity_2m")
    private String relativeHumidity2m;
}
