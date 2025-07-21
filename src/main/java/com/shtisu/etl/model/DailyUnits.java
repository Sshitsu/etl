package com.shtisu.etl.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
/**
 * Вспомогательная модель
 */
@Data
public class DailyUnits {
    private String time;
    private String sunrise;
    private String sunset;
    @JsonProperty("daylight_duration")
    private String daylightDuration;
}
