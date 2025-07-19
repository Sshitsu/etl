package com.shtisu.etl.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.List;

@Data
public class DailyData {
    @JsonProperty("time")
    private List<Long> time;
    @JsonProperty("sunrise")
    private List<Long> sunrise;
    @JsonProperty("sunset")
    private List<Long> sunset;
    @JsonProperty("daylight_duration")
    private List<Long> daylightDuration;
}
