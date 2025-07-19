package com.shtisu.etl.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class OpenMeteoResponse {

    private double latitude;
    private double longitude;
    private double generationtimeMs;
    private int utcOffsetSeconds;
    private String timezone;
    private String timezoneAbbreviation;
    private double elevation;

    @JsonProperty("hourly_units")
    private HourlyUnits hourlyUnits;
    @JsonProperty("daily_units")
    private DailyUnits dailyUnits;
    @JsonProperty("hourly")
    private HourlyData hourly;
    @JsonProperty("daily")
    private DailyData daily;
}
