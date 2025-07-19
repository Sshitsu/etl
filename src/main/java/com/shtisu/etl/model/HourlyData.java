package com.shtisu.etl.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.List;

@Data
public class HourlyData {
    @JsonProperty("time")
    private List<Long> time;
    @JsonProperty("temperature_2m")
    private List<Double> temperature2m;
    @JsonProperty("relative_humidity_2m")
    private List<Integer> relativeHumidity2m;
    @JsonProperty("dew_point_2m")
    private List<Double> dewPoint2m;
    @JsonProperty("apparent_temperature")
    private List<Double> apparentTemperature;
    @JsonProperty("temperature_80m")
    private List<Double> temperature80m;
    @JsonProperty("temperature_120m")
    private List<Double> temperature120m;
    @JsonProperty("wind_speed_10m")
    private List<Double> windSpeed10m;
    @JsonProperty("wind_speed_80m")
    private List<Double> windSpeed80m;
    @JsonProperty("wind_direction_10m")
    private List<Integer> windDirection10m;
    @JsonProperty("wind_direction_80m")
    private List<Integer> windDirection80m;
    @JsonProperty("visibility")
    private List<Double> visibility;
    @JsonProperty("evapotranspiration")
    private List<Double> evapotranspiration;
    @JsonProperty("weather_code")
    private List<Integer> weatherCode;
    @JsonProperty("soil_temperature_0cm")
    private List<Double> soilTemperature0cm;
    @JsonProperty("soil_temperature_6cm")
    private List<Double> soilTemperature6cm;
    @JsonProperty("rain")
    private List<Double> rain;
    @JsonProperty("showers")
    private List<Double> showers;
    @JsonProperty("snowfall")
    private List<Double> snowfall;
}
