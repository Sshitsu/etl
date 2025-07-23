package com.shtisu.etl.model;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.List;
/**
 * Вспомогательная модель
 */
@Data
public class HourlyData {
    @JsonProperty("time")
    private List<Long> time;

    @JsonProperty("temperature_2m")
    private List<Double> temperature2m;

    // JsonAlias это альтернативное имя переменной в Api заросе relativehumidity_2m такое, а
    // в Json из примера в заданий именна другие (relative_humidity_2m), поэтому для предотвращения конфликтов,
    // я написал альтернативный вариант
    @JsonProperty("relativehumidity_2m")
    @JsonAlias("relative_humidity_2m")
    private List<Integer> relativeHumidity2m;

    @JsonProperty("dewpoint_2m")
    @JsonAlias("dew_point_2m")
    private List<Double> dewPoint2m;

    @JsonProperty("apparent_temperature")
    private List<Double> apparentTemperature;

    @JsonProperty("temperature_80m")
    private List<Double> temperature80m;

    @JsonProperty("temperature_120m")
    private List<Double> temperature120m;

    @JsonProperty("windspeed_10m")
    @JsonAlias({"wind_speed_10m"})
    private List<Double> windSpeed10m;

    @JsonProperty("windspeed_80m")
    @JsonAlias({"wind_speed_80m"})
    private List<Double> windSpeed80m;

    @JsonProperty("winddirection_10m")
    @JsonAlias({"wind_direction_10m"})
    private List<Integer> windDirection10m;

    @JsonProperty("winddirection_80m")
    @JsonAlias({"wind_direction_80m"})
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
