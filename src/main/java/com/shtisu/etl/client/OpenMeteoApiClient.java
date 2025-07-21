package com.shtisu.etl.client;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.LocalDate;
import java.util.StringJoiner;

import com.shtisu.etl.model.OpenMeteoResponse;
import com.shtisu.etl.parser.OpenMeteoApiJsonParser;
import lombok.extern.slf4j.Slf4j;

/**
 * Класс для обращения к Open Meteo API
 */
public class OpenMeteoApiClient {

    private final HttpClient http;
    private final String baseUrl;


    public OpenMeteoApiClient(String baseUrl) {
        this.http = HttpClient.newHttpClient();
        this.baseUrl = baseUrl;
    }

    /**
     *
     * @param latitude Задем широту
     * @param longitude Задаем долготу
     * @param start Задаем начала отрезка времени по которму хотим получить данные
     * @param end Задаем конец
     * @return Возвращает OpenMeteoResponse который предстовляет модель описывающие все поля Json из API запроса
     * @throws IOException
     * @throws InterruptedException
     */
    public OpenMeteoResponse fetch(double latitude,
                                   double longitude,
                                   LocalDate start,
                                   LocalDate end) throws IOException, InterruptedException {


        String url = buildUrl(latitude, longitude, start, end);
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .GET()
                .build();

        HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
        if (resp.statusCode() != 200) {
            throw new IOException("Unexpected HTTP status: " + resp.statusCode());
        }


        return OpenMeteoApiJsonParser.parse(resp.body());
    }


    private String buildUrl(double lat, double lon, LocalDate start, LocalDate end) {
        StringJoiner sj = new StringJoiner("&", baseUrl + "/v1/forecast?", "");
        sj.add("latitude="     + lat);
        sj.add("longitude="    + lon);
        sj.add("start_date="   + start);
        sj.add("end_date="     + end);

        // Указываем все нужные поля
        sj.add("hourly=temperature_2m"
                + ",relativehumidity_2m"
                + ",dewpoint_2m"
                + ",apparent_temperature"
                + ",temperature_80m"
                + ",temperature_120m"
                + ",windspeed_10m"
                + ",windspeed_80m"
                + ",winddirection_10m"
                + ",winddirection_80m"
                + ",visibility"
                + ",evapotranspiration"
                + ",rain"
                + ",showers"
                + ",snowfall"
                + ",soil_temperature_0cm"
                + ",soil_temperature_6cm");

        sj.add("daily=sunrise,sunset,daylight_duration");

        sj.add("timezone=auto");

        // Указываем формат времени
        sj.add("timeformat=unixtime");

        return sj.toString();
    }

}
