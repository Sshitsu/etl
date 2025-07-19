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


public class OpenMeteoApiClient {

    private final HttpClient http;
    private final String baseUrl;


    public OpenMeteoApiClient(String baseUrl) {
        this.http = HttpClient.newHttpClient();
        this.baseUrl = baseUrl;
    }

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
        // Пример: https://api.open‑meteo.com/v1/forecast?latitude=52.0&longitude=4.0&start_date=2025‑07‑01&end_date=2025‑07‑01&hourly=temperature_2m,…
        StringJoiner sj = new StringJoiner("&", baseUrl + "/v1/forecast?", "");
        sj.add("latitude=" + lat);
        sj.add("longitude=" + lon);
        sj.add("start_date=" + start);
        sj.add("end_date="   + end);
        sj.add("hourly=time,temperature_2m" +
                ",relative_humidity_2m,dew_point_2m" +
                ",apparent_temperature,temperature_80m" +
                ",temperature_120m,wind_speed_10m" +
                ",wind_speed_80m,visibility" +
                ",rain,showers,snowfall" +
                ",soil_temperature_0cm" +
                ",soil_temperature_6cm");
        sj.add("daily=time,sunrise,sunset");
        return sj.toString();
    }
}
