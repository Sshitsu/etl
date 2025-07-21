package com.shtisu.etl.client;

import com.shtisu.etl.model.OpenMeteoResponse;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;

import static org.assertj.core.api.Assertions.assertThat;


class OpenMeteoApiClientTest {

    @Test
    void fetch_shouldReturnParsedResponse() throws IOException, InterruptedException {

        try (MockWebServer server = new MockWebServer()) {
            String sampleJson = Files.readString(Path.of("D:\\IdeaProjects\\etl\\src\\test\\resources\\sample-open-meteo.json"));
            server.enqueue(new MockResponse()
                    .setBody(sampleJson)
                    .setHeader("Content-Type", "application/json"));

            server.start();


            String baseUrl = server.url("").toString().replaceAll("/$", "");
            OpenMeteoApiClient client = new OpenMeteoApiClient(baseUrl);


            OpenMeteoResponse resp = client.fetch(
                    55.0, 82.0,
                    LocalDate.of(2025, 7, 1),
                    LocalDate.of(2025, 7, 1)
            );


            assertThat(resp.getLatitude()).isEqualTo(55.0);
            assertThat(resp.getLongitude()).isEqualTo(83.0);
            assertThat(resp.getDaily().getTime()).isNotEmpty();
            assertThat(resp.getHourly().getTemperature2m()).isNotEmpty();
        }
    }
}
