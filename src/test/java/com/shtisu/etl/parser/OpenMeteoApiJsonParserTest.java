package com.shtisu.etl.parser;

import com.shtisu.etl.model.OpenMeteoResponse;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.TimeZone;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

 class OpenMeteoApiJsonParserTest {

    @Test
     void parseSampleJson_shouldPopulateAllTopLevelFields() throws IOException {

       // Просто считываем поля из Json и проверяем их (Все поля проверять не стал но стоит это сделать в будующем)

        String json = Files.readString(Paths.get("D:\\IdeaProjects\\etl\\src\\test\\resources\\sample-open-meteo.json"));
        OpenMeteoResponse resp = OpenMeteoApiJsonParser.parse(json);
        System.out.println(resp);

        assertThat(resp).isNotNull();
        assertThat(resp.getLatitude()).isEqualTo(55.0);
        assertThat(resp.getLongitude()).isEqualTo(83.0);
        assertThat(resp.getGenerationtimeMs()).isEqualTo(40.9938097000122);


        assertThat(resp.getDaily()).isNotNull();
        assertThat(resp.getHourly()).isNotNull();

        assertThat(resp.getDaily().getTime()).isNotNull();
        assertThat(resp.getHourly().getTime()).isNotNull();
        assertThat(resp.getTimezone()).isEqualTo("Asia/Novosibirsk");




    }
}
