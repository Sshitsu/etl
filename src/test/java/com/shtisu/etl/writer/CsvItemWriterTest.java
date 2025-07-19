package com.shtisu.etl.writer;

import com.shtisu.etl.model.FinalRecord;
import com.shtisu.etl.util.UnitConverter;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class CsvItemWriterTest {

    @Test
    void write_createsCsvWithExpectedColumnsAndValues_forTwoRecords() throws Exception {

        FinalRecord rec1 = new FinalRecord();
        rec1.setLatitude(52.0);
        rec1.setLongitude(4.0);

        LocalDate date1 = LocalDate.of(2025, 7, 15);
        rec1.setDate(date1);
        Instant sunrise1 = Instant.parse("2025-07-15T04:00:00Z");
        Instant sunset1  = Instant.parse("2025-07-15T19:00:00Z");
        rec1.setSunriseIso(sunrise1);
        rec1.setSunsetIso(sunset1);
        rec1.setDaylightHours(Duration.between(sunrise1, sunset1).toHours());

        double avgTempC1 = UnitConverter.fahrenheitToCelsius(50.0);
        rec1.setAvgTemperature2m24h(avgTempC1);
        double totalRainMm1 = UnitConverter.inchToMillimeter(24.0);
        rec1.setTotalRain24h(totalRainMm1);

        Instant fetchedAt1 = Instant.parse("2025-07-15T20:00:00Z");
        rec1.setFetchedAt(fetchedAt1);

        FinalRecord rec2 = new FinalRecord();
        rec2.setLatitude(53.0);
        rec2.setLongitude(5.0);

        LocalDate date2 = LocalDate.of(2025, 7, 16);
        rec2.setDate(date2);
        Instant sunrise2 = Instant.parse("2025-07-16T04:10:00Z");
        Instant sunset2  = Instant.parse("2025-07-16T19:10:00Z");
        rec2.setSunriseIso(sunrise2);
        rec2.setSunsetIso(sunset2);
        rec2.setDaylightHours(Duration.between(sunrise2, sunset2).toHours());

        double avgTempC2 = UnitConverter.fahrenheitToCelsius(60.0);
        rec2.setAvgTemperature2m24h(avgTempC2);
        double totalRainMm2 = UnitConverter.inchToMillimeter(12.0);
        rec2.setTotalRain24h(totalRainMm2);

        Instant fetchedAt2 = Instant.parse("2025-07-16T20:00:00Z");
        rec2.setFetchedAt(fetchedAt2);


        Path out = Paths.get("src/test/resources/etl-test.csv");
        Files.createDirectories(out.getParent());
        Files.deleteIfExists(out);

        CsvItemWriter writer = new CsvItemWriter(out);
        writer.write(List.of(rec1, rec2));

        List<String> lines = Files.readAllLines(out);

        assertThat(lines).hasSize(3);

        String[] vals1 = Arrays.stream(lines.get(1).split(";"))
                .map(s -> s.replaceAll("^\"|\"$", ""))
                .toArray(String[]::new);
        assertThat(Double.parseDouble(vals1[0])).isEqualTo(52.0);
        assertThat(Double.parseDouble(vals1[1])).isEqualTo(4.0);
        assertThat(vals1[2]).isEqualTo("2025-07-15");
        assertThat(Double.parseDouble(vals1[6])).isEqualTo(avgTempC1);
        assertThat(Double.parseDouble(vals1[15])).isEqualTo(totalRainMm1);
        assertThat(vals1[vals1.length - 1]).isEqualTo("2025-07-15T20:00:00Z");

        String[] vals2 = Arrays.stream(lines.get(2).split(";"))
                .map(s -> s.replaceAll("^\"|\"$", ""))
                .toArray(String[]::new);
        assertThat(Double.parseDouble(vals2[0])).isEqualTo(53.0);
        assertThat(Double.parseDouble(vals2[1])).isEqualTo(5.0);
        assertThat(vals2[2]).isEqualTo("2025-07-16");
        assertThat(Double.parseDouble(vals2[6])).isEqualTo(avgTempC2);
        assertThat(Double.parseDouble(vals2[15])).isEqualTo(totalRainMm2);
        assertThat(vals2[vals2.length - 1]).isEqualTo("2025-07-16T20:00:00Z");
    }
}