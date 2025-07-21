package com.shtisu.etl.writer;

import com.shtisu.etl.model.FinalRecord;
import com.shtisu.etl.util.UnitConverter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

class CsvItemWriterTest {

    @TempDir
    Path tempDir;
    private Path csvPath;
    private Path bloomPath;
    private CsvItemWriter writer;

    @BeforeEach
    void setUp() throws IOException {
        csvPath = tempDir.resolve("data.csv");
        bloomPath = tempDir.resolve("data.bloom");
        // ожидаем максимум 100 записей, fpp 1%
        writer = new CsvItemWriter(csvPath, bloomPath, 100, 0.01);
    }




    @Test
    void writeCreateAndWriteOnlyDistinctFinalRecords() throws Exception{

        FinalRecord record = new FinalRecord();
        record.setLatitude(10.0);
        record.setLongitude(20.0);
        record.setDate(LocalDate.of(2025, 7, 1));
        record.setSunriseIso(Instant.parse("2025-07-01T04:00:00Z"));
        record.setSunsetIso(Instant.parse("2025-07-01T20:00:00Z"));
        record.setDaylightHours(16);
        record.setAvgTemperature2m24h(25.0);
        record.setAvgRelativeHumidity2m24h(50.0);
        record.setAvgDewPoint2m24h(15.0);
        record.setAvgApparentTemperature24h(24.5);
        record.setAvgTemperature80m24h(26.0);
        record.setAvgTemperature120m24h(27.0);
        record.setAvgWindSpeed10m24h(5.0);
        record.setAvgWindSpeed80m24h(6.0);
        record.setAvgVisibility24h(10000.0);
        record.setTotalRain24h(0.0);
        record.setTotalShowers24h(0.0);
        record.setTotalSnowfall24h(0.0);
        record.setAvgTemperature2mDaylight(26.0);
        record.setAvgRelativeHumidity2mDaylight(45.0);
        record.setAvgDewPoint2mDaylight(14.0);
        record.setAvgApparentTemperatureDaylight(25.5);
        record.setAvgTemperature80mDaylight(26.5);
        record.setAvgTemperature120mDaylight(27.5);
        record.setAvgWindSpeed10mDaylight(5.5);
        record.setAvgWindSpeed80mDaylight(6.5);
        record.setAvgVisibilityDaylight(12000.0);
        record.setTotalRainDaylight(0.0);
        record.setTotalShowersDaylight(0.0);
        record.setTotalSnowfallDaylight(0.0);
        record.setWindSpeed10mMPerS(1.0);
        record.setWindSpeed80mMPerS(1.2);
        record.setTemperature2mCelsius(25.0);
        record.setApparentTemperatureCelsius(24.5);
        record.setTemperature80mCelsius(26.0);
        record.setTemperature120mCelsius(27.0);
        record.setSoilTemperature0cmCelsius(22.0);
        record.setSoilTemperature6cmCelsius(21.0);
        record.setRainMm(0.0);
        record.setShowersMm(0.0);
        record.setSnowfallMm(0.0);
        record.setFetchedAt(Instant.parse("2025-07-01T00:00:00Z"));

        // генерируем список из 10 одинаковых записей
        List<FinalRecord> records = Collections.nCopies(10, record);

        writer.write(records);

        List<String> lines = Files.readAllLines(csvPath, StandardCharsets.UTF_8);

        assertEquals(2, lines.size(), "CSV должен содержать header и одну запись");

    }




}