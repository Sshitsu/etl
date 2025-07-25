package com.shtisu.etl.porcessor;

import com.shtisu.etl.model.*;
import com.shtisu.etl.processor.FinalRecordItemProcessor;
import com.shtisu.etl.util.UnitConverter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.batch.item.ItemProcessor;

import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

class FinalRecordItemProcessorTest {

    private FinalRecordItemProcessor processor;

    @BeforeEach
    void setUp() {
        processor = new FinalRecordItemProcessor();
    }


    @Test
    void process_basic24hAggregates() throws Exception {
        // Просто задаем OpenMeteoResponse resp заполняем поля и после выполнения processRange() проверяем что все поля заполнились
        OpenMeteoResponse resp = new OpenMeteoResponse();
        resp.setLatitude(52.0);
        resp.setLongitude(4.0);

        long epochStart = Instant.parse("2025-07-15T00:00:00Z").getEpochSecond();
        DailyData daily = new DailyData();
        daily.setTime(List.of(epochStart));
        daily.setSunrise(List.of(epochStart));
        daily.setSunset(List.of(epochStart + 23 * 3600L));
        resp.setDaily(daily);

        HourlyData hourly = new HourlyData();

        List<Long> times = IntStream.range(0, 24)
                .mapToLong(i -> epochStart + i * 3600L)
                .boxed()
                .toList();
        hourly.setTime(times);


        List<Double> tempF  = IntStream.range(0, 24)
                .mapToObj(i -> 50.0)
                .toList();

        List<Double> rainIn  = IntStream.range(0, 24)
                .mapToObj(i -> 1.0)
                .toList();

        List<Integer> rhPct  = IntStream.range(0, 24)
                .map(i -> 50)
                .boxed()
                .toList();

        hourly.setTemperature2m(tempF);
        hourly.setRain(rainIn);


        hourly.setRelativeHumidity2m(rhPct);
        hourly.setDewPoint2m(tempF);
        hourly.setApparentTemperature(tempF);
        hourly.setTemperature80m(tempF);
        hourly.setTemperature120m(tempF);
        hourly.setWindSpeed10m(tempF);
        hourly.setWindSpeed80m(tempF);
        hourly.setVisibility(tempF);
        hourly.setShowers(rainIn);
        hourly.setSnowfall(rainIn);
        hourly.setSoilTemperature0cm(tempF);
        hourly.setSoilTemperature6cm(tempF);


        resp.setHourly(hourly);

        List<FinalRecord> rec = processor.processRange(resp);


        assertThat(rec.get(0).getLatitude()).isEqualTo(52.0);
        assertThat(rec.get(0).getLongitude()).isEqualTo(4.0);
        assertThat(rec.get(0).getDate()).isEqualTo(LocalDate.of(2025, 7, 15));
        assertThat(rec.get(0).getDaylightHours()).isEqualTo(23.0);

        double expectedTempC = UnitConverter.fahrenheitToCelsius(50.0);
        assertThat(rec.get(0).getAvgTemperature2m24h()).isEqualTo(expectedTempC);


        double expectedRainMm = UnitConverter.inchToMillimeter(24.0);
        assertThat(rec.get(0).getTotalRain24h()).isEqualTo(expectedRainMm);

        assertThat(rec.get(0).getAvgTemperature2mDaylight()).isEqualTo(expectedTempC);
        assertThat(rec.get(0).getTotalRainDaylight()).isEqualTo(expectedRainMm);

        assertThat(rec.get(0).getFetchedAt()).isNotNull();
        System.out.println(rec);
    }
}
