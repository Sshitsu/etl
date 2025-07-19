package com.shtisu.etl.processor;

import com.shtisu.etl.model.DailyData;
import com.shtisu.etl.model.FinalRecord;
import com.shtisu.etl.model.HourlyData;
import com.shtisu.etl.model.OpenMeteoResponse;
import com.shtisu.etl.util.UnitConverter;
import org.springframework.batch.item.ItemProcessor;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.function.ToDoubleFunction;
import java.util.stream.IntStream;

public class FinalRecordItemProcessor implements ItemProcessor<OpenMeteoResponse, FinalRecord> {

    @Override
    public FinalRecord process(OpenMeteoResponse resp) {
        FinalRecord rec = new FinalRecord();

        // 1. Метаданные
        fillMetadata(rec, resp);

        // 2. 24‑часовые агрегаты
        fill24hAggregates(rec, resp.getHourly());

        // 3. Агрегаты за световой день
        fillDaylightAggregates(rec, resp.getHourly(), rec.getSunriseIso(), rec.getSunsetIso());

        // 4. Точечные конверсии
        fillPointValues(rec, resp.getHourly());

        // 5. fetchedAt
        rec.setFetchedAt(Instant.now());

        return rec;
    }

    /**
     * Заполняет базовые метаданные (широту, долготу, дату, рассвет, закат и продолжительность дня).
     */
    private void fillMetadata(FinalRecord rec, OpenMeteoResponse resp) {
        rec.setLatitude(resp.getLatitude());
        rec.setLongitude(resp.getLongitude());

        DailyData d = resp.getDaily();
        Instant sunrise = Instant.ofEpochSecond(d.getSunrise().get(0));
        Instant sunset  = Instant.ofEpochSecond(d.getSunset().get(0));
        Instant d0       = Instant.ofEpochSecond(d.getTime().get(0));

        rec.setDate(d0.atZone(ZoneOffset.UTC).toLocalDate());
        rec.setSunriseIso(sunrise);
        rec.setSunsetIso(sunset);
        rec.setDaylightHours(Duration.between(sunrise, sunset).toHours());
    }
    /**
     * Считает 24‑часовые агрегаты по температурам, осадкам, скорости ветра, видимости и т.д..
     */
    private void fill24hAggregates(FinalRecord rec, HourlyData h) {

        rec.setAvgTemperature2m24h   (averageAndConvert(h.getTemperature2m(), UnitConverter::fahrenheitToCelsius));
        rec.setAvgRelativeHumidity2m24h(average(h.getRelativeHumidity2m()));
        rec.setAvgDewPoint2m24h      (averageAndConvert(h.getDewPoint2m(), UnitConverter::fahrenheitToCelsius));
        rec.setAvgApparentTemperature24h(averageAndConvert(h.getApparentTemperature(), UnitConverter::fahrenheitToCelsius));
        rec.setAvgTemperature80m24h  (averageAndConvert(h.getTemperature80m(), UnitConverter::fahrenheitToCelsius));
        rec.setAvgTemperature120m24h (averageAndConvert(h.getTemperature120m(), UnitConverter::fahrenheitToCelsius));
        rec.setAvgWindSpeed10m24h    (averageAndConvert(h.getWindSpeed10m(), UnitConverter::knotsToMetersPerSecond));
        rec.setAvgWindSpeed80m24h    (averageAndConvert(h.getWindSpeed80m(), UnitConverter::knotsToMetersPerSecond));
        rec.setAvgVisibility24h      (averageAndConvert(h.getVisibility(), UnitConverter::feetToMeters));

        rec.setTotalRain24h          (sumAndConvert(h.getRain(), UnitConverter::inchToMillimeter));
        rec.setTotalShowers24h       (sumAndConvert(h.getShowers(), UnitConverter::inchToMillimeter));
        rec.setTotalSnowfall24h      (sumAndConvert(h.getSnowfall(), UnitConverter::inchToMillimeter));
    }
    /**
     *  Считает агрегаты по температурам, осадкам, скорости ветра, видимости и т.д. но только внутри светового дня (по индексам между рассветом и закатом).
     */
    private void fillDaylightAggregates(FinalRecord rec, HourlyData h, Instant sunrise, Instant sunset) {

        List<Integer> idx = IntStream.range(0, h.getTime().size())
                .filter(i -> {
                    Instant t = Instant.ofEpochSecond(h.getTime().get(i));
                    return !t.isBefore(sunrise) && !t.isAfter(sunset);
                }).boxed()
                .toList();

        rec.setAvgTemperature2mDaylight   (averageAndConvert(h.getTemperature2m(), idx, UnitConverter::fahrenheitToCelsius));
        rec.setAvgRelativeHumidity2mDaylight(average(h.getRelativeHumidity2m(), idx));
        rec.setAvgDewPoint2mDaylight      (averageAndConvert(h.getDewPoint2m(), idx, UnitConverter::fahrenheitToCelsius));
        rec.setAvgApparentTemperatureDaylight(averageAndConvert(h.getApparentTemperature(), idx, UnitConverter::fahrenheitToCelsius));
        rec.setAvgTemperature80mDaylight  (averageAndConvert(h.getTemperature80m(), idx, UnitConverter::fahrenheitToCelsius));
        rec.setAvgTemperature120mDaylight (averageAndConvert(h.getTemperature120m(), idx, UnitConverter::fahrenheitToCelsius));
        rec.setAvgWindSpeed10mDaylight    (averageAndConvert(h.getWindSpeed10m(), idx, UnitConverter::knotsToMetersPerSecond));
        rec.setAvgWindSpeed80mDaylight    (averageAndConvert(h.getWindSpeed80m(), idx, UnitConverter::knotsToMetersPerSecond));
        rec.setAvgVisibilityDaylight      (averageAndConvert(h.getVisibility(), idx, UnitConverter::feetToMeters));
        rec.setTotalRainDaylight          (sumAndConvert(h.getRain(), idx, UnitConverter::inchToMillimeter));
        rec.setTotalShowersDaylight       (sumAndConvert(h.getShowers(), idx, UnitConverter::inchToMillimeter));
        rec.setTotalSnowfallDaylight      (sumAndConvert(h.getSnowfall(), idx, UnitConverter::inchToMillimeter));
    }
    /**
     *  Точечные конверсии первого часового значения (температура, ветер, осадки и т.д.).
     */
    private void fillPointValues(FinalRecord rec, HourlyData h) {
        rec.setTemperature2mCelsius       (UnitConverter.fahrenheitToCelsius(h.getTemperature2m().get(0)));
        rec.setApparentTemperatureCelsius (UnitConverter.fahrenheitToCelsius(h.getApparentTemperature().get(0)));
        rec.setTemperature80mCelsius      (UnitConverter.fahrenheitToCelsius(h.getTemperature80m().get(0)));
        rec.setTemperature120mCelsius     (UnitConverter.fahrenheitToCelsius(h.getTemperature120m().get(0)));
        rec.setSoilTemperature0cmCelsius  (UnitConverter.fahrenheitToCelsius(h.getSoilTemperature0cm().get(0)));
        rec.setSoilTemperature6cmCelsius  (UnitConverter.fahrenheitToCelsius(h.getSoilTemperature6cm().get(0)));
        rec.setWindSpeed10mMPerS          (UnitConverter.knotsToMetersPerSecond(h.getWindSpeed10m().get(0)));
        rec.setWindSpeed80mMPerS          (UnitConverter.knotsToMetersPerSecond(h.getWindSpeed80m().get(0)));
        rec.setRainMm                     (UnitConverter.inchToMillimeter(h.getRain().get(0)));
        rec.setShowersMm                  (UnitConverter.inchToMillimeter(h.getShowers().get(0)));
        rec.setSnowfallMm                 (UnitConverter.inchToMillimeter(h.getSnowfall().get(0)));
    }

    /**
     * Среднее по любому списку числовых значений (Double, Integer и т.д.).
     */
    private double average(List<? extends Number> data) {
        return data.stream()
                .mapToDouble(Number::doubleValue)
                .average()
                .orElse(0.0);
    }

    /**
     * Среднее по любому списку числовых значений, но только по индексам из idx.
     */
    private double average(List<? extends Number> data, List<Integer> idx) {
        return idx.stream()
                .mapToDouble(i -> data.get(i).doubleValue())
                .average()
                .orElse(0.0);
    }

    private double averageAndConvert(List<Double> data, ToDoubleFunction<Double> conv) {
        return conv.applyAsDouble(average(data));
    }
    private double averageAndConvert(List<Double> data, List<Integer> idx, ToDoubleFunction<Double> conv) {
        return conv.applyAsDouble(
                idx.stream().mapToDouble(data::get).average().orElse(0.0)
        );
    }
    private double sumAndConvert(List<Double> data, ToDoubleFunction<Double> conv) {
        return conv.applyAsDouble(data.stream().mapToDouble(Double::doubleValue).sum());
    }
    private double sumAndConvert(List<Double> data, List<Integer> idx, ToDoubleFunction<Double> conv) {
        return conv.applyAsDouble(
                idx.stream().mapToDouble(data::get).sum()
        );
    }
}
