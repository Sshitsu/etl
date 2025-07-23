package com.shtisu.etl.processor;

import com.shtisu.etl.model.DailyData;
import com.shtisu.etl.model.FinalRecord;
import com.shtisu.etl.model.HourlyData;
import com.shtisu.etl.model.OpenMeteoResponse;
import com.shtisu.etl.util.UnitConverter;
import org.jetbrains.annotations.NotNull;
import org.springframework.batch.item.ItemProcessor;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.*;
import java.util.function.ToDoubleFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;


/**
 * Преобразует ответ OpenMeteo в список {@link FinalRecord}: один объект на каждый день диапазона.
 */
public class FinalRecordItemProcessor  {
    /**
     * Главный метод: из одного API-ответа делает список FinalRecord (по дням).
     */
    public List<FinalRecord> processRange(@NotNull OpenMeteoResponse resp) {

        DailyData d = Objects.requireNonNull(resp.getDaily(), "daily is null");
        HourlyData h = Objects.requireNonNull(resp.getHourly(), "hourly is null");

        validateSizes(h); // см. ниже




        Map<LocalDate, List<Integer>> dayToIdx = IntStream.range(0,h.getTime().size())
                .boxed()
                        .collect(Collectors.groupingBy(i ->
                                Instant.ofEpochSecond(h.getTime().get(i))
                                        .atZone(ZoneOffset.UTC)
                                        .toLocalDate()
                        ));

        List<FinalRecord> result = new ArrayList<>(d.getTime().size());

        for (int di = 0; di < d.getTime().size(); di++) {
            LocalDate date = Instant.ofEpochSecond(d.getTime().get(di))
                    .atZone(ZoneOffset.UTC)
                    .toLocalDate();

            // Если по этой дате нет часовых данных — пропускаем
            List<Integer> dayIdx = dayToIdx.getOrDefault(date, List.of());
            if (dayIdx.isEmpty()) continue;

            Instant sunrise = Instant.ofEpochSecond(d.getSunrise().get(di));
            Instant sunset  = Instant.ofEpochSecond(d.getSunset().get(di));

            FinalRecord rec = new FinalRecord();

            rec.setLatitude(resp.getLatitude());
            rec.setLongitude(resp.getLongitude());
            rec.setDate(date);
            rec.setSunriseIso(sunrise);
            rec.setSunsetIso(sunset);
            rec.setDaylightHours(Duration.between(sunrise, sunset).toHours());

            // 24h агрегаты по всем индексам дня
            fill24hAggregates(rec, h, dayIdx);

            // Индексы только светового времени для этого дня
            List<Integer> daylightIdx = dayIdx.stream()
                    .filter(i -> {
                        Instant t = Instant.ofEpochSecond(h.getTime().get(i));
                        return !t.isBefore(sunrise) && !t.isAfter(sunset);
                    })
                    .toList();
            fillDaylightAggregates(rec, h, daylightIdx);

            // Точечные значения — возьмём первый час дня
            fillPointValues(rec, h, dayIdx.get(0));

            rec.setFetchedAt(Instant.now());
            result.add(rec);
        }
        return result;

    }
    /**
     * Считает 24‑часовые агрегаты по температурам, осадкам, скорости ветра, видимости и т.д
     */

    private void fill24hAggregates(@NotNull FinalRecord rec,
                                   @NotNull HourlyData h,
                                   @NotNull List<Integer> idx) {
        rec.setAvgTemperature2m24h        (averageAndConvert(h.getTemperature2m(), idx, UnitConverter::fahrenheitToCelsius));
        rec.setAvgRelativeHumidity2m24h   (average(h.getRelativeHumidity2m(), idx));
        rec.setAvgDewPoint2m24h           (averageAndConvert(h.getDewPoint2m(), idx, UnitConverter::fahrenheitToCelsius));
        rec.setAvgApparentTemperature24h  (averageAndConvert(h.getApparentTemperature(), idx, UnitConverter::fahrenheitToCelsius));
        rec.setAvgTemperature80m24h       (averageAndConvert(h.getTemperature80m(), idx, UnitConverter::fahrenheitToCelsius));
        rec.setAvgTemperature120m24h      (averageAndConvert(h.getTemperature120m(), idx, UnitConverter::fahrenheitToCelsius));
        rec.setAvgWindSpeed10m24h         (averageAndConvert(h.getWindSpeed10m(), idx, UnitConverter::knotsToMetersPerSecond));
        rec.setAvgWindSpeed80m24h         (averageAndConvert(h.getWindSpeed80m(), idx, UnitConverter::knotsToMetersPerSecond));
        rec.setAvgVisibility24h           (averageAndConvert(h.getVisibility(), idx, UnitConverter::feetToMeters));

        rec.setTotalRain24h               (sumAndConvert(h.getRain(), idx, UnitConverter::inchToMillimeter));
        rec.setTotalShowers24h            (sumAndConvert(h.getShowers(), idx, UnitConverter::inchToMillimeter));
        rec.setTotalSnowfall24h           (sumAndConvert(h.getSnowfall(), idx, UnitConverter::inchToMillimeter));
    }
    /**
     *  Считает агрегаты по температурам, осадкам, скорости ветра, видимости и т.д. но только внутри светового дня (по индексам между рассветом и закатом)
     */
    private void fillDaylightAggregates(@NotNull FinalRecord rec,
                                        @NotNull HourlyData h,
                                        @NotNull List<Integer> daylightIdx) {
        rec.setAvgTemperature2mDaylight        (averageAndConvert(h.getTemperature2m(), daylightIdx, UnitConverter::fahrenheitToCelsius));
        rec.setAvgRelativeHumidity2mDaylight   (average(h.getRelativeHumidity2m(), daylightIdx));
        rec.setAvgDewPoint2mDaylight           (averageAndConvert(h.getDewPoint2m(), daylightIdx, UnitConverter::fahrenheitToCelsius));
        rec.setAvgApparentTemperatureDaylight  (averageAndConvert(h.getApparentTemperature(), daylightIdx, UnitConverter::fahrenheitToCelsius));
        rec.setAvgTemperature80mDaylight       (averageAndConvert(h.getTemperature80m(), daylightIdx, UnitConverter::fahrenheitToCelsius));
        rec.setAvgTemperature120mDaylight      (averageAndConvert(h.getTemperature120m(), daylightIdx, UnitConverter::fahrenheitToCelsius));
        rec.setAvgWindSpeed10mDaylight         (averageAndConvert(h.getWindSpeed10m(), daylightIdx, UnitConverter::knotsToMetersPerSecond));
        rec.setAvgWindSpeed80mDaylight         (averageAndConvert(h.getWindSpeed80m(), daylightIdx, UnitConverter::knotsToMetersPerSecond));
        rec.setAvgVisibilityDaylight           (averageAndConvert(h.getVisibility(), daylightIdx, UnitConverter::feetToMeters));

        rec.setTotalRainDaylight               (sumAndConvert(h.getRain(), daylightIdx, UnitConverter::inchToMillimeter));
        rec.setTotalShowersDaylight            (sumAndConvert(h.getShowers(), daylightIdx, UnitConverter::inchToMillimeter));
        rec.setTotalSnowfallDaylight           (sumAndConvert(h.getSnowfall(), daylightIdx, UnitConverter::inchToMillimeter));
    }
    /**
     *  Точечные конверсии первого часового значения (температура, ветер, осадки и т.д.)
     */
    private void fillPointValues(@NotNull FinalRecord rec,
                                 @NotNull HourlyData h,
                                 int firstIdx) {
        rec.setTemperature2mCelsius       (UnitConverter.fahrenheitToCelsius(h.getTemperature2m().get(firstIdx)));
        rec.setApparentTemperatureCelsius (UnitConverter.fahrenheitToCelsius(h.getApparentTemperature().get(firstIdx)));
        rec.setTemperature80mCelsius      (UnitConverter.fahrenheitToCelsius(h.getTemperature80m().get(firstIdx)));
        rec.setTemperature120mCelsius     (UnitConverter.fahrenheitToCelsius(h.getTemperature120m().get(firstIdx)));
        rec.setSoilTemperature0cmCelsius  (UnitConverter.fahrenheitToCelsius(h.getSoilTemperature0cm().get(firstIdx)));
        rec.setSoilTemperature6cmCelsius  (UnitConverter.fahrenheitToCelsius(h.getSoilTemperature6cm().get(firstIdx)));
        rec.setWindSpeed10mMPerS          (UnitConverter.knotsToMetersPerSecond(h.getWindSpeed10m().get(firstIdx)));
        rec.setWindSpeed80mMPerS          (UnitConverter.knotsToMetersPerSecond(h.getWindSpeed80m().get(firstIdx)));
        rec.setRainMm                     (UnitConverter.inchToMillimeter(h.getRain().get(firstIdx)));
        rec.setShowersMm                  (UnitConverter.inchToMillimeter(h.getShowers().get(firstIdx)));
        rec.setSnowfallMm                 (UnitConverter.inchToMillimeter(h.getSnowfall().get(firstIdx)));
    }

    /**
     * Среднее по любому списку числовых значений (Double, Integer и т.д.)
     */
    private double average(@NotNull List<? extends Number> data) {
        return data.stream()
                .mapToDouble(Number::doubleValue)
                .average()
                .orElse(0.0);
    }

    /**
     * Среднее по любому списку числовых значений, но только по индексам из idx
     */
    private double average(List<? extends Number> data, @NotNull List<Integer> idx) {

        return idx.stream()
                .mapToDouble(i -> data.get(i).doubleValue())
                .average()
                .orElse(0.0);
    }

    /**
     * Среднее по списку Double и преобразование значений используя нужный конвектор.
     */
    private double averageAndConvert(List<Double> data, @NotNull ToDoubleFunction<Double> conv) {
        return conv.applyAsDouble(average(data));
    }
    /**
     * Среднее по списку Double и преобразование значений используя нужный конвектор, но только по индексам из idx
     * */
    private double averageAndConvert(@NotNull List<Double> data, @NotNull List<Integer> idx, @NotNull ToDoubleFunction<Double> conv) {
        return conv.applyAsDouble(
                idx.stream().mapToDouble(data::get).average().orElse(0.0)
        );
    }

    /**
     * Сумма значений
     */
    private double sumAndConvert(@NotNull List<Double> data, @NotNull ToDoubleFunction<Double> conv) {
        return conv.applyAsDouble(data.stream().mapToDouble(Double::doubleValue).sum());
    }
    /**
     * Сумма значений, но только по индексам из idx
     */
    private double sumAndConvert(@NotNull List<Double> data, @NotNull List<Integer> idx, @NotNull ToDoubleFunction<Double> conv) {
        return conv.applyAsDouble(
                idx.stream().mapToDouble(data::get).sum()
        );
    }
    private void validateSizes(HourlyData h) {
        int n = h.getTime().size();
        Stream.of(
                h.getTemperature2m(), h.getRelativeHumidity2m(), h.getDewPoint2m(),
                h.getApparentTemperature(), h.getTemperature80m(), h.getTemperature120m(),
                h.getWindSpeed10m(), h.getWindSpeed80m(), h.getVisibility(),
                h.getRain(), h.getShowers(), h.getSnowfall(),
                h.getSoilTemperature0cm(), h.getSoilTemperature6cm()
        ).forEach(list -> {
            if (list == null) throw new IllegalArgumentException("One of hourly lists is null");
            if (list.size() != n) throw new IllegalArgumentException("Hourly list size mismatch: " + list.size() + " != " + n);
        });
    }
}
