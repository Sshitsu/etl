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
import java.time.ZoneOffset;
import java.util.List;
import java.util.function.ToDoubleFunction;
import java.util.stream.IntStream;


/**
 * {@code FinalRecordItemProcessor} является реализацией Spring Batch {@link org.springframework.batch.item.ItemProcessor}
 * и отвечает за преобразование входного ответа от OpenMeteo API ({@link com.shtisu.etl.model.OpenMeteoResponse})
 * в окончательную модель {@link com.shtisu.etl.model.FinalRecord}.
 *
 * <p>В процессе обработки выполняются следующие шаги:
 * <ol>
 *   <li>Заполнение базовых метаданных (широта, долгота, дата, рассвет, закат, продолжительность дня).</li>
 *   <li>Вычисление агрегатов за последние 24 часа: средние значения температур, относительной влажности, точки росы,
 *       кажущейся температуры, скорости ветра, видимости и суммарные осадки.</li>
 *   <li>Вычисление агрегатов за световой период (между рассветом и закатом): аналогичные показатели,
 *       но только за дневные часы.</li>
 *   <li>Извлечение точечных значений первого часового измерения и конвертация единиц.</li>
 *   <li>Установка метки времени {@code fetchedAt}.</li>
 * </ol>
 *
 * <p>Утилиты для агрегации и преобразования единиц вынесены в приватные методы.
 */
public class FinalRecordItemProcessor implements ItemProcessor<OpenMeteoResponse, FinalRecord> {
    /**
     * Основной метод обработки. Преобразует {@code OpenMeteoResponse} в {@code FinalRecord}.
     *
     * @param resp входной объект, содержащий сырые данные hourly и daily
     * @return заполненный объект {@code FinalRecord}
     * @throws Exception если при заполнении данных произошла ошибка
     */
    @Override
    public FinalRecord process(OpenMeteoResponse resp) {
        FinalRecord rec = new FinalRecord();

        // Методы заполняют поля финальный таблицы а именно высчитывают средние значения за 24 часа и т.д
        fillMetadata(rec, resp);

        fill24hAggregates(rec, resp.getHourly());

        fillDaylightAggregates(rec, resp.getHourly(), rec.getSunriseIso(), rec.getSunsetIso());


        fillPointValues(rec, resp.getHourly());

        // Время когда были заполнены данные
        rec.setFetchedAt(Instant.now());

        return rec;
    }

    /**
     * Заполняет базовые метаданные (широту, долготу, дату, рассвет, закат и продолжительность дня)
     */
    private void fillMetadata(@NotNull FinalRecord rec, @NotNull OpenMeteoResponse resp) {
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
     * Считает 24‑часовые агрегаты по температурам, осадкам, скорости ветра, видимости и т.д
     */
    private void fill24hAggregates(@NotNull FinalRecord rec, @NotNull HourlyData h) {

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
     *  Считает агрегаты по температурам, осадкам, скорости ветра, видимости и т.д. но только внутри светового дня (по индексам между рассветом и закатом)
     */
    private void fillDaylightAggregates(@NotNull FinalRecord rec, @NotNull HourlyData h, Instant sunrise, Instant sunset) {

        // Индексы между восходом и закатом
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
     *  Точечные конверсии первого часового значения (температура, ветер, осадки и т.д.)
     */
    private void fillPointValues(@NotNull FinalRecord rec, @NotNull HourlyData h) {
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
}
