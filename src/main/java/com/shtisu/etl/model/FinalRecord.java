package com.shtisu.etl.model;

import lombok.Data;

import java.time.Instant;
import java.time.LocalDate;


/**
 * Класс описывает все нужные поля для модели, которую мы отпровляем в итоговую таблицу
 */
@Data
public class FinalRecord {

    private double latitude;
    private double longitude;
    private LocalDate date;
    private Instant sunriseIso;
    private Instant sunsetIso;
    private double daylightHours;

    private double avgTemperature2m24h;
    private double avgRelativeHumidity2m24h;
    private double avgDewPoint2m24h;
    private double avgApparentTemperature24h;
    private double avgTemperature80m24h;
    private double avgTemperature120m24h;
    private double avgWindSpeed10m24h;
    private double avgWindSpeed80m24h;
    private double avgVisibility24h;
    private double totalRain24h;
    private double totalShowers24h;
    private double totalSnowfall24h;


    private double avgTemperature2mDaylight;
    private double avgRelativeHumidity2mDaylight;
    private double avgDewPoint2mDaylight;
    private double avgApparentTemperatureDaylight;
    private double avgTemperature80mDaylight;
    private double avgTemperature120mDaylight;
    private double avgWindSpeed10mDaylight;
    private double avgWindSpeed80mDaylight;
    private double avgVisibilityDaylight;
    private double totalRainDaylight;
    private double totalShowersDaylight;
    private double totalSnowfallDaylight;

    private double windSpeed10mMPerS;
    private double windSpeed80mMPerS;
    private double temperature2mCelsius;
    private double apparentTemperatureCelsius;
    private double temperature80mCelsius;
    private double temperature120mCelsius;
    private double soilTemperature0cmCelsius;
    private double soilTemperature6cmCelsius;
    private double rainMm;
    private double showersMm;
    private double snowfallMm;

    // Время когда был добавлен объект в csv или базу данных
    private Instant fetchedAt;
}
