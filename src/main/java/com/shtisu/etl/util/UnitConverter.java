package com.shtisu.etl.util;

/**
 * Вспомогательный класс для конвертаций данных
 */
public class UnitConverter {

    /** Переводит температуру из Fahrenheit в Celsius.  */
    public static double fahrenheitToCelsius(double f) {
        return (f - 32) * 5.0 / 9.0;
    }

    /** Переводит длину осадков из дюймов (inch) в миллиметры (mm). */
    public static double inchToMillimeter(double inches) {
        return inches * 25.4;
    }

    /** Переводит скорость ветра из узлов (knots) в метры в секунду (m/s). */
    public static double knotsToMetersPerSecond(double knots) {
        return knots * 0.514444;
    }

    /** Переводит расстояние из футов (feet) в метры (m). */
    public static double feetToMeters(double feet) {
        return feet * 0.3048;
    }

}
