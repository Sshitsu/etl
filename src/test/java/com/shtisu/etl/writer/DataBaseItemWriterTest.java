package com.shtisu.etl.writer;

import com.shtisu.etl.database.DataSourceFactory;
import com.shtisu.etl.model.FinalRecord;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.sql.DataSource;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.*;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DataBaseItemWriterTest {
    @TempDir
    Path tempDir;

    Path bloomFile;

    DataBaseItemWriter writer;

    @BeforeEach
    void setUp() throws Exception {
        // Подготовка BloomFilter-файла
        bloomFile = tempDir.resolve("data.bloom");

        this.writer = new DataBaseItemWriter(bloomFile, 100, 0.01);

        // Используем базу данных H2 in memory
        DataSource ds = DataSourceFactory.getDataSource();
        try (Connection conn = ds.getConnection();
             // Создаем таблицу
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE IF NOT EXISTS final_records (" +
                    "latitude DOUBLE, longitude DOUBLE, date DATE, sunrise_iso TIMESTAMP, sunset_iso TIMESTAMP, daylight_hours DOUBLE, " +
                    "avg_temperature_2m24h DOUBLE, avg_relative_humidity_2m24h DOUBLE, avg_dew_point_2m24h DOUBLE, avg_apparent_temperature_24h DOUBLE, " +
                    "avg_temperature_80m24h DOUBLE, avg_temperature_120m24h DOUBLE, avg_wind_speed_10m24h DOUBLE, avg_wind_speed_80m24h DOUBLE, " +
                    "avg_visibility_24h DOUBLE, total_rain_24h DOUBLE, total_showers_24h DOUBLE, total_snowfall_24h DOUBLE, " +
                    "avg_temperature_2m_daylight DOUBLE, avg_relative_humidity_2m_daylight DOUBLE, avg_dew_point_2m_daylight DOUBLE, avg_apparent_temperature_daylight DOUBLE, " +
                    "avg_temperature_80m_daylight DOUBLE, avg_temperature_120m_daylight DOUBLE, avg_wind_speed_10m_daylight DOUBLE, avg_wind_speed_80m_daylight DOUBLE, " +
                    "avg_visibility_daylight DOUBLE, total_rain_daylight DOUBLE, total_showers_daylight DOUBLE, total_snowfall_daylight DOUBLE, " +
                    "wind_speed_10m_mpers DOUBLE, wind_speed_80m_mpers DOUBLE, temperature_2m_celsius DOUBLE, apparent_temperature_celsius DOUBLE, " +
                    "temperature_80m_celsius DOUBLE, temperature_120m_celsius DOUBLE, soil_temperature_0cm_celsius DOUBLE, soil_temperature_6cm_celsius DOUBLE, " +
                    "rain_mm DOUBLE, showers_mm DOUBLE, snowfall_mm DOUBLE, fetched_at TIMESTAMP, " +
                    "PRIMARY KEY(latitude, longitude, date)" +
                    ")");
        }
    }

    @Test
    public void writeIntoDataBaseTableOnlyDistinctValue() throws IOException, SQLException {

        // Создаем и заполняем данными нашу модель
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

        DataBaseItemWriter dataBaseItemWriter = new DataBaseItemWriter(bloomFile, 100, 0.01);
        dataBaseItemWriter.write(records);

        // Проверяем что в базе данных тольк одна запись
        DataSource ds = DataSourceFactory.getDataSource();
        try (Connection conn = ds.getConnection();
             PreparedStatement ps = conn.prepareStatement("SELECT COUNT(*) FROM final_records");
             ResultSet rs = ps.executeQuery()) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1), "База данных должна содержать только одну запись");
        }


    }

}
