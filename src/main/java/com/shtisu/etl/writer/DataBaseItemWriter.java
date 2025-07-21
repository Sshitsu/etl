package com.shtisu.etl.writer;


import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import com.shtisu.etl.database.DataSourceFactory;
import com.shtisu.etl.model.FinalRecord;

import javax.sql.DataSource;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.sql.*;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.List;

/**
 * DataBaseItemWriter с использованием BloomFilter для предотвращения вставки дубликатов
 * на основе ключа date+latitude+longitude.
 */
public class DataBaseItemWriter {
    // DataSource для подключенияк базе данных
    private final DataSource ds;
    private final Path bloomPath;

    // BloomFilter для предотвращения вставки дубликотаов, и сохранения уже вставленных значений
    private final BloomFilter<CharSequence> bloomFilter;

    // Форматер для форматирования даты
    private static final DateTimeFormatter FMT_DATE = DateTimeFormatter.ISO_DATE;

    // Список колонок таблицы final_records
    private static final String COLUMN_LIST =
            "latitude, longitude, date, sunrise_iso, sunset_iso, daylight_hours, " +
                    "avg_temperature_2m24h, avg_relative_humidity_2m24h, avg_dew_point_2m24h, avg_apparent_temperature_24h, " +
                    "avg_temperature_80m24h, avg_temperature_120m24h, avg_wind_speed_10m24h, avg_wind_speed_80m24h, " +
                    "avg_visibility_24h, total_rain_24h, total_showers_24h, total_snowfall_24h, " +
                    "avg_temperature_2m_daylight, avg_relative_humidity_2m_daylight, avg_dew_point_2m_daylight, avg_apparent_temperature_daylight, " +
                    "avg_temperature_80m_daylight, avg_temperature_120m_daylight, avg_wind_speed_10m_daylight, avg_wind_speed_80m_daylight, " +
                    "avg_visibility_daylight, total_rain_daylight, total_showers_daylight, total_snowfall_daylight, " +
                    "wind_speed_10m_mpers, wind_speed_80m_mpers, temperature_2m_celsius, apparent_temperature_celsius, " +
                    "temperature_80m_celsius, temperature_120m_celsius, soil_temperature_0cm_celsius, soil_temperature_6cm_celsius, " +
                    "rain_mm, showers_mm, snowfall_mm, fetched_at";

    // Генерим 42 знака "?" через Collections.nCopies
    private static final String PLACEHOLDERS = String.join(
            ", ", Collections.nCopies(COLUMN_LIST.split(",").length, "?")
    );

    private static final String INSERT_SQL =
                    "INSERT INTO final_records (" + COLUMN_LIST + ") " +
                    "VALUES (" + PLACEHOLDERS + ") " +
                    "ON CONFLICT  DO NOTHING";

    /**
     * @param bloomPath       путь к файлу сериализации BloomFilter
     * @param expectedEntries оценка числа уникальных записей для Bloom‑фильтра
     * @param fpp             допустимая ложноположительная вероятность (0–1)
     */
    public DataBaseItemWriter(Path bloomPath,
                              long expectedEntries,
                              double fpp) throws IOException{
        this.ds = DataSourceFactory.getDataSource();
        this.bloomPath = bloomPath;

        if(bloomPath.getParent() == null){
            Files.createDirectories(bloomPath.getParent());
        }

        boolean exists = Files.exists(bloomPath) && Files.size(bloomPath) > 0;
        BloomFilter<CharSequence> filter;
        if (exists) {
            try (InputStream is = Files.newInputStream(bloomPath, StandardOpenOption.READ)) {
                filter = BloomFilter.readFrom(is, Funnels.stringFunnel(StandardCharsets.UTF_8));
            } catch (IOException e) {
                // повреждённый файл → стартуем с нуля
                filter = BloomFilter.create(
                        Funnels.stringFunnel(StandardCharsets.UTF_8),
                        expectedEntries, fpp
                );
            }
        } else {
            filter = BloomFilter.create(
                    Funnels.stringFunnel(StandardCharsets.UTF_8),
                    expectedEntries, fpp
            );
        }
        this.bloomFilter = filter;
    }




    /**
     * Вставляет список FinalRecord в БД, пропуская уже виденные по BloomFilter.
     * После вставки сериализует BloomFilter на диск.
     */
    public void write(List<FinalRecord> records) throws SQLException, IOException {
        try (Connection conn = ds.getConnection();
             PreparedStatement ps = conn.prepareStatement(INSERT_SQL)) {
            conn.setAutoCommit(false);

            for (FinalRecord r : records) {
                String key = buildKey(r);

                if (!bloomFilter.mightContain(key)) {
                    int idx = 1;

                    ps.setDouble(idx++, r.getLatitude());
                    ps.setDouble(idx++, r.getLongitude());
                    ps.setDate  (idx++, Date.valueOf((r.getDate())));
                    ps.setTimestamp(idx++, Timestamp.from(r.getSunriseIso()));
                    ps.setTimestamp(idx++, Timestamp.from(r.getSunsetIso()));
                    ps.setDouble(idx++, r.getDaylightHours());

                    ps.setDouble(idx++, r.getAvgTemperature2m24h());
                    ps.setDouble(idx++, r.getAvgRelativeHumidity2m24h());
                    ps.setDouble(idx++, r.getAvgDewPoint2m24h());
                    ps.setDouble(idx++, r.getAvgApparentTemperature24h());
                    ps.setDouble(idx++, r.getAvgTemperature80m24h());
                    ps.setDouble(idx++, r.getAvgTemperature120m24h());
                    ps.setDouble(idx++, r.getAvgWindSpeed10m24h());
                    ps.setDouble(idx++, r.getAvgWindSpeed80m24h());
                    ps.setDouble(idx++, r.getAvgVisibility24h());
                    ps.setDouble(idx++, r.getTotalRain24h());
                    ps.setDouble(idx++, r.getTotalShowers24h());
                    ps.setDouble(idx++, r.getTotalSnowfall24h());

                    ps.setDouble(idx++, r.getAvgTemperature2mDaylight());
                    ps.setDouble(idx++, r.getAvgRelativeHumidity2mDaylight());
                    ps.setDouble(idx++, r.getAvgDewPoint2mDaylight());
                    ps.setDouble(idx++, r.getAvgApparentTemperatureDaylight());
                    ps.setDouble(idx++, r.getAvgTemperature80mDaylight());
                    ps.setDouble(idx++, r.getAvgTemperature120mDaylight());
                    ps.setDouble(idx++, r.getAvgWindSpeed10mDaylight());
                    ps.setDouble(idx++, r.getAvgWindSpeed80mDaylight());
                    ps.setDouble(idx++, r.getAvgVisibilityDaylight());
                    ps.setDouble(idx++, r.getTotalRainDaylight());
                    ps.setDouble(idx++, r.getTotalShowersDaylight());
                    ps.setDouble(idx++, r.getTotalSnowfallDaylight());

                    ps.setDouble(idx++, r.getWindSpeed10mMPerS());
                    ps.setDouble(idx++, r.getWindSpeed80mMPerS());
                    ps.setDouble(idx++, r.getTemperature2mCelsius());
                    ps.setDouble(idx++, r.getApparentTemperatureCelsius());
                    ps.setDouble(idx++, r.getTemperature80mCelsius());
                    ps.setDouble(idx++, r.getTemperature120mCelsius());
                    ps.setDouble(idx++, r.getSoilTemperature0cmCelsius());
                    ps.setDouble(idx++, r.getSoilTemperature6cmCelsius());
                    ps.setDouble(idx++, r.getRainMm());
                    ps.setDouble(idx++, r.getShowersMm());
                    ps.setDouble(idx++, r.getSnowfallMm());

                    ps.setTimestamp(idx, Timestamp.from(r.getFetchedAt()));
                    ps.addBatch();

                    bloomFilter.put(key);
                }
            }

            ps.executeBatch();
            conn.commit();
        }

        // Сохраняем состояние BloomFilter
        try (OutputStream os = Files.newOutputStream(bloomPath,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING)) {
            bloomFilter.writeTo(os);
        }
    }

    /** Ключ для фильтрации: date:lat:lon */
    private String buildKey(FinalRecord r) {
        return r.getDate().format(FMT_DATE)
                + ":" + r.getLatitude()
                + ":" + r.getLongitude();
    }


}
