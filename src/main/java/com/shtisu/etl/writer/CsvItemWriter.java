package com.shtisu.etl.writer;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import com.opencsv.CSVWriter;
import com.opencsv.ICSVWriter;
import com.shtisu.etl.model.FinalRecord;
import org.jetbrains.annotations.NotNull;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.format.DateTimeFormatter;
import java.util.List;

/**
 * CsvItemWriter с использованием BloomFilter для предотвращения вставки дубликатов на основе ключа date+latitude+longitude.
 * BloomFilter сохраняется на диск, чтобы не сканировать CSV-файл целиком при каждом запуске. Далее за сложность O(1) (в лучшем случае) мы можем обратиться к ключу
 * и узнать если у нас такой дубликат, это намного эфективнее чем каждый раз заново считывать csv файл и за O(n) проходиться по всем его значениям
 * при больших колличествах данных O(n) может работать слишком долгго. Но у способа с BloomFilter так же есть недостатки возможны коллизий при получений хэша у ключа,
 * а также придется выделить память для хранения ключей на диске, но даже для огромного файла ключи не будут весить слишком много
 */
public class CsvItemWriter {


    private final Path outputCsvPath;
    private final Path bloomPath;

    // BloomFilter для предотвращения вставки дубликотаов, и сохранения уже вставленных значений
    private final BloomFilter<CharSequence> bloomFilter;

    // Форматер для форматирвоания даты
    private final DateTimeFormatter fmtDate = DateTimeFormatter.ISO_DATE;

    // Список колоннок
    private static final String[] HEADER = {
            "latitude","longitude","date","sunriseIso","sunsetIso","daylightHours",
            "avgTemperature2m24h","avgRelativeHumidity2m24h","avgDewPoint2m24h","avgApparentTemperature24h",
            "avgTemperature80m24h","avgTemperature120m24h","avgWindSpeed10m24h","avgWindSpeed80m24h",
            "avgVisibility24h","totalRain24h","totalShowers24h","totalSnowfall24h",
            "avgTemperature2mDaylight","avgRelativeHumidity2mDaylight","avgDewPoint2mDaylight",
            "avgApparentTemperatureDaylight","avgTemperature80mDaylight","avgTemperature120mDaylight",
            "avgWindSpeed10mDaylight","avgWindSpeed80mDaylight","avgVisibilityDaylight",
            "totalRainDaylight","totalShowersDaylight","totalSnowfallDaylight",
            "windSpeed10mMPerS","windSpeed80mMPerS","temperature2mCelsius","apparentTemperatureCelsius",
            "temperature80mCelsius","temperature120mCelsius","soilTemperature0cmCelsius","soilTemperature6cmCelsius",
            "rainMm","showersMm","snowfallMm","fetchedAt"
    };

    /**
     * Инициализирует пути для CSV и BloomFilter.
     * Если CSV ещё не существует — создаёт файл и записывает заголовок.
     *
     * @param outputCsvPath         путь к CSV-файлу
     *
     * @param bloomPath             путь к файлу для сериализации BloomFilter
     *
     * @param expectedEntries       это не жёсткий "лимит" количества ключей, а оценка размера для оптимизации Bloom‑фильтра.
     *                              Вы можете вставить и больше ключей, чем expectedEntries без ошибок или выбросов. Но при этом вероятность
     *                              ложноположительных срабатываний начнёт расти выше заданного fpp.
     *
     * @param fpp                   (fals positive probability) - это желаемая максимально допустимая вероятность ложного срабатывания => "есть запись" при этом это новая запись
     *
     *
     * Пример: если вы ожидаете до 1 000 000 уникальных записей за весь запуск, указывайте expectedEntries = 1 000 000, fpp = 0.01.
     * Тогда при вставке, скажем, 500 000 записей фактическая ложноположительная вероятность будет даже ниже 1%;
     * при 2 000 000 — выше, но фильтр всё равно продолжит работать, просто будет пропускать часть реально новых как «уже виденные».
     * В данной работе я задам эти значения просто большими, но в реальном ETLPipline при огромном колличестве данных, эти переменные надо переопределять динамический
     * при увеличений данных или использовать другой механизм
     */
    public CsvItemWriter(@NotNull Path outputCsvPath,
                         Path bloomPath,
                         long expectedEntries,
                         double fpp) throws IOException {
        this.outputCsvPath = outputCsvPath;
        this.bloomPath = bloomPath;

        if (outputCsvPath.getParent() != null) {
            Files.createDirectories(outputCsvPath.getParent());
        }
        if (bloomPath.getParent() != null) {
            Files.createDirectories(bloomPath.getParent());
        }

        boolean bloomFileExists   = Files.exists(bloomPath);
        boolean bloomFileNotEmpty = bloomFileExists && Files.size(bloomPath) > 0;

        // Use a local variable to avoid multiple assignments to the final field
        BloomFilter<CharSequence> filter;
        if (bloomFileNotEmpty) {
            try (InputStream is = Files.newInputStream(bloomPath, StandardOpenOption.READ)) {
                filter = BloomFilter.readFrom(
                        is,
                        Funnels.stringFunnel(StandardCharsets.UTF_8)
                );
            } catch (IOException e) {
                // corrupted or truncated → fall back to a fresh filter
                filter = BloomFilter.create(
                        Funnels.stringFunnel(StandardCharsets.UTF_8),
                        expectedEntries,
                        fpp
                );
            }
        } else {
            // file absent or empty → start fresh
            filter = BloomFilter.create(
                    Funnels.stringFunnel(StandardCharsets.UTF_8),
                    expectedEntries,
                    fpp
            );
        }
        this.bloomFilter = filter;

        // если CSV не существует, создаём и пишем header
        if (!Files.exists(outputCsvPath)) {
            try (BufferedWriter bw = Files.newBufferedWriter(outputCsvPath,
                    StandardOpenOption.CREATE);
                 CSVWriter csv = new CSVWriter(
                         bw,
                         ';',
                         ICSVWriter.NO_QUOTE_CHARACTER,
                         ICSVWriter.DEFAULT_ESCAPE_CHARACTER,
                         ICSVWriter.DEFAULT_LINE_END
                 )) {
                csv.writeNext(HEADER, false);
            }
        }
    }

    /**
     * Дописывает новые записи, проверяя BloomFilter перед записью.
     */
    public void write(@NotNull List<FinalRecord> records) throws IOException {
        try (BufferedWriter bw = Files.newBufferedWriter(outputCsvPath,
                StandardOpenOption.APPEND);
             CSVWriter csv = new CSVWriter(bw, ';',
                     ICSVWriter.NO_QUOTE_CHARACTER,
                     ICSVWriter.DEFAULT_ESCAPE_CHARACTER,
                     ICSVWriter.DEFAULT_LINE_END)) {
            for (FinalRecord r : records) {
                String key = buildKey(r);
                // проверяем, возможно ли присутствие ключа
                if (!bloomFilter.mightContain(key)) {
                    String[] line = buildLine(r);
                    csv.writeNext(line, false);
                    bloomFilter.put(key);
                }
            }
        }
        // сохраняем BloomFilter на диск
        try (OutputStream os = Files.newOutputStream(bloomPath,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING)) {
            bloomFilter.writeTo(os);
        }
    }


    /**
     * Сздаем ключ на основе date + latitude + longitude, который будет использоваться в BloomFilter
     */
    @NotNull
    private String buildKey(@NotNull FinalRecord r) {
        return r.getDate().format(fmtDate)
                + ":" + r.getLatitude()
                + ":" + r.getLongitude();
    }

    /**
     * Приватный метод который создает строку которую мы вставем в csv
     */
    @NotNull
    private String[] buildLine(@NotNull FinalRecord r) {
        String[] line = new String[HEADER.length];
        int i = 0;

        line[i++] = String.valueOf(r.getLatitude());
        line[i++] = String.valueOf(r.getLongitude());
        line[i++] = r.getDate().format(fmtDate);
        line[i++] = r.getSunriseIso().toString();
        line[i++] = r.getSunsetIso().toString();
        line[i++] = String.valueOf(r.getDaylightHours());

        line[i++] = String.valueOf(r.getAvgTemperature2m24h());
        line[i++] = String.valueOf(r.getAvgRelativeHumidity2m24h());
        line[i++] = String.valueOf(r.getAvgDewPoint2m24h());
        line[i++] = String.valueOf(r.getAvgApparentTemperature24h());
        line[i++] = String.valueOf(r.getAvgTemperature80m24h());
        line[i++] = String.valueOf(r.getAvgTemperature120m24h());
        line[i++] = String.valueOf(r.getAvgWindSpeed10m24h());
        line[i++] = String.valueOf(r.getAvgWindSpeed80m24h());
        line[i++] = String.valueOf(r.getAvgVisibility24h());

        line[i++] = String.valueOf(r.getTotalRain24h());
        line[i++] = String.valueOf(r.getTotalShowers24h());
        line[i++] = String.valueOf(r.getTotalSnowfall24h());

        line[i++] = String.valueOf(r.getAvgTemperature2mDaylight());
        line[i++] = String.valueOf(r.getAvgRelativeHumidity2mDaylight());
        line[i++] = String.valueOf(r.getAvgDewPoint2mDaylight());
        line[i++] = String.valueOf(r.getAvgApparentTemperatureDaylight());
        line[i++] = String.valueOf(r.getAvgTemperature80mDaylight());
        line[i++] = String.valueOf(r.getAvgTemperature120mDaylight());
        line[i++] = String.valueOf(r.getAvgWindSpeed10mDaylight());
        line[i++] = String.valueOf(r.getAvgWindSpeed80mDaylight());
        line[i++] = String.valueOf(r.getAvgVisibilityDaylight());

        line[i++] = String.valueOf(r.getTotalRainDaylight());
        line[i++] = String.valueOf(r.getTotalShowersDaylight());
        line[i++] = String.valueOf(r.getTotalSnowfallDaylight());

        line[i++] = String.valueOf(r.getWindSpeed10mMPerS());
        line[i++] = String.valueOf(r.getWindSpeed80mMPerS());

        line[i++] = String.valueOf(r.getTemperature2mCelsius());
        line[i++] = String.valueOf(r.getApparentTemperatureCelsius());
        line[i++] = String.valueOf(r.getTemperature80mCelsius());
        line[i++] = String.valueOf(r.getTemperature120mCelsius());
        line[i++] = String.valueOf(r.getSoilTemperature0cmCelsius());
        line[i++] = String.valueOf(r.getSoilTemperature6cmCelsius());

        line[i++] = String.valueOf(r.getRainMm());
        line[i++] = String.valueOf(r.getShowersMm());
        line[i++] = String.valueOf(r.getSnowfallMm());

        line[i] = r.getFetchedAt().toString();

        return line;
    }
}
