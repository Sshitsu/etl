package com.shtisu.etl;

import com.shtisu.etl.client.OpenMeteoApiClient;
import com.shtisu.etl.model.FinalRecord;
import com.shtisu.etl.model.OpenMeteoResponse;
import com.shtisu.etl.parser.OpenMeteoApiJsonParser;
import com.shtisu.etl.processor.FinalRecordItemProcessor;
import com.shtisu.etl.writer.CsvItemWriter;
import com.shtisu.etl.writer.DataBaseItemWriter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.type.TypeReference;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * Простой CLI для запуска ETL-сценариев.
 */
public class WeatherCli {

    private static final Scanner SC = new Scanner(System.in);
    private static final DateTimeFormatter DATE_FMT = DateTimeFormatter.ISO_LOCAL_DATE;


    private static final Path DEFAULT_CSV_PATH = Paths.get("D:\\IdeaProjects\\etl\\data\\weather.csv");
    private static final Path DEFAULT_DB_BLOOM = Paths.get("D:\\IdeaProjects\\etl\\cache\\DataBaseWeather.bloom");
    private static final Path DEFAULT_CSV_BLOOM = Paths.get("D:\\IdeaProjects\\etl\\cache\\CsvWeather.bloom");
    private static final String DEFAULT_API_BASE = "https://api.open-meteo.com";
    private static final ZoneId ZONE = ZoneOffset.UTC;

    private final OpenMeteoApiClient client;
    private final FinalRecordItemProcessor processor;

    public WeatherCli(OpenMeteoApiClient client, FinalRecordItemProcessor processor) {
        this.client = client;
        this.processor = processor;
    }

    public static void main(String[] args) {
        WeatherCli app = new WeatherCli(
                new OpenMeteoApiClient(DEFAULT_API_BASE),
                new FinalRecordItemProcessor()
        );
        app.run();
    }

    private void run() {
        while (true) {
            printMenu();
            int choice = readInt("Ваш выбор: ");
            switch (choice) {
                case 1 -> apiToDb();
                case 2 -> apiToCsv();
                case 3 -> jsonFlow();
                case 0 -> {
                    System.out.println("Выход...");
                    return;
                }
                default -> System.out.println("Неизвестный пункт меню. Повторите ввод.");
            }
            System.out.println();
        }
    }

    private void printMenu() {
        System.out.println("""
                ===============================
                Что вы хотите сделать?
                1. API -> DataBase
                2. API -> CSV
                3. JSON -> DataBase -> CSV
                0. Выход
                ===============================""");
    }

    /**
     * Запись в базу данных из Api по заданному интервалу времени
     */
    private void apiToDb() {
        double lat = readDoubleOrRandom("Введите широту (latitude) [Enter — рандом]: ");
        double lon = readDoubleOrRandom("Введите долготу (longitude) [Enter — рандом]: ");
        LocalDate start = readDateOrDefault("Введите start date (YYYY-MM-DD) [Enter — сегодня]: ", LocalDate.now());
        LocalDate end   = readDateOrDefault("Введите end date   (YYYY-MM-DD) [Enter — сегодня]: ", LocalDate.now());

        List<FinalRecord> records = fetchFromApi(lat, lon, start, end);

        Path dbBloom = readPathOrDefault("Путь к bloom-файлу DB (Enter — по умолчанию): ", DEFAULT_DB_BLOOM);
        int batchSize = readIntWithDefault("Batch size для DB writer (по умолчанию 100 000): ", 100_000);
        double fpRate = readDoubleWithDefault("False positive rate для DB writer (по умолчанию 0.001): ", 0.001);

        try(DataBaseItemWriter dbWriter = new DataBaseItemWriter(dbBloom, batchSize, fpRate);)  {
            dbWriter.write(records);
            System.out.println("Готово: записано в DB.");
        } catch (Exception e) {
            System.err.println("Ошибка при записи в DB: " + e.getMessage());
            e.printStackTrace();
        }
    }
    /**
     * Запись в csv файл из Api по заданному интервалу времени
     */
    private void apiToCsv() {
        double lat = readDoubleOrRandom("Введите широту (latitude) [Enter — рандом]: ");
        double lon = readDoubleOrRandom("Введите долготу (longitude) [Enter — рандом]: ");
        LocalDate start = readDateOrDefault("Введите start date (YYYY-MM-DD) [Enter — сегодня]: ", LocalDate.now());
        LocalDate end   = readDateOrDefault("Введите end date   (YYYY-MM-DD) [Enter — сегодня]: ", LocalDate.now());

        List<FinalRecord> records = fetchFromApi(lat, lon, start, end);

        Path csvPath  = readPathOrDefault("Путь к CSV (Enter — по умолчанию): ", DEFAULT_CSV_PATH);
        Path csvBloom = readPathOrDefault("Путь к bloom-файлу CSV (Enter — по умолчанию): ", DEFAULT_CSV_BLOOM);
        int batchSize = readIntWithDefault("Batch size для CSV writer (по умолчанию 100_000): ", 100_000);
        double fpRate = readDoubleWithDefault("False positive rate для CSV writer (по умолчанию 0.01): ", 0.001);

        try (CsvItemWriter csvWriter = new CsvItemWriter(csvPath, csvBloom, batchSize, fpRate)) {
            csvWriter.write(records);
            System.out.println("Готово: записано в CSV.");
        } catch (Exception e) {
            System.err.println("Ошибка при записи в CSV: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Запись в csv или базу данных из файла Json
     */
    private void jsonFlow() {
        Path jsonPath = readPath("Путь к JSON-файлу с данными: ");
        List<FinalRecord> records;
        try {
            records = readRecordsFromJson(jsonPath);
        } catch (IOException e) {
            System.err.println("Не удалось прочитать JSON: " + e.getMessage());
            return;
        }

        System.out.println("""
            Куда писать данные из JSON?
            1. В DataBase
            2. В CSV
            0. Отмена
            """);

        int dest = readInt("Ваш выбор: ");
        switch (dest) {
            case 1 -> jsonToDb(records);
            case 2 -> jsonToCsv(records);
            case 0 -> System.out.println("Отмена.");
            default -> System.out.println("Неизвестный выбор.");
        }
    }

    /**
     * Записывает из Json -> базу данных
     */
    private void jsonToDb(List<FinalRecord> records) {
        Path dbBloom = readPathOrDefault("Путь к bloom-файлу DB (Enter — по умолчанию): ", DEFAULT_DB_BLOOM);
        int batch = readIntWithDefault("Batch size для DB (по умолчанию 100_000): ", 100_000);
        double fpRate = readDoubleWithDefault("FP rate для DB (по умолчанию 0.01): ", 0.001);

        try (DataBaseItemWriter dbWriter = new DataBaseItemWriter(dbBloom, batch, fpRate)) {
            dbWriter.write(records);
            System.out.println("Запись в DB завершена.");
        } catch (Exception e) {
            System.err.println("Ошибка при записи в DB: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Записывает из Json -> csv
     */
    private void jsonToCsv(List<FinalRecord> records) {
        Path csvPath  = readPathOrDefault("Путь к CSV-файлу (Enter — по умолчанию): ", DEFAULT_CSV_PATH);
        Path csvBloom = readPathOrDefault("Путь к bloom-файлу CSV (Enter — по умолчанию): ", DEFAULT_CSV_BLOOM);
        int batch = readIntWithDefault("Batch size для CSV (по умолчанию 100_000): ", 100_000);
        double fpRate = readDoubleWithDefault("FP rate для CSV (по умолчанию 0.01): ", 0.001);

        try (CsvItemWriter csvWriter = new CsvItemWriter(csvPath, csvBloom, batch, fpRate)) {
            csvWriter.write(records);
            System.out.println("Запись в CSV завершена.");
        } catch (Exception e) {
            System.err.println("Ошибка при записи в CSV: " + e.getMessage());
            e.printStackTrace();
        }
    }

    // ====== Вспомогательные методы ======

    /**
     * Просто парсит долготу и широту, если ничего не ввели будет по умолчанию (рандомно)
     */
    private double readDoubleOrRandom(String prompt) {
        System.out.print(prompt);
        String line = SC.nextLine().trim();
        if (line.isEmpty()) {
            return randomCoordForPrompt(prompt);
        }
        try {
            return Double.parseDouble(line);
        } catch (NumberFormatException e) {
            System.out.println("Некорректное число. Будет сгенерировано случайное значение.");
            return randomCoordForPrompt(prompt);
        }
    }
    /**
     * Определяет широта или долгота и возвращает рандомное значение для широты и долготы
     */
    private double randomCoordForPrompt(String prompt) {
        if (prompt.toLowerCase().contains("lat")) {
            return -90 + Math.random() * 180;
        } else {
            return -180 + Math.random() * 360;
        }
    }
    /**
     * Парсит дату если дата не валидная использует по умолчанию LocalDate.now() то есть сегоднешнего дня.
     */
    private LocalDate readDateOrDefault(String prompt, LocalDate def) {
        System.out.print(prompt);
        String line = SC.nextLine().trim();
        LocalDate today = LocalDate.now(ZONE);
        if (line.isEmpty()) return clampToToday(def, "Дата по умолчанию");
        try {
            LocalDate d = LocalDate.parse(line, DATE_FMT);
            return clampToToday(d, "Дата");
        } catch (Exception e) {
            System.out.println("Формат даты должен быть YYYY-MM-DD. Использую " + def);
            return clampToToday(def, "Дата по умолчанию");
        }
    }

    private LocalDate readDate(String prompt) {
        while (true) {
            System.out.print(prompt);
            String line = SC.nextLine().trim();
            try {
                LocalDate d = LocalDate.parse(line, DATE_FMT);
                return clampToToday(d, "Дата");
            } catch (Exception e) {
                System.out.println("Формат даты должен быть YYYY-MM-DD.");
            }
        }
    }

    /**
     * Перед запросом к Api проверяем корректность введеных данных
     */
    private List<FinalRecord> fetchFromApi(double lat, double lon, LocalDate start, LocalDate end) {
        LocalDate today = LocalDate.now(ZONE);
        start = clampToToday(start, "start date");
        end   = clampToToday(end,   "end date");

        if (end.isBefore(start)) {

            System.out.println("end date раньше start date. Меняю местами.");
            LocalDate tmp = start;
            start = end;
            end = tmp;
        }

        try {
            OpenMeteoResponse resp = client.fetch(lat, lon, start, end);
            return processor.processRange(resp);
        } catch (Exception e) {
            System.err.println("Ошибка получения/обработки данных из API: " + e.getMessage());
            e.printStackTrace();
            return Collections.emptyList();
        }
    }

    /**
     * Считывает данные из Json
     */
    private List<FinalRecord> readRecordsFromJson(Path jsonPath) throws IOException {
        String json = Files.readString(jsonPath);
        OpenMeteoResponse response =  OpenMeteoApiJsonParser.parse(json);
        return processor.processRange(response);
    }

    // ==== Вспомогательные методы считывания и валидаций (Ничего интересного)

    private int readInt(String prompt) {
        while (true) {
            System.out.print(prompt);
            String line = SC.nextLine().trim();
            try {
                return Integer.parseInt(line);
            } catch (NumberFormatException e) {
                System.out.println("Введите целое число.");
            }
        }
    }

    private int readIntWithDefault(String prompt, int def) {
        System.out.print(prompt);
        String line = SC.nextLine().trim();
        if (line.isEmpty()) return def;
        try {
            return Integer.parseInt(line);
        } catch (NumberFormatException e) {
            System.out.println("Некорректное число. Использую значение по умолчанию: " + def);
            return def;
        }
    }

    private double readDouble(String prompt) {
        while (true) {
            System.out.print(prompt);
            String line = SC.nextLine().trim();
            try {
                return Double.parseDouble(line);
            } catch (NumberFormatException e) {
                System.out.println("Введите число (double).");
            }
        }
    }

    private double readDoubleWithDefault(String prompt, double def) {
        System.out.print(prompt);
        String line = SC.nextLine().trim();
        if (line.isEmpty()) return def;
        try {
            return Double.parseDouble(line);
        } catch (NumberFormatException e) {
            System.out.println("Некорректное число. Использую значение по умолчанию: " + def);
            return def;
        }
    }

    private Path readPath(String prompt) {
        while (true) {
            System.out.print(prompt);
            String line = SC.nextLine().trim();
            if (!line.isEmpty()) {
                return Paths.get(line);
            }
            System.out.println("Путь не может быть пустым.");
        }
    }

    private Path readPathOrDefault(String prompt, Path def) {
        System.out.print(prompt);
        String line = SC.nextLine().trim();
        if (line.isEmpty()) return def;
        return Paths.get(line);
    }

    private LocalDate clampToToday(LocalDate date, String fieldName) {
        LocalDate today = LocalDate.now(ZONE);
        if (date.isAfter(today)) {
            System.out.println(fieldName + " не может быть в будущем. Использую " + today);
            return today;
        }
        return date;
    }
}
