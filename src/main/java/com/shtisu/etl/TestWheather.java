package com.shtisu.etl;

import com.shtisu.etl.client.OpenMeteoApiClient;
import com.shtisu.etl.model.FinalRecord;
import com.shtisu.etl.model.OpenMeteoResponse;
import com.shtisu.etl.processor.FinalRecordItemProcessor;
import com.shtisu.etl.writer.CsvItemWriter;
import com.shtisu.etl.writer.DataBaseItemWriter;
import lombok.Value;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

public class TestWheather {

    public static void main(String[] args) throws Exception {

        OpenMeteoApiClient client = new OpenMeteoApiClient("https://api.open-meteo.com");

        LocalDate start = LocalDate.of(2025,7,1), end = LocalDate.of(2025,7,1);
        List<FinalRecord> a = new ArrayList<>();


        FinalRecordItemProcessor finalRecordItemProcessor = new FinalRecordItemProcessor();


        Path csv = Path.of("D:\\IdeaProjects\\etl\\data\\weather.csv");
        Path DbBloom = Path.of("D:\\IdeaProjects\\etl\\cache\\DataBaseWeather.bloom");
        Path CsvBloom = Path.of("D:\\IdeaProjects\\etl\\cache\\CsvWeather.bloom");

        for (int i = 0; i < 10; i++){

            // рандомная широта от -90 до +90
            double lat = -90 + Math.random() * 180;
            // рандомная долгота от -180 до +180
            double lon = -180 + Math.random() * 360;

            OpenMeteoResponse resp = client.fetch(lat, lon, start, end);

            a.add(finalRecordItemProcessor.process(resp));
        }

        DataBaseItemWriter dbWriter = new DataBaseItemWriter(DbBloom, 100, 0.01);
        dbWriter.write(a);

        CsvItemWriter csvWriter = new CsvItemWriter(csv, CsvBloom, 100, 0.01);
        csvWriter.write(a);


    }
}
