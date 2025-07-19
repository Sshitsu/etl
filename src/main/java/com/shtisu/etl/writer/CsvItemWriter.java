package com.shtisu.etl.writer;

import com.opencsv.CSVWriter;
import com.opencsv.ICSVWriter;
import com.shtisu.etl.model.FinalRecord;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.format.DateTimeFormatter;
import java.util.List;

public class CsvItemWriter {

    private final Path output;

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

    public CsvItemWriter(Path output) {
        this.output = output;
    }

    /**
     * Просто пишет один header и список записей в CSV.
     */
    public void write(List<FinalRecord> records) throws IOException {

        if (output.getParent() != null) {
            Files.createDirectories(output.getParent());
        }

        try (BufferedWriter bw = Files.newBufferedWriter(output);
             CSVWriter csv = new CSVWriter(
                     bw,
                     ';',
                     ICSVWriter.NO_QUOTE_CHARACTER,
                     ICSVWriter.DEFAULT_ESCAPE_CHARACTER,
                     ICSVWriter.DEFAULT_LINE_END
             )) {

            csv.writeNext(HEADER, false);

            DateTimeFormatter fmtDate   = DateTimeFormatter.ISO_DATE;

            for (FinalRecord r : records) {
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

                csv.writeNext(line, false);
            }
        }
    }
}
