package com.shtisu.etl.comands;

import com.shtisu.etl.client.OpenMeteoApiClient;
import com.shtisu.etl.processor.FinalRecordItemProcessor;
import picocli.CommandLine;

import java.time.LocalDate;
import java.util.concurrent.Callable;

@CommandLine.Command(
        name = "api2db",
        description = "Вы указываете date (star,end) промежуток времени и longitude, latitude для получения Api запроса"
)
public class Api2Db implements Callable<Integer> {

    @CommandLine.Option(names = "--start-date", required = false
    ,description = "Начала перода (yyyy-MM-dd)")
    LocalDate startDate;
    @CommandLine.Option(names = "--end-date", required = false
    ,description = "Конец периода (yyyy-MM-dd)")
    LocalDate endDate;
    @CommandLine.Option(names = "--lat", required = false
    ,description = "Широта")
    double latitude;
    @CommandLine.Option(names = "--lon", required = false
    ,description = "Долгота")
    double longitude;
    OpenMeteoApiClient client = new OpenMeteoApiClient("https://api.open-meteo.com");
    private final FinalRecordItemProcessor processor = new FinalRecordItemProcessor();


    @Override
    public Integer call() throws Exception {
//        try {
//            //validateDates();
//
//        }
        return 1;
    }
}
