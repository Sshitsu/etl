package com.shtisu.etl.comands;

import picocli.CommandLine;

import java.util.concurrent.Callable;

@CommandLine.Command(
        name = "etl-app",
        mixinStandardHelpOptions = true,
        version = "1.0",
        description = "ETL- приложение: json2db, json2csv, api2db, api2csv",
        subcommands = {
                Json2Db.class, Json2Csv.class,
                Api2Db.class,  Api2Csv.class
        }
)
public class Main implements Callable<Integer> {
    public static void main(String[] args) {
        int exitCode = new CommandLine(new Main()).execute(args);
        System.exit(exitCode);
    }
    @Override
    public Integer call() {
        System.out.println("Укажите команду. Например: etl-app json2db --file input.json");
        return 0;
    }
}
