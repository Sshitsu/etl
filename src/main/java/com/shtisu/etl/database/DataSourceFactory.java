package com.shtisu.etl.database;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import javax.sql.DataSource;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;


/**
 * Класс для создание подключения к базе данных, а также настройки параметров подключения
 */
public class DataSourceFactory {

    private static final HikariDataSource dateSource;

    private DataSourceFactory() { }
    public static DataSource getDataSource() {
        return dateSource;
    }


    static {
        try (InputStream in = DataSourceFactory.class
                .getClassLoader()
                .getResourceAsStream("application.properties")){
            if(in == null) throw new IllegalStateException("application.properties not found in classpath");

            Properties properties = new Properties();
            properties.load(in);

            HikariConfig config = new HikariConfig();
            config.setJdbcUrl(properties.getProperty("db.url"));
            config.setUsername(properties.getProperty("db.user"));
            config.setPassword(properties.getProperty("db.password"));
            config.setMaximumPoolSize(
                    Integer.parseInt(properties.getProperty("db.maximumPoolSize", "10"))
            );
            config.setConnectionTimeout(30000);
            config.setIdleTimeout(600000);
            config.setValidationTimeout(5000);

            dateSource = new HikariDataSource(config);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }





}
