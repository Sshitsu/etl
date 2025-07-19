package com.shtisu.etl;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import com.openmeteo.sdk.Model;


@SpringBootApplication
public class EtlApplication {

	public static void main(String[] args) {

		SpringApplication.run(EtlApplication.class, args);

	}

}
