package com.github.record_stream;

import org.apache.camel.model.dataformat.JsonLibrary;
import org.apache.camel.spring.SpringRouteBuilder;
import org.apache.camel.spring.boot.CamelAutoConfiguration;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@SpringBootApplication
@Import(CamelAutoConfiguration.class) // This import should not be needed
@Configuration
public class RecordStreamApplication extends SpringRouteBuilder {

	@Override
	public void configure() throws Exception {
		from("sql:{{jdbc.statement}}?outputType=StreamList").split(body()).marshal().json(JsonLibrary.Gson).to("{{apache.camel.to}}");
	}

	public static void main(String[] args) {
		SpringApplication.run(RecordStreamApplication.class, args);
	}
}
