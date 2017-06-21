package com.github.record_stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.camel.ConsumerTemplate;
import org.apache.camel.test.spring.CamelSpringBootRunner;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.JdbcTest;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.jdbc.Sql;
import org.springframework.test.context.jdbc.Sql.ExecutionPhase;
import org.springframework.test.context.jdbc.SqlConfig;
import org.springframework.test.context.jdbc.SqlConfig.TransactionMode;

@RunWith(CamelSpringBootRunner.class)
@SpringBootTest
@JdbcTest
@Sql(scripts = { "/test-schema.sql", "/test-user-data.sql" }, config = @SqlConfig(transactionMode = TransactionMode.ISOLATED))
@Sql(scripts = "/drop-schema.sql", executionPhase = ExecutionPhase.AFTER_TEST_METHOD)
public class RecordStreamApplicationTests {
	private static String SEDA_EP = "seda://testEndPoint";
	@Autowired
	private DataSource dataSource;

	@Autowired
	private ConsumerTemplate consumer;

	@Test
	public void verifySpringBootTestData() throws SQLException {
		try (Connection conn = dataSource.getConnection()) {
			try (Statement s = conn.createStatement()) {
				try (ResultSet rs = s
						.executeQuery("select * from transactions")) {
					ResultSetMetaData meta = rs.getMetaData();
					int size = 0;
					while (rs.next()) {
						Map<String, Object> record = new HashMap<>();
						for (int c = meta.getColumnCount(); c > 0; c--) {
							record.put(meta.getColumnName(c), rs.getObject(c));
						}
						System.out.println(record);
						assertEquals(5, record.size());
						size++;
					}
					assertEquals(12, size);
				}
			}
		}
	}

	@Test
	public void verifyCamelRoute() throws SQLException {
		for (int i = 1; i < 13; i++) {
			String body = consumer.receiveBody(SEDA_EP, 1000, String.class);
			assertNotNull("Iteration" + i, body);
			assertTrue("Searching for AMOUNT\":" + (i + 100) + " in " + body,
					body.contains("AMOUNT\":" + (i + 100)));
		}
	}

	@BeforeClass
	public static void setup() {
		System.setProperty("jdbc.statement", "select * from transactions");
		System.setProperty("apache.camel.to", SEDA_EP);
	}

}
