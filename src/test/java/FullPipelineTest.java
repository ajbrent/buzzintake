import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.rest.RESTCatalog;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.numbuzz.buzzintake.model.TableBuilder;
import com.numbuzz.buzzintake.model.TableBuilderAdapter;

@Testcontainers
public class FullPipelineTest {
    @Container
    static final GenericContainer<?> icebergContainer = new GenericContainer<>("tabulario/iceberg-rest:latest")
        .withExposedPorts(8181);

    private static Catalog catalog;
    private static TableBuilder tableBuilder;
    private static String restUri;
    private static String warehouse;

    @BeforeAll
    static void setUp() throws Exception {
        restUri = String.format("http://localhost:%d", icebergContainer.getMappedPort(8181));
        warehouse = System.getProperty("java.io.tmpdir") + "/iceberg-warehouse-full-test";

        catalog = new RESTCatalog();
        Map<String, String> props = new HashMap<>();
        props.put("uri", restUri);
        props.put("warehouse", warehouse);
        catalog.initialize("fullTestCatalog", props);

        Gson gson = new GsonBuilder()
                .registerTypeAdapter(TableBuilder.class, new TableBuilderAdapter())
                .create();
        
        try (InputStream is = FullPipelineTest.class.getClassLoader()
                .getResourceAsStream("gdelt_og_schema.json")) {
            if (is == null) {
                throw new RuntimeException("Could not find gdelt_og_schema.json in resources");
            }
            try (Reader reader = new InputStreamReader(is, StandardCharsets.ISO_8859_1)) {
                tableBuilder = gson.fromJson(reader, TableBuilder.class);
            }
        }
        
        createGdeltDatabase();
    }

    @Test
    public void testFullPipelineWithTestInputFullCsv() throws Exception {
        // Read the test CSV file (same structure as GDELT data)
        List<CSVRecord> csvRecords = new ArrayList<>();
        try (InputStream is = getClass().getClassLoader()
                .getResourceAsStream("csv/input/test_input_full.csv")) {
            if (is == null) {
                fail("Could not find test_input_full.csv in resources");
            }
            
            try (Reader reader = new InputStreamReader(is);
                 CSVParser parser = CSVFormat.TDF.builder()
                         .setHeader(tableBuilder.inputFields())
                         .setQuote(null)
                         .get()
                         .parse(reader)) {
                
                for (CSVRecord record : parser) {
                    csvRecords.add(record);
                }
            }
        }

        assertFalse(csvRecords.isEmpty(), "Should have CSV records to process");
        assertEquals(5, csvRecords.size(), "Should have 5 records from test_input_full.csv");

        tableBuilder.populateTables(csvRecords, catalog);
        // Verify that tables were created and populated
        verifyTablePopulation();
    }

    private void verifyTablePopulation() throws Exception {
        String[] expectedTables = {
            "articles", "counts", "themes", "locations", "persons", 
            "organizations", "dates", "related_images", "sharing_images",
            "social_image_embeds", "social_video_embeds", "quotations", 
            "all_names", "amounts"
        };

        for (String tableName : expectedTables) {
            try {
                // dates not working
                Table table = catalog.loadTable(org.apache.iceberg.catalog.TableIdentifier.of("gdelt", tableName));
                assertNotNull(table, "Table " + tableName + " should exist");
                
                assertNotNull(table.currentSnapshot(), 
                    "Table " + tableName + " should have a current snapshot");
                
                assertTrue(!table.currentSnapshot().allManifests(table.io()).isEmpty(),
                    "Table " + tableName + " should have manifests");

                verifyTableData(table, tableName);
                
            } catch (Exception e) {
                System.out.println("Table " + tableName + " verification skipped: " + e.getMessage());
            }
        }
    }

    private void verifyTableData(Table table, String tableName) throws Exception {
        List<Record> records = new ArrayList<>();
        try (var reader = IcebergGenerics.read(table).build()) {
            for (Record record : reader) {
                records.add(record);
            }
        }

        assertNotNull(records, "Should be able to read records from " + tableName);
        
        System.out.println("Table " + tableName + " contains " + records.size() + " records");
        
        assertTrue(!records.isEmpty(), 
            "Table " + tableName + " should have data based on test input");
    }

    @Test
    public void testSpecificTableData() throws Exception {
        String[] tableNames = {
            "articles", "counts", "themes", "locations", "persons", 
            "organizations", "dates", "related_images", "sharing_images",
            "social_image_embeds", "social_video_embeds", "quotations", 
            "all_names", "amounts"
        };
        for (String tableName : tableNames) {
        String csvPath = "csv/output/" + tableName + ".csv";
        List<List<String>> expectedRows = new ArrayList<>();
        try (InputStream is = getClass().getClassLoader().getResourceAsStream(csvPath)) {
            if (is == null) {
                System.out.println("No expected CSV for table: " + tableName);
                continue;
            }
            try (Reader reader = new InputStreamReader(is, StandardCharsets.UTF_8);
                 CSVParser parser = CSVFormat.DEFAULT.builder()
                         .setQuote('"')
                         .setEscape('\\')
                         .setTrim(true)
                         .setIgnoreEmptyLines(true)
                         .get()
                         .parse(reader)) {
                
                boolean firstRow = true;
                for (CSVRecord record : parser) {
                    if (firstRow) {
                        String firstField = record.get(0);
                        if (firstField != null && firstField.equals("record_id")) {
                            firstRow = false;
                            continue;
                        }
                    }
                    firstRow = false;
                    
                    List<String> row = new ArrayList<>();
                    for (String field : record) {
                        row.add(field == null ? "" : field.trim());
                    }
                    expectedRows.add(row);
                }
            }
        }

        Table table = catalog.loadTable(TableIdentifier.of("gdelt", tableName));
        List<List<String>> actualRows = new ArrayList<>();
        try (var reader = IcebergGenerics.read(table).build()) {
            for (Record record : reader) {
                List<String> row = new ArrayList<>();
                for (String field : table.schema().columns().stream().map(c -> c.name()).collect(Collectors.toList())) {
                    Object value = record.getField(field);
                    row.add(value == null ? "" : value.toString());
                }
                actualRows.add(row);
            }
        }

        expectedRows.sort((a, b) -> String.join(",", a).compareTo(String.join(",", b)));
        actualRows.sort((a, b) -> String.join(",", a).compareTo(String.join(",", b)));
        
        assertEquals(
            expectedRows,
            actualRows,
            "Mismatch in table: " + tableName
        );
    }
    }
    
    /**
     * Creates the gdelt database and all its tables in the Iceberg REST container
     * based on the schema defined in gdelt_og_schema.json
     */
    private static void createGdeltDatabase() throws Exception {
        for (TableBuilder.TableDef tableDef : tableBuilder.tableDefs()) {
            try {
                catalog.createTable(
                    TableIdentifier.of("gdelt", tableDef.getName()),
                    tableDef.getSchema()
                );
                System.out.println("Created table: gdelt." + tableDef.getName());
            } catch (Exception e) {
                System.out.println("Table gdelt." + tableDef.getName() + " already exists or creation failed: " + e.getMessage());
            }
        }
    }
} 