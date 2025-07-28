import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.Arguments;
import static org.junit.jupiter.api.Assertions.*;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Testcontainers
public class DatabaseTest {
    @Container
    static final GenericContainer<?> icebergContainer = new GenericContainer<>("tabulario/iceberg-rest:latest")
        .withExposedPorts(8181);

    @ParameterizedTest
    @MethodSource("provideColumnGroupDefs")
    void testTableBuilderPopulateTablesWithIcebergRest(TableBuilder.TableDef.ColumnGroupDef[] columnGroupDefs) throws Exception {
        // Get mapped port for REST
        String restUri = String.format("http://localhost:%d", icebergContainer.getMappedPort(8181));
        String warehouse = System.getProperty("java.io.tmpdir") + "/iceberg-warehouse-test";

        // Set up RESTCatalog
        Catalog catalog = new RESTCatalog();
        Map<String, String> props = new HashMap<>();
        props.put("uri", restUri);
        props.put("warehouse", warehouse);
        catalog.initialize("testCatalog", props);

        // Create a table in the catalog
        String namespace = "gdelt";
        String tableName = "test_table";
        Schema schema = new Schema(
            Types.NestedField.optional(1, "name", Types.StringType.get()),
            Types.NestedField.optional(2, "value", Types.IntegerType.get())
        );
        catalog.createTable(org.apache.iceberg.catalog.TableIdentifier.of(namespace, tableName), schema);

        TableBuilder.TableDef tableDef = new TableBuilder.TableDef(tableName, columnGroupDefs);
        TableBuilder tableBuilder = new TableBuilder(new String[] {"col_a", "col_b"}, new TableBuilder.TableDef[] {tableDef});

        // Prepare CSV data
        String csvData = "col_a,col_b\ntest1,100\ntest2,200";
        try (StringReader reader = new StringReader(csvData);
            CSVParser parser = CSVFormat.DEFAULT.builder().setHeader().get().parse(reader)) {
            List<CSVRecord> records = parser.getRecords();
            // Should not throw
            tableBuilder.populateTables(records, catalog);
        }
        
        Table table = catalog.loadTable(org.apache.iceberg.catalog.TableIdentifier.of(namespace, tableName));
        
        List<Record> actualRecords = new ArrayList<>();
        try (var reader = IcebergGenerics.read(table).build()) {
            for (Record record : reader) {
                actualRecords.add(record);
            }
        }
        
        assertEquals(2, actualRecords.size(), "Should have 2 records in the table");
        
        Record firstRecord = actualRecords.get(0);
        Record secondRecord = actualRecords.get(1);
        
        assertEquals("test1", firstRecord.getField("name"), "First record name should be 'test1'");
        assertEquals(100, firstRecord.getField("value"), "First record value should be 100");
        
        assertEquals("test2", secondRecord.getField("name"), "Second record name should be 'test2'");
        assertEquals(200, secondRecord.getField("value"), "Second record value should be 200");
    }

    static java.util.stream.Stream<Arguments> provideColumnGroupDefs() throws Exception {
        return java.util.stream.Stream.of(
            Arguments.of((Object) new TableBuilder.TableDef.ColumnGroupDef[] {
                new TableBuilder.TableDef.ColumnGroupDef(
                    new String[] {"name"},
                    "col_a",
                    new String[] {"str"},
                    new int[] {1},
                    false,
                    null,
                    true
                ),
                new TableBuilder.TableDef.ColumnGroupDef(
                    new String[] {"value"},
                    "col_b",
                    new String[] {"int"},
                    new int[] {2},
                    false,
                    null,
                    true
                )
            })
        );
    }
}
