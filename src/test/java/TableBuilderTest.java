import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.iceberg.data.GenericRecord;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.numbuzz.buzzintake.model.TableBuilder;

public class TableBuilderTest {

    public void testPopulateTables() {

    }

    public static Stream<Arguments> provideUnpackRowTestCases() throws IOException {
        return Stream.of(
                Arguments.of(
                        "Simple test with single value",
                        new TableBuilder.TableDef.ColumnGroupDef[]{
                                new TableBuilder.TableDef.ColumnGroupDef(
                                        new String[]{"simple_field"},
                                        "col_a",
                                        new String[]{"str"},
                                        new int[]{1},
                                        false,
                                        null,
                                        true
                                )
                        },
                        "single_test_table",
                        "col_a\nsimple_value",
                        1,
                        1,
                        List.of(
                                Map.of("simple_field", "simple_value")
                        )
                ),
                Arguments.of(
                        "Complex test with string and regex fields",
                        new TableBuilder.TableDef.ColumnGroupDef[]{
                                new TableBuilder.TableDef.ColumnGroupDef(
                                        new String[]{"name"},
                                        "col_a",
                                        new String[]{"str"},
                                        new int[]{1},
                                        false,
                                        null,
                                        true
                                ),
                                new TableBuilder.TableDef.ColumnGroupDef(
                                        new String[]{"value", "count"},
                                        "col_b",
                                        new String[]{"float", "int"},
                                        new int[]{2, 3},
                                        true,
                                        "(-?\\d*(?:\\.\\d*)?),(-?\\d*)(?:;|$)",
                                        false
                                )
                        },
                        "test_table",
                        "col_a,col_b\nalpha,\"1.34,2;-13.23,27;32.1,-8\"",
                        2,
                        3,
                        List.of(
                                Map.of("name", "alpha", "value", 1.34f, "count", 2),
                                Map.of("name", "alpha", "value", -13.23f, "count", 27),
                                Map.of("name", "alpha", "value", 32.1f, "count", -8)
                        )
                ),
                Arguments.of(
                        "Test with multiple regex matches",
                        new TableBuilder.TableDef.ColumnGroupDef[]{
                                new TableBuilder.TableDef.ColumnGroupDef(
                                        new String[]{"value", "count"},
                                        "col_b",
                                        new String[]{"float", "int"},
                                        new int[]{2, 3},
                                        true,
                                        "(-?\\d*(?:\\.\\d*)?),(-?\\d*)(?:;|$)",
                                        false
                                )
                        },
                        "multi_match_table",
                        "col_b\n\"100.33,-3;200,2\"",
                        1,
                        2,
                        List.of(
                                Map.of("value", 100.33f, "count", -3),
                                Map.of("value", 200f, "count", 2)
                        )
                )
        );
    }

    @ParameterizedTest
    @MethodSource("provideUnpackRowTestCases")
    public void testTableDefUnpackRow(
            String testDescription,
            TableBuilder.TableDef.ColumnGroupDef[] columnGroupDefs,
            String tableName,
            String csvData,
            int expectedColumnGroupCount,
            int expectedRecordCount,
            List<Map<String, Object>> expectedRecords
    ) throws IOException {
        TableBuilder.TableDef tableDef = new TableBuilder.TableDef(tableName, columnGroupDefs);

        try (StringReader reader = new StringReader(csvData);
             CSVParser parser = CSVFormat.DEFAULT.builder()
                     .setHeader()
                     .get()
                     .parse(reader)) {
            
            CSVRecord csvRecord = parser.iterator().next();

            tableDef.unpackRow(csvRecord);
            
            // Verify that records are populated correctly
            List<GenericRecord> records = tableDef.getRecords();
            assertNotNull(records, "Records should not be null");
            assertEquals(expectedRecordCount, records.size(), 
                    "Should create " + expectedRecordCount + " records for: " + testDescription);
            
            for (int i = 0; i < expectedRecords.size(); i++) {
                GenericRecord record = records.get(i);
                Map<String, Object> expectedRecord = expectedRecords.get(i);
                
                for (Map.Entry<String, Object> entry : expectedRecord.entrySet()) {
                    String fieldName = entry.getKey();
                    Object expectedValue = entry.getValue();
                    Object actualValue = record.getField(fieldName);
                    
                    assertEquals(expectedValue, actualValue, 
                            String.format("Record %d, field '%s' mismatch for test: %s", 
                                    i, fieldName, testDescription));
                }
            }
        }
    }

    public static Stream<Arguments> provideCgParams() {
        return Stream.of(
                Arguments.of(
                        new String[]{"col_1"},
                        "col_a",
                        new String[]{"str"},
                        new int[]{1},
                        false,
                        null,
                        true
                ),
                Arguments.of(
                        new String[]{"col_2", "col_3"},
                        "col_b",
                        new String[]{"float", "int"},
                        new int[]{2, 3},
                        true,
                        "(-?\\d*(?:\\.\\d*)?),(\\d*)(?:;|$)",
                        true
                )
        );
    }
    @ParameterizedTest
    @MethodSource("provideCgParams")
    public void testTableDefColumnGroupDefPopulate(
            String[] names,
            String from,
            String[] types,
            int[] fieldIds,
            Boolean isRegex,
            String regexp,
            Boolean single
    ) throws IOException {
        TableBuilder.TableDef.ColumnGroupDef cgDef =
                new TableBuilder.TableDef.ColumnGroupDef(names, from, types, fieldIds, isRegex, regexp, single);

        ;
        try (InputStream is = getClass().getClassLoader().getResourceAsStream("csv/input/simple_input.csv")) {
            if (is == null) {
                fail("Empty file: csv/input/simple_input.csv");
            }
            Reader in = new BufferedReader(new InputStreamReader(is));
            Map<String, List<Object>> recordMap = new HashMap<>();

            CSVParser parser = CSVFormat.DEFAULT.builder()
                    .setHeader()
                    .get()
                    .parse(in);
            int currentSize = 0;
            for (CSVRecord record : parser) {
                String[] colNames = cgDef.getNames();
                cgDef.populate(recordMap, record);
                assertEquals(colNames.length, recordMap.size());
                boolean first = true;
                for (String colName : colNames) {
                    assertTrue(recordMap.containsKey(colName));
                    List<Object> col = recordMap.get(colName);
                    if (first) {
                        first = false;
                        assertTrue(currentSize < col.size());
                        currentSize = col.size();
                    } else {
                        assertEquals(currentSize, col.size());
                    }
                }
            }

        } catch (FileNotFoundException e) {
            fail("File does not exist: " + e.getMessage());
        }
    }

    public static Stream<Arguments> provideRecordsToFileTestCases() throws IOException {
        return Stream.of(
                Arguments.of(
                        "Basic functionality with multiple records",
                        new TableBuilder.TableDef.ColumnGroupDef[]{
                                new TableBuilder.TableDef.ColumnGroupDef(
                                        new String[]{"name"},
                                        "col_a",
                                        new String[]{"str"},
                                        new int[]{1},
                                        false,
                                        null,
                                        true
                                ),
                                new TableBuilder.TableDef.ColumnGroupDef(
                                        new String[]{"value"},
                                        "col_b",
                                        new String[]{"int"},
                                        new int[]{2},
                                        false,
                                        null,
                                        true
                                )
                        },
                        "test_table",
                        "col_a,col_b\ntest1,100\ntest2,200",
                        2,
                        true,
                        true,
                        true
                ),
                Arguments.of(
                        "Complex data types",
                        new TableBuilder.TableDef.ColumnGroupDef[]{
                                new TableBuilder.TableDef.ColumnGroupDef(
                                        new String[]{"float_value"},
                                        "col_a",
                                        new String[]{"float"},
                                        new int[]{1},
                                        false,
                                        null,
                                        true
                                ),
                                new TableBuilder.TableDef.ColumnGroupDef(
                                        new String[]{"string_value"},
                                        "col_b",
                                        new String[]{"str"},
                                        new int[]{2},
                                        false,
                                        null,
                                        true
                                )
                        },
                        "complex_table",
                        "col_a,col_b\n3.14,pi\n-2.5,negative",
                        2,
                        true,
                        true,
                        true 
                )
        );
    }

    @ParameterizedTest
    @MethodSource("provideRecordsToFileTestCases")
    public void testTableDefRecordsToFile(
            String testDescription,
            TableBuilder.TableDef.ColumnGroupDef[] columnGroupDefs,
            String tableName,
            String csvData,
            int expectedRecordsBeforeFile,
            boolean expectFileCreated,
            boolean expectFileNonEmpty,
            boolean expectRecordsCleared
    ) throws IOException {
        TableBuilder.TableDef tableDef = new TableBuilder.TableDef(tableName, columnGroupDefs);

        if (!csvData.isEmpty()) {
            try (StringReader reader = new StringReader(csvData);
                 CSVParser parser = CSVFormat.DEFAULT.builder()
                         .setHeader()
                         .get()
                         .parse(reader)) {
                
                for (CSVRecord csvRecord : parser) {
                    tableDef.unpackRow(csvRecord);
                }
            }
        }

        assertEquals(expectedRecordsBeforeFile, tableDef.getRecords().size(), 
                "Should have " + expectedRecordsBeforeFile + " records before file creation for: " + testDescription);

        File tmpFile = tableDef.recordsToFile();

        if (expectFileCreated) {
            assertNotNull(tmpFile, "Should return a file for: " + testDescription);
            assertTrue(tmpFile.exists(), "File should exist for: " + testDescription);
            assertTrue(tmpFile.getName().startsWith("iceberg-"), 
                    "File should start with 'iceberg-' for: " + testDescription);
            assertTrue(tmpFile.getName().endsWith(".parquet"), 
                    "File should end with '.parquet' for: " + testDescription);
            
            if (expectFileNonEmpty) {
                assertTrue(tmpFile.length() > 0, "File should not be empty for: " + testDescription);
            } else {
                assertEquals(0, tmpFile.length(), "File should be empty for: " + testDescription);
            }
        } else {
            assertNull(tmpFile, "Should return null for: " + testDescription);
        }

        if (expectRecordsCleared) {
            assertEquals(0, tableDef.getRecords().size(), 
                    "Records should be cleared after writing to file for: " + testDescription);
        }

        if (tmpFile != null && tmpFile.exists()) {
            tmpFile.delete();
        }
    }
}