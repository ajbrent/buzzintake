import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.csv.CSVRecord;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

public record TableBuilder(String[] inputFields, TableBuilder.TableDef[] tableDefs) {
    private static final Logger logger = Logger.getLogger(TableBuilder.class.getName());
    private static final String dbName = "gdelt";

    public TableBuilder(String[] inputFields, TableDef[] tableDefs) {
        this.inputFields = inputFields;
        this.tableDefs = tableDefs;
    }

    public static class TableDef {

        private final String name;
        private final ColumnGroupDef[] columnGroupDefs;
        private final Schema schema;
        private final List<GenericRecord> records = new ArrayList<>();
        private final FileFormat format = FileFormat.PARQUET;

        public TableDef(String name, ColumnGroupDef[] columnGroupDefs) {
            this.name = name;
            this.columnGroupDefs = columnGroupDefs;
            List<Types.NestedField> fields = new ArrayList<>();
            for (ColumnGroupDef cgDef : columnGroupDefs) {
                String[] cgNames = cgDef.getNames();
                Type[] cgTypes = cgDef.getTypes();
                int[] cgFieldIds = cgDef.getFieldIds();
                if (cgTypes.length != cgNames.length) {
                    throw new IllegalArgumentException("Names and types must be of equal length");
                }
                if (cgTypes.length != cgFieldIds.length) {
                    throw new IllegalArgumentException("types and fieldIds must be of equal length");
                }
                for (int i=0; i<cgNames.length; i++) {
                    fields.add(Types.NestedField.optional(cgFieldIds[i], cgNames[i], cgTypes[i]));
                }
            }
            this.schema = new Schema(fields);
        }

        public static class ColumnGroupDef {
            private final String[] names;
            private final Type[] types;
            private final String from;
            private final Boolean isRegex;
            private final Pattern pattern;
            private final Boolean single;
            private final int[] fieldIds;

            public String[] getNames() {
                return names;
            }

            public String getFrom() {
                return from;
            }

            public Type[] getTypes() {
                return types;
            }

            public Boolean isRegex() {
                return isRegex;
            }

            public String getRegexp() {
                if(pattern != null) {
                    return pattern.pattern();
                }
                return null;
            }

            public Boolean isSingle() {
                return single;
            }

            public int[] getFieldIds() { return fieldIds; }

            public ColumnGroupDef(String[] names, String from, String[] types, int[] fieldIds, Boolean isRegex, String regexp, Boolean single) throws IOException {
                this.names = names;
                this.types = new Type[types.length];
                this.from = from;
                this.isRegex = isRegex;
                this.fieldIds = fieldIds;
                if (regexp == null) {
                    this.pattern = null;
                } else {
                    this.pattern = Pattern.compile(regexp);
                }
                this.single = single;
                for (int i = 0; i < types.length; i++) {
                    this.types[i] = switch (types[i]) {
                        case "str" -> Types.StringType.get();
                        case "float" -> Types.FloatType.get();
                        case "int" -> Types.IntegerType.get();
                        default -> throw new IOException("Invalid type in json def");
                    };
                }
            }

            private static Object cast(String v, Type t) {
                if (v == null || "".equals(v)) return null;

                return switch (t.typeId()) {
                    case STRING -> v;
                    case INTEGER -> Integer.valueOf(v);
                    case LONG -> Long.valueOf(v);
                    case FLOAT -> Float.valueOf(v);
                    case DOUBLE -> Double.valueOf(v);
                    case BOOLEAN -> Boolean.valueOf(v);
                    case DATE -> (int) java.time.LocalDate.parse(v).toEpochDay();
                    case TIMESTAMP -> java.time.Instant.parse(v);
                    default -> throw new UnsupportedOperationException("Unsupported type: " + t);
                };
            }

            private static String getCsvField(CSVRecord csvRecord, String field) {
                try {
                    String value = csvRecord.get(field);
                    // Need to test this logic
                    if (value == null) return null;
                    value = value.replace("\"", "");
                    return value.replaceAll(",+$", "");
                } catch (IllegalArgumentException e) {
                    throw new IllegalArgumentException("Field '" + field + "' not found in CSV record\n" + csvRecord.toString(), e);
                }
            }

            public int populate(Map<String, List<Object>> recordMap, CSVRecord csvRecord) {
                int count = 0;
                if (isRegex) {
                    String cleanField = getCsvField(csvRecord, from);
                    Matcher matcher = pattern.matcher(cleanField);
                    while (matcher.find()) {
                        for (int i = 0; i < names.length; i++) {
                            if (recordMap.containsKey(names[i])) {
                                recordMap.get(names[i]).add(cast(matcher.group(i+1), types[i]));
                            } else {
                                List<Object> newList = new ArrayList<>();
                                try {
                                    newList.add(cast(matcher.group(i+1), types[i]));
                                } catch (NumberFormatException e) {
                                    throw new IllegalArgumentException(
                                        "Invalid value from '" + 
                                        this.from + 
                                        "' field '" + 
                                        names[i] + "': " + 
                                        matcher.group(i+1) +
                                        "\n\tOriginal: " + csvRecord.get(from) +
                                        "\n\tClean: " + cleanField,
                                        e
                                    );
                                }
                                recordMap.put(names[i], newList);
                            }
                        }
                        count++;
                    }
                } else {
                    if (recordMap.containsKey(names[0])) {
                        recordMap.get(names[0]).add(cast(getCsvField(csvRecord, from), types[0]));
                    } else {
                        List<Object> newList = new ArrayList<>();
                        newList.add(cast(getCsvField(csvRecord, from), types[0]));
                        recordMap.put(names[0], newList);
                    }
                }
                if (single) {
                    return -1;
                } else {
                    return count;
                }
            }
        }

        public void unpackRow(CSVRecord csvRecord) {
            Map<String, List<Object>> recordMap = new HashMap<>();
            int size = -1;
            for (ColumnGroupDef columnGroupDef : columnGroupDefs) {
                int groupSize = columnGroupDef.populate(recordMap, csvRecord);
                if (size > -1) {
                    assert groupSize == -1 || groupSize == size;
                } else {
                    size = groupSize;
                }
            }
            if (size == -1) {
                size = 1;
            }
            for (int i = 0; i < size; i++) {
                GenericRecord r = GenericRecord.create(schema);
                for (String key : recordMap.keySet()) {
                    int idx = 0;
                    if (recordMap.get(key).size() > 1) {
                        idx = i;
                    }
                    r.setField(key, recordMap.get(key).get(idx));
                }
                records.add(r);
            }
        }

        public File recordsToFile() throws IOException {
            String tempDir = System.getProperty("java.io.tmpdir");
            String fileName = "iceberg-" + System.currentTimeMillis() + "-" + 
                            java.util.UUID.randomUUID().toString().substring(0, 8) + ".parquet";
            File tmpFile = new File(tempDir, fileName);
            tmpFile.deleteOnExit();
            
            OutputFile outputFile = Files.localOutput(tmpFile);

            try (FileAppender<GenericRecord> appender = Parquet.write(outputFile)
                    .schema(schema)
                    .createWriterFunc(GenericParquetWriter::buildWriter)
                    .build()) {
                for (GenericRecord record : records) {
                    appender.add(record);
                }
            } catch(IOException e) {
                // failed to create file for given table.
                return null;
            }
            records.clear();
            return tmpFile;
        }

        public void commitFile(File tmpFile, Table table) {
            logger.log(Level.INFO, "Committing file {0} to table {1}", new Object[]{tmpFile.getPath(), table.name()});
            DataFile dataFile = DataFiles.builder(table.spec())
                    .withPath(tmpFile.getPath())
                    .withFormat(format)
                    .withFileSizeInBytes(tmpFile.length())
                    .withRecordCount(records.size())
                    .build();
            table.newAppend()
                    .appendFile(dataFile)
                    .commit();
        }

        public String getName() {
            return name;
        }

        public ColumnGroupDef[] getColumnGroupDefs() {
            return columnGroupDefs;
        }
        public Table getTable(Catalog catalog) {
            return catalog.loadTable(TableIdentifier.of(dbName, name));
        }

        public List<GenericRecord> getRecords() {
            return records;
        }
        
        public Schema getSchema() {
            return schema;
        }
    }

    public void populateTables(Iterable<CSVRecord> csvRecords, Catalog catalog) {
        logger.info("Populating tables");
        for (CSVRecord csvRecord : csvRecords) {
            for (TableDef tableDef : tableDefs) {
                tableDef.unpackRow(csvRecord);
            }
        }
        for (TableDef tableDef : tableDefs) {
            try {
                File tmpFile = tableDef.recordsToFile();
                Table table = tableDef.getTable(catalog);
                tableDef.commitFile(tmpFile, table);
                logger.log(Level.INFO, "Committed file {0} to table {1}", new Object[]{tmpFile.getPath(), table.name()});
            } catch (IOException e) {
                logger.log(Level.SEVERE, "Failed appending records to table {0} {1}", new Object[]{tableDef.getName(), e});
            }

        }
    }
}
