import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.*;

class TableBuilderAdapterTest {
    @Test
    void testRead() {
        String[] expectedInputFields = {"col_a", "col_b"};
        String[] expectedTableNames = {"table_one"};
        String[][] expectedColFrom = {{"col_a", "col_b"}};
        String[][][] expectedColNames = {{{"col_1"}, {"col_2", "col_3"}}};
        int[][][] expectedFieldIds = {{{1}, {2, 3}}};
        Type[][][] expectedColTypes = {{{Types.StringType.get()}, {Types.FloatType.get(), Types.IntegerType.get()}}};
        Boolean[][] expectedRegBooleans = {{false, true}};
        String[][] expectedRegStrings = {{null, "^(-?\\\\d*(?:\\\\.\\\\d*)?),(\\d*)"}};
        Boolean[][] expectedSingle = {{true, false}};

        Gson gson = new GsonBuilder()
                .registerTypeAdapter(TableBuilder.class, new TableBuilderAdapter())
                .create();
        TableBuilder tableBuilder;
        File resourceFile = new File(Objects.requireNonNull(getClass().getClassLoader().getResource("json/test_schema.json")).getFile());
        try (FileReader reader = new FileReader(resourceFile)) {
            tableBuilder = gson.fromJson(reader, TableBuilder.class);
        } catch(IOException e) {
            fail("Exception was thrown: " + e.getMessage());
            return;
        }
        assertArrayEquals(expectedInputFields, tableBuilder.inputFields());
        TableBuilder.TableDef[] actualTableDefs = tableBuilder.tableDefs();
        assertEquals(expectedTableNames.length, actualTableDefs.length);

        for (int i=0; i<actualTableDefs.length; i++) {
            assertEquals(expectedTableNames[i], actualTableDefs[i].getName());
            TableBuilder.TableDef.ColumnGroupDef[] actualColumnGroupDefs = actualTableDefs[i].getColumnGroupDefs();
            for (int j=0; j<actualColumnGroupDefs.length; j++) {
                TableBuilder.TableDef.ColumnGroupDef cgDef = actualColumnGroupDefs[j];
                assertEquals(expectedColFrom[i][j], cgDef.getFrom());
                assertArrayEquals(expectedColNames[i][j], cgDef.getNames());
                assertArrayEquals(expectedColTypes[i][j], cgDef.getTypes());
                assertArrayEquals(expectedFieldIds[i][j], cgDef.getFieldIds());
                assertEquals(expectedRegBooleans[i][j], cgDef.isRegex());
                assertEquals(expectedRegStrings[i][j], cgDef.getRegexp());
                assertEquals(expectedSingle[i][j], cgDef.isSingle());
            }
        }
    }
}
