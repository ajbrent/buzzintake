import com.google.gson.*;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;

import java.io.IOException;
import java.lang.reflect.Type;

public class TableBuilderAdapter extends TypeAdapter<TableBuilder>{
    private final Gson gson;
    private final Type listType;
    public TableBuilderAdapter() {
        this.gson = new Gson();
        this.listType = new TypeToken<String[]>(){}.getType();
    }
    @Override
    public void write(JsonWriter out, TableBuilder value) {
        throw new UnsupportedOperationException("Serialization not supported");
    }
    @Override
    public TableBuilder read(JsonReader in) throws IOException {
        JsonObject obj = JsonParser.parseReader(in).getAsJsonObject();

        JsonArray inputJsonFields = obj.getAsJsonArray("inputFields");
        String[] inputFields = new String[inputJsonFields.size()];
        for (int i=0; i<inputJsonFields.size(); i++) {
            inputFields[i] = inputJsonFields.get(i).getAsString();
        }
        JsonArray tableDefsJson = obj.getAsJsonArray("tableDefs");
        TableBuilder.TableDef[] tableDefs = readTableDefs(tableDefsJson);
        return new TableBuilder(inputFields, tableDefs);
    }

    private TableBuilder.TableDef[] readTableDefs(JsonArray jsonArray) throws IOException {
        TableBuilder.TableDef[] tableDefs = new TableBuilder.TableDef[jsonArray.size()];
        for(int i=0; i<jsonArray.size(); i++) {
            JsonObject obj = jsonArray.get(i).getAsJsonObject();
            String name = obj.get("name").getAsString();
            JsonArray colArray = obj.get("columnGroups").getAsJsonArray();
            TableBuilder.TableDef.ColumnGroupDef[] columnGroupDefs = readColGroupDef(colArray);
            TableBuilder.TableDef td = new TableBuilder.TableDef(name, columnGroupDefs);
            tableDefs[i] = td;
        }
        return tableDefs;
    }

    private TableBuilder.TableDef.ColumnGroupDef[] readColGroupDef(JsonArray jsonArray) throws IOException {
        TableBuilder.TableDef.ColumnGroupDef[] colArray = new TableBuilder.TableDef.ColumnGroupDef[jsonArray.size()];
        for(int i = 0; i<jsonArray.size(); i++) {

            String[] names = null;
            String[] types = null;
            int[] fieldIds = null;
            String from = null;
            boolean isRegex = false;
            boolean single = true;
            String regexp = null;

            JsonObject obj = jsonArray.get(i).getAsJsonObject();
            names = gson.fromJson(obj.get("names"), listType);
            types = gson.fromJson(obj.get("types"), listType);
            fieldIds = gson.fromJson(obj.get("fieldIds"), new TypeToken<int[]>(){}.getType());
            from = obj.get("from").getAsString();
            isRegex = obj.get("isRegex").getAsBoolean();
            single = obj.get("single").getAsBoolean();
            JsonElement regexpHolder = obj.get("regexp");
            if (regexpHolder != null) {
                regexp = regexpHolder.getAsString();
            }
            colArray[i] = new TableBuilder.TableDef.ColumnGroupDef(names, from, types, fieldIds, isRegex, regexp, single);
        }
        return colArray;
    }
}
