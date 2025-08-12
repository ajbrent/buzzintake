package com.numbuzz.buzzintake.model;

import java.io.IOException;
import java.lang.reflect.Type;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.TypeAdapter;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;

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
            String regexp = null;

            JsonObject obj = jsonArray.get(i).getAsJsonObject();
            String[] names = gson.fromJson(obj.get("names"), listType);
            String[] types = gson.fromJson(obj.get("types"), listType);
            int[] fieldIds = gson.fromJson(obj.get("fieldIds"), new TypeToken<int[]>(){}.getType());
            String from = obj.get("from").getAsString();
            boolean isRegex = obj.get("isRegex").getAsBoolean();
            boolean single = obj.get("single").getAsBoolean();
            JsonElement regexpHolder = obj.get("regexp");
            if (regexpHolder != null) {
                regexp = regexpHolder.getAsString();
            }
            colArray[i] = new TableBuilder.TableDef.ColumnGroupDef(names, from, types, fieldIds, isRegex, regexp, single);
        }
        return colArray;
    }
}
