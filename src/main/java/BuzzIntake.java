import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.csv.CSVRecord;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.rest.RESTCatalog;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class BuzzIntake {
    private static final Logger logger = Logger.getLogger(TableBuilder.class.getName());
    public static void main(String[] args) {
        Controller controller;
        switch (args.length) {
            case 1 -> controller = new Controller(args[0]);
            case 3 -> controller = new Controller(args[0], args[1], args[2]);
            case 2 -> controller = new Controller(args[0], args[1]);
            default -> {
                logger.log(Level.SEVERE, "Invalid number of arguments: {0}", args.length);
                return;
            }
        }
        Gson gson = new GsonBuilder()
                .registerTypeAdapter(TableBuilder.class, new TableBuilderAdapter())
                .create();
        TableBuilder tableBuilder;
        try (FileReader reader = new FileReader("resources/gdelt_og_schema.json")) {
            tableBuilder = gson.fromJson(reader, TableBuilder.class);
        } catch(IOException e) {
            logger.log(Level.SEVERE, "Failed to read gdelt_og_schema.json: {0}", e);
            return;
        }
        Catalog catalog = new RESTCatalog();
        Map<String, String> properties = new HashMap<>();
        properties.put("uri", System.getenv("ICEBERG_REST_URI"));
        String warehouse = System.getenv("ICEBERG_WAREHOUSE");
        if (warehouse != null) {
            properties.put("warehouse", warehouse);
        }
        catalog.initialize("numbuzzCatalog", properties);
        logger.info("Initialized catalog");

        GDELTCollector gdc = new GDELTCollector();
        for (String dtSuffix : controller.getDtStrings()) {
            Iterable<CSVRecord> csvRecords = gdc.processCSV(tableBuilder.inputFields(), dtSuffix);
            logger.log(Level.INFO, "Created records for {0}", dtSuffix);
            tableBuilder.populateTables(csvRecords, catalog);
            logger.log(Level.INFO, "Populated tables for {0}", dtSuffix);
        }
    }
}
