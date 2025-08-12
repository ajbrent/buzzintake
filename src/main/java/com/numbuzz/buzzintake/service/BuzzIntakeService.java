package com.numbuzz.buzzintake.service;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.csv.CSVRecord;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.rest.RESTCatalog;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.numbuzz.buzzintake.model.Controller;
import com.numbuzz.buzzintake.model.GDELTCollector;
import com.numbuzz.buzzintake.model.TableBuilder;
import com.numbuzz.buzzintake.model.TableBuilderAdapter;

@Service
public class BuzzIntakeService {
    private static final Logger logger = Logger.getLogger(BuzzIntakeService.class.getName());
    
    @Value("${iceberg.rest.uri:http://iceberg-rest:8181}")
    private String icebergRestUri;
    
    @Value("${iceberg.warehouse:}")
    private String icebergWarehouse;
    
    private final Gson gson;
    
    public BuzzIntakeService() {
        this.gson = new GsonBuilder()
                .registerTypeAdapter(TableBuilder.class, new TableBuilderAdapter())
                .create();
    }
    
    public void processDateSuffix(String dateSuffix) {
        try {
            Controller controller = new Controller(dateSuffix);
            processWithController(controller);
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error processing single date suffix: " + dateSuffix, e);
            throw new RuntimeException("Failed to process date suffix: " + dateSuffix, e);
        }
    }
    
    public void processDateRange(String template, String start, String end) {
        try {
            Controller controller = new Controller(template, start, end);
            processWithController(controller);
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error processing date range: " + start + " to " + end, e);
            throw new RuntimeException("Failed to process date range: " + start + " to " + end, e);
        }
    }
    
    public void processWithStartEnd(String start, String end) {
        try {
            Controller controller = new Controller(start, end);
            processWithController(controller);
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error processing start/end: " + start + " to " + end, e);
            throw new RuntimeException("Failed to process start/end: " + start + " to " + end, e);
        }
    }
    
    private void processWithController(Controller controller) throws IOException {
        TableBuilder tableBuilder;
        try (InputStream inputStream = getClass().getClassLoader().getResourceAsStream("gdelt_og_schema.json");
             InputStreamReader reader = new InputStreamReader(inputStream)) {
            tableBuilder = gson.fromJson(reader, TableBuilder.class);
        }
        
        Catalog catalog = new RESTCatalog();
        Map<String, String> properties = new HashMap<>();
        properties.put("uri", icebergRestUri);
        if (icebergWarehouse != null && !icebergWarehouse.isEmpty()) {
            properties.put("warehouse", icebergWarehouse);
        }
        catalog.initialize("numbuzzCatalog", properties);
        logger.log(Level.INFO, "Initialized catalog with URI: {0}", icebergRestUri);

        GDELTCollector gdc = new GDELTCollector();
        for (String dtSuffix : controller.getDtStrings()) {
            logger.log(Level.INFO, "Processing date suffix: {0}", dtSuffix);
            Iterable<CSVRecord> csvRecords = gdc.processCSV(tableBuilder.inputFields(), dtSuffix);
            logger.log(Level.INFO, "Created records for {0}", dtSuffix);
            
            tableBuilder.populateTables(csvRecords, catalog);
            logger.log(Level.INFO, "Populated tables for {0}", dtSuffix);
        }
    }
}
