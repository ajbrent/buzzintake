package com.numbuzz.buzzintake.controller;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.numbuzz.buzzintake.service.BuzzIntakeService;

@RestController
@RequestMapping("/api/buzzintake")
public class BuzzIntakeController {
    private static final Logger logger = Logger.getLogger(BuzzIntakeController.class.getName());
    
    @Autowired
    private BuzzIntakeService buzzIntakeService;
    
    @PostMapping("/process")
    public ResponseEntity<Map<String, Object>> processDateSuffix(@RequestBody Map<String, String> request) {
        try {
            String dateSuffix = request.get("dateSuffix");
            if (dateSuffix == null || dateSuffix.isEmpty()) {
                return ResponseEntity.badRequest().body(createErrorResponse("dateSuffix is required"));
            }
            
            logger.log(Level.INFO, "Processing date suffix: {0}", dateSuffix);
            buzzIntakeService.processDateSuffix(dateSuffix);
            
            return ResponseEntity.ok(createSuccessResponse("Successfully processed date suffix: " + dateSuffix));
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error processing request: {0}", e.getMessage());
            return ResponseEntity.internalServerError().body(createErrorResponse(e.getMessage()));
        }
    }
    
    @PostMapping("/process-range")
    public ResponseEntity<Map<String, Object>> processDateRange(@RequestBody Map<String, String> request) {
        try {
            String template = request.get("template");
            String start = request.get("start");
            String end = request.get("end");
            
            if (template == null || start == null || end == null) {
                return ResponseEntity.badRequest().body(createErrorResponse("template, start, and end are required"));
            }
            
            logger.log(Level.INFO, "Processing date range: {0} to {1} with template: {2}", new Object[]{start, end, template});
            buzzIntakeService.processDateRange(template, start, end);
            
            return ResponseEntity.ok(createSuccessResponse("Successfully processed date range: " + start + " to " + end));
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error processing range request: {0}", e.getMessage());
            return ResponseEntity.internalServerError().body(createErrorResponse(e.getMessage()));
        }
    }
    
    @PostMapping("/process-start-end")
    public ResponseEntity<Map<String, Object>> processStartEnd(@RequestBody Map<String, String> request) {
        try {
            String start = request.get("start");
            String end = request.get("end");
            
            if (start == null || end == null) {
                return ResponseEntity.badRequest().body(createErrorResponse("start and end are required"));
            }
            
            logger.log(Level.INFO, "Processing start/end: {0} to {1}", new Object[]{start, end});
            buzzIntakeService.processWithStartEnd(start, end);
            
            return ResponseEntity.ok(createSuccessResponse("Successfully processed start/end: " + start + " to " + end));
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error processing start/end request: {0}", e.getMessage());
            return ResponseEntity.internalServerError().body(createErrorResponse(e.getMessage()));
        }
    }
    
    @PostMapping("/process-current")
    public ResponseEntity<Map<String, Object>> processCurrent() {
        try {
            logger.log(Level.INFO, "Processing current 15-minute slot");
            buzzIntakeService.processCurrent();
            return ResponseEntity.ok(createSuccessResponse("Successfully processed current 15-minute slot"));
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error processing current request: {0}", e.getMessage());
            return ResponseEntity.internalServerError().body(createErrorResponse(e.getMessage()));
        }
    }
    
    @GetMapping("/health")
    public ResponseEntity<Map<String, Object>> health() {
        Map<String, Object> response = new HashMap<>();
        response.put("status", "UP");
        response.put("service", "BuzzIntake");
        return ResponseEntity.ok(response);
    }
    
    private Map<String, Object> createSuccessResponse(String message) {
        Map<String, Object> response = new HashMap<>();
        response.put("success", true);
        response.put("message", message);
        response.put("timestamp", System.currentTimeMillis());
        return response;
    }
    
    private Map<String, Object> createErrorResponse(String error) {
        Map<String, Object> response = new HashMap<>();
        response.put("success", false);
        response.put("error", error);
        response.put("timestamp", System.currentTimeMillis());
        return response;
    }
}
