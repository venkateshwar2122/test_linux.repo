package com.example.nifi.controller;

import com.example.nifi.dto.FlowRequest;
import com.example.nifi.service.FlowBuilderService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/flow")
public class FlowController {

    private static final Logger log = LoggerFactory.getLogger(FlowController.class);

    private final FlowBuilderService service;

    public FlowController(FlowBuilderService service) {
        this.service = service;
    }

    /**
     * 🚀 CREATE + START FLOW (DYNAMIC JSON INPUT)
     */
    @PostMapping("/create")
    public ResponseEntity<?> createFlow(@RequestBody FlowRequest request) {

        try {
            log.info("📥 API CALL: Create Flow | datastreamName={}", request.getDatastreamName());

            String result = service.buildFlow(request);

            log.info("✅ Flow created successfully");

            return ResponseEntity.ok(result);

        } catch (Exception e) {
            log.error("❌ Flow creation failed", e);

            return ResponseEntity.internalServerError()
                    .body("Flow creation failed: " + e.getMessage());
        }
    }

    /**
     * ▶️ START FLOW
     */
    @PostMapping("/start/{pgId}")
    public ResponseEntity<?> startFlow(@PathVariable String pgId) {

        try {
            log.info("📥 API CALL: Start Flow | pgId={}", pgId);

            service.startFlow(pgId);

            return ResponseEntity.ok("▶️ Flow STARTED for PG_ID=" + pgId);

        } catch (Exception e) {
            log.error("❌ Start Flow failed", e);

            return ResponseEntity.internalServerError()
                    .body("Start failed: " + e.getMessage());
        }
    }

    /**
     * ⛔ STOP FLOW
     */
    @PostMapping("/stop/{pgId}")
    public ResponseEntity<?> stopFlow(@PathVariable String pgId) {

        try {
            log.info("📥 API CALL: Stop Flow | pgId={}", pgId);

            service.stopFlow(pgId);

            return ResponseEntity.ok("⛔ Flow STOPPED for PG_ID=" + pgId);

        } catch (Exception e) {
            log.error("❌ Stop Flow failed", e);

            return ResponseEntity.internalServerError()
                    .body("Stop failed: " + e.getMessage());
        }
    }

    /**
     * 🔁 RESTART FLOW
     */
    @PostMapping("/restart/{pgId}")
    public ResponseEntity<?> restartFlow(@PathVariable String pgId) {

        try {
            log.info("📥 API CALL: Restart Flow | pgId={}", pgId);

            service.stopFlow(pgId);
            service.startFlow(pgId);

            return ResponseEntity.ok("🔁 Flow RESTARTED for PG_ID=" + pgId);

        } catch (Exception e) {
            log.error("❌ Restart Flow failed", e);

            return ResponseEntity.internalServerError()
                    .body("Restart failed: " + e.getMessage());
        }
    }

    /**
     * ❤️ HEALTH CHECK
     */
    @GetMapping("/health")
    public ResponseEntity<String> health() {
        return ResponseEntity.ok("NiFi Dynamic Flow App is running 🚀");
    }
}
