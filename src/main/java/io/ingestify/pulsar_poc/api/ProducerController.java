package io.ingestify.pulsar_poc.api;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.pulsar.reactive.core.ReactivePulsarTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.annotation.PostConstruct;


@Tag(name = "Producer", description = "Manage Queue Listeners")
@Controller
public class ProducerController {

    @Autowired
    private ReactivePulsarTemplate<String> pulsarTemplate;

    @PostConstruct
    public void init() {
        System.out.println("Producer initialized");
    }
    
    @Operation(
        operationId = "produceMessage",
        summary = "Produce Message.",
        description = "Produce Message.",
        tags = { "Producer" },
        responses = {
            @ApiResponse(responseCode = "200", description = "Success")
        }
    )
    @RequestMapping(
        method = RequestMethod.GET,
        value = "/produceMessage"
    )
    public ResponseEntity<Void> produceMessage(
        @RequestParam(value = "topic", required = true) String topic,
        @RequestParam(value = "message", required = true) String message
    ) {
        System.out.println("Publishing message to topic: " + topic);
        
        pulsarTemplate.send("persistent://tenant/namespace/"+topic, message).subscribe();
        
        return ResponseEntity.ok(null);
    }
}
