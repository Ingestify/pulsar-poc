package io.ingestify.pulsar_poc;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.pulsar.core.PulsarTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import io.ingestify.pulsar_poc.topics.poc_topic.POCTopic;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.annotation.PostConstruct;


@Tag(name = "Producer", description = "Manage Queue Listeners")
@Controller
public class Producer {
    
    @Autowired
    POCTopic pocTopic;

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
        // containersTopic.publish(ContainersAction.STOP);
        System.out.println("Publishing message to topic: " + topic);
        
        
        // pulsarTemplate.newMessage(message).withMessageCustomizer(c -> c.key("asd"));


        // pulsarTemplate.send("persistent://tenant/namespace/"+topic, message);

        pocTopic.publish(message);
        
        return ResponseEntity.ok(null);
    }
}
