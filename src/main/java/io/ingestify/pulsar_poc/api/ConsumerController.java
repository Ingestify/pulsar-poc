package io.ingestify.pulsar_poc.api;

import java.util.Set;

import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.PulsarClientException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import io.ingestify.pulsar_poc.topics.poc_topic.POCTopic;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.parameters.RequestBody;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;

// @Configuration
@Tag(name = "Consumer", description = "Manage Queue Listeners")
@Controller
@RequestMapping("/consumer")
public class ConsumerController {

    @Autowired
    POCTopic pocTopic;

    // @PulsarListener(subscriptionName = "${props.name1}", topics = "persistent://tenant/namespace/test")//, subscriptionType = SubscriptionType.Key_Shared)
    // void listen(String message) {
    //     System.out.println("sub 1 Message Received: " + message);
    // }

    // @PulsarListener(subscriptionName = "${props.name2}", topics = "persistent://tenant/namespace/test")
    // void listen2(String message) {
    //     System.out.println("sub 2 Message Received: " + message);
    // }


    @Operation(
        operationId = "createSubscription",
        tags = { "Consumer" },
        responses = {
            @ApiResponse(responseCode = "200", description = "Success")
        }
    )
    @RequestMapping(
        method = RequestMethod.GET,
        value = "/createSubscription"
    )
    public ResponseEntity<Void> createSubscription(
        @RequestBody ListenerProcessorInitilizerDTO body
    ) throws PulsarAdminException, PulsarClientException {

        pocTopic.addMessageListener(body.buildInitilizer());

        return ResponseEntity.ok(null);
    }
    
    @Operation(
        operationId = "cancelSubscription",
        tags = { "Consumer" },
        responses = {
            @ApiResponse(responseCode = "200", description = "Success")
        }
    )
    @RequestMapping(
        method = RequestMethod.GET,
        value = "/cancelSubscription"
    )
    public ResponseEntity<Void> cancelSubscription(
        @RequestParam(value = "subscriptionName", required = false) String listenerId
    ) throws PulsarAdminException, PulsarClientException {
        
        pocTopic.removeMessageListener(listenerId);

        // pulsarAdmin.topics().deleteSubscription("persistent://tenant/namespace/test", subscriptionName);
        return ResponseEntity.ok(null);
    }


    @Operation(
        operationId = "getAllListeners",
        tags = { "Consumer" },
        responses = {
            @ApiResponse(responseCode = "200", description = "Success")
        }
    )
    @RequestMapping(
        method = RequestMethod.GET,
        value = "/getAllListeners"
    )
    public ResponseEntity<Set<String>> getAllListeners() {
        
        return ResponseEntity.ok(pocTopic.getAllListeners());
    }
}
