package io.ingestify.pulsar_poc;

// import org.openapitools.jackson.nullable.JsonNullableModule;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import io.swagger.v3.oas.models.Components;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Contact;
import io.swagger.v3.oas.models.info.Info;

@Configuration
public class SpringConfig implements WebMvcConfigurer {


    
    // @Bean
    // public JsonNullableModule jsonNullableModule() {
    //     return new JsonNullableModule();
    // }

    // **** Swagger Config **** //

    @Bean
    OpenAPI customOpenAPI() {

        // final String securitySchemeName = "bearerAuth";
        // final String header = "headerAuth";

        OpenAPI oApi = new OpenAPI()
                .components(//new Components()
                        // .addSecuritySchemes(securitySchemeName,
                        // new SecurityScheme()
                        //         .type(SecurityScheme.Type.HTTP)
                        //         .scheme("bearer")
                        //         .bearerFormat("JWT")))
                        new Components()
                            // .addSecuritySchemes(header,
                            //     new SecurityScheme()
                            //             .type(SecurityScheme.Type.APIKEY)
                            //             .name(props.authHeaderName())
                            //             .in(SecurityScheme.In.HEADER))
                )
                // .security(List.of(new SecurityRequirement().addList(header)))
                .info(new Info()
                        .title("Pulsar POC API")
                        .version("1")
                        .contact(new Contact()
                                .name("ingestify.io")));
                                // .email("support@ingestify.io")));

        return oApi;
    }

    // @Bean
    // public IncomingRequestInterceptor tokenAuthInterceptor() {
    //     return new IncomingRequestInterceptor();
    // }

    // @Override
    // public void addInterceptors(InterceptorRegistry registry){
    //     registry.addInterceptor(tokenAuthInterceptor());
    // }
}