package com.mapfre.cdc.admin;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.mapfre.cdc.admin.commands.CommandResult;

/**
 * Serializa CommandResult a JSON y lo escribe en stdout.
 *
 * IMPORTANTE: stdout es el canal exclusivo para el output JSON
 * que consume Ansible. Los logs de aplicación van siempre a stderr
 * (configurado así en logback.xml).
 */
public class OutputFormatter {

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .enable(SerializationFeature.INDENT_OUTPUT);

    public static void print(CommandResult result) {
        try {
            System.out.println(MAPPER.writeValueAsString(result));
        } catch (Exception e) {
            // Fallback mínimo si Jackson falla
            System.out.println("{\"status\":\"ERROR\",\"message\":\"Error serializando resultado: "
                    + e.getMessage() + "\"}");
        }
    }
}
