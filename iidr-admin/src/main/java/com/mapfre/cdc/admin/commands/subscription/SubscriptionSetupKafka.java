package com.mapfre.cdc.admin.commands.subscription;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.replication.cdc.scripting.ResultStringTable;
import com.mapfre.cdc.admin.IIDRConnection;
import com.mapfre.cdc.admin.commands.Command;
import com.mapfre.cdc.admin.commands.CommandResult;

/**
 * Configura (de forma compuesta e idempotente) una suscripción con destino
 * Kafka para que pueda arrancar: mapeos de tablas + propiedades Kafka
 * (commit topic) + user exit (KCOP). NO arranca la suscripción (usar
 * 'subscription start' aparte) ni genera el fichero .properties (eso lo hace
 * el playbook, que sí tiene acceso al filesystem del motor Kafka).
 *
 * Es un comando COMPUESTO porque el orden es obligatorio en IIDR:
 *   1. La suscripción debe existir.
 *   2. Al menos una tabla mapeada (PRERREQUISITO): sin mapeos, IIDR rechaza
 *      configurar tanto las kafka properties como el user exit
 *      (rc -2001 / -2317).
 *   3. modify subscription kafka properties (commit topic).
 *   4. modify subscription user exit (clase KCOP + -file:<properties>).
 *
 * Uso:
 *   iidr.sh subscription setup-kafka
 *     --source <ds> --target <ds> --name <sub>
 *     --tables SCHEMA.T1,SCHEMA.T2
 *     --commit-topic <topic>
 *     --properties-path <ruta absoluta del .properties en el motor Kafka>
 *     [--kcop-classname <clase>]   (default KcopSingleRowAvroAuditIntegrated)
 *
 * Idempotente: los mapeos ya presentes se omiten; las propiedades y el user
 * exit se (re)establecen (operación idempotente en IIDR). Devuelve OK con el
 * detalle de lo hecho, o SKIPPED si ya estaba todo configurado.
 */
public class SubscriptionSetupKafka implements Command {

    private static final Logger log = LoggerFactory.getLogger(SubscriptionSetupKafka.class);

    private static final String DEFAULT_KCOP =
        "com.datamirror.ts.target.publication.userexit.sample.kafka.KcopSingleRowAvroAuditIntegrated";

    @Override
    public CommandResult execute(IIDRConnection connection, String[] args) throws Exception {

        Options opts = new Options();
        opts.addRequiredOption("s", "source",        true, "Datastore origen");
        opts.addRequiredOption("t", "target",        true, "Datastore destino (Kafka)");
        opts.addRequiredOption("n", "name",          true, "Nombre de la suscripción");
        opts.addRequiredOption("T", "tables",        true, "Tablas 'SCHEMA.TABLA' separadas por coma");
        opts.addRequiredOption("c", "commit-topic",  true, "Nombre del commit stream topic");
        opts.addRequiredOption("f", "properties-path", true, "Ruta absoluta del <SUB>.properties en el motor Kafka");
        opts.addOption("k", "kcop-classname", true, "Clase Java del KCOP (default KcopSingleRowAvroAuditIntegrated)");
        CommandLine cli = new DefaultParser().parse(opts, args);

        String source       = cli.getOptionValue("source");
        String target       = cli.getOptionValue("target");
        String name         = cli.getOptionValue("name");
        String commitTopic  = cli.getOptionValue("commit-topic");
        String propsPath    = cli.getOptionValue("properties-path");
        String kcopClass    = cli.getOptionValue("kcop-classname", DEFAULT_KCOP);

        List<String> tables = new ArrayList<>();
        for (String t : cli.getOptionValue("tables").split(",")) {
            String tt = t.trim();
            if (!tt.isEmpty()) tables.add(tt);
        }
        if (tables.isEmpty()) {
            return CommandResult.error("subscription.setup-kafka",
                    "No se especificó ninguna tabla (--tables SCHEMA.TABLA,...).");
        }

        log.info("Configurando Kafka en suscripción '{}': {} tabla(s), commit topic '{}'",
                name, tables.size(), commitTopic);

        SubscriptionHelper.connectDatastore(connection, source);
        SubscriptionHelper.connectTargetDatastore(connection, target);
        try {
            // 0. La suscripción debe existir.
            boolean exists = SubscriptionHelper.listSubscriptions(connection).stream()
                    .anyMatch(sub -> name.equalsIgnoreCase(sub.get("name")));
            if (!exists) {
                return CommandResult.error("subscription.setup-kafka",
                        "La suscripción no existe: " + name + " (créala antes con subscription create).");
            }

            // No configurar si está corriendo (hay que pararla antes).
            String state = SubscriptionHelper.getState(connection, name);
            if (SubscriptionHelper.isRunning(state)) {
                return CommandResult.error("subscription.setup-kafka",
                        "La suscripción '" + name + "' está en estado '" + state +
                        "'. Párala antes de reconfigurar el KCOP.");
            }

            connection.executeForMessage("select subscription name \"" + name + "\"");
            connection.executeForMessage("lock subscription name \"" + name + "\"");

            List<String> mappedNow = new ArrayList<>();
            List<String> mappedSkipped = new ArrayList<>();
            try {
                // 1. Mapeos de tablas (prerrequisito). Idempotente por tabla.
                for (String full : tables) {
                    String[] parts = full.split("\\.", 2);
                    if (parts.length != 2) {
                        throw new IllegalArgumentException(
                            "Tabla inválida '" + full + "': se espera formato SCHEMA.TABLA");
                    }
                    String schema = parts[0];
                    String tableN = parts[1];
                    if (isMapped(connection, name, full)) {
                        mappedSkipped.add(full);
                    } else {
                        // SYNTAX 5 (Kafka): solo sourceSchema + sourceTable.
                        connection.executeForMessage(
                            "add table mapping sourceSchema " + schema + " sourceTable " + tableN);
                        mappedNow.add(full);
                    }
                }

                // 2. Propiedades Kafka. communicationMethod/encrypted son
                //    obligatorios por sintaxis aunque con Confluent Cloud no se
                //    usen (quedan vacíos/No); lo relevante es el commit topic.
                connection.executeForMessage(
                    "modify subscription kafka properties" +
                    " communicationMethod ZooKeeper encrypted no schemaRegistryEncrypted no" +
                    " commitStreamTopicName " + commitTopic);

                // 3. User exit (KCOP). Requiere mapeos ya presentes (paso 1).
                connection.executeForMessage(
                    "modify subscription user exit type javaclass" +
                    " classname " + kcopClass +
                    " parameter " + IIDRConnection.quoteStringLiteral("-file:" + propsPath));
            } finally {
                connection.executeForMessage("unlock subscription name \"" + name + "\"");
            }

            Map<String, Object> data = new LinkedHashMap<>();
            data.put("subscription", name);
            data.put("tablesMapped", mappedNow);
            data.put("tablesAlreadyMapped", mappedSkipped);
            data.put("commitTopic", commitTopic);
            data.put("kcopClass", kcopClass);
            data.put("propertiesPath", propsPath);

            log.info("Suscripción '{}' configurada para Kafka (nuevos mapeos: {}, ya existían: {})",
                    name, mappedNow.size(), mappedSkipped.size());
            return CommandResult.ok("subscription.setup-kafka", data);

        } finally {
            SubscriptionHelper.disconnectBothDatastores(connection);
        }
    }

    private boolean isMapped(IIDRConnection connection, String subscription, String fullTable)
            throws Exception {
        ResultStringTable table = connection.execute(
                "list table mappings name \"" + subscription + "\"");
        if (table == null) return false;
        for (int i = 0; i < table.getRowCount(); i++) {
            String src = SubscriptionHelper.safeGet(table, i, "SOURCE TABLE").trim();
            if (src.equalsIgnoreCase(fullTable)) return true;
        }
        return false;
    }
}
