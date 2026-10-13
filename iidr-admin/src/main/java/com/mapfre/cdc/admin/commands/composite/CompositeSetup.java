package com.mapfre.cdc.admin.commands.composite;

import java.io.File;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ibm.replication.cdc.scripting.ResultStringTable;
import com.mapfre.cdc.admin.IIDRConnection;
import com.mapfre.cdc.admin.commands.Command;
import com.mapfre.cdc.admin.commands.CommandResult;
import com.mapfre.cdc.admin.commands.subscription.SubscriptionHelper;
import com.mapfre.cdc.admin.registry.TableSubscriptionRegistry;

/**
 * Operación compuesta: crea datastores + suscripción + mappings de una vez.
 * Totalmente idempotente: cada paso verifica el estado actual antes de actuar.
 *
 * Uso: iidr.sh composite setup --config <fichero.json>
 *
 * Formato del JSON de configuración:
 * {
 *   "source_datastore": { "name": "DS_SOURCE", "host": "host1", "port": 11701 },
 *   "target_datastore": { "name": "DS_TARGET", "host": "host2", "port": 11702 },
 *   "subscription":     { "name": "SUB_NAME", "description": "opcional" },
 *   "tables": [
 *     { "source": "SCHEMA.TABLA", "target": "SCHEMA.TABLA" },
 *     { "source": "SCHEMA.OTRA" }
 *   ]
 * }
 */
public class CompositeSetup implements Command {

    private static final Logger log = LoggerFactory.getLogger(CompositeSetup.class);

    // --- JSON model ----------------------------------------------------------

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Config {
        @JsonProperty("source_datastore") public DatastoreConfig sourceDatastore;
        @JsonProperty("target_datastore") public DatastoreConfig targetDatastore;
        @JsonProperty("subscription")     public SubConfig       subscription;
        @JsonProperty("tables")           public List<TableMapping> tables = new ArrayList<>();
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class DatastoreConfig {
        public String name;
        public String host;
        public int    port;
        public String description = "";
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class SubConfig {
        public String name;
        public String description = "";
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class TableMapping {
        public String source;          // "SCHEMA.TABLA" (obligatorio)
        public String target;          // "SCHEMA.TABLA" (por defecto = source)
    }

    // --- execute -------------------------------------------------------------

    @Override
    public CommandResult execute(IIDRConnection connection, String[] args) throws Exception {

        Options opts = new Options();
        opts.addRequiredOption("c", "config", true, "Fichero JSON de configuración");
        CommandLine cli = new DefaultParser().parse(opts, args);
        String configPath = cli.getOptionValue("config");

        Config cfg = new ObjectMapper().readValue(new File(configPath), Config.class);

        List<Map<String, Object>> steps = new ArrayList<>();

        // 1. Source datastore
        steps.add(setupDatastore(connection, cfg.sourceDatastore));

        // 2. Target datastore
        steps.add(setupDatastore(connection, cfg.targetDatastore));

        // 3. Subscription
        String subName    = cfg.subscription.name;
        String sourceName = cfg.sourceDatastore.name;
        String targetName = cfg.targetDatastore.name;
        steps.add(setupSubscription(connection, sourceName, targetName, cfg.subscription));

        // 4. Table mappings
        SubscriptionHelper.connectDatastore(connection, sourceName);
        SubscriptionHelper.connectTargetDatastore(connection, targetName);
        try {
            TableSubscriptionRegistry registry =
                    new TableSubscriptionRegistry(connection.getRegistryFile());

            for (TableMapping tm : cfg.tables) {
                String src    = tm.source.toUpperCase();
                String tgt    = (tm.target != null) ? tm.target.toUpperCase() : src;
                int dot       = src.indexOf('.');
                String srcSchema = src.substring(0, dot);
                String srcTable  = src.substring(dot + 1);
                int dotT         = tgt.indexOf('.');
                String tgtSchema = tgt.substring(0, dotT);
                String tgtTable  = tgt.substring(dotT + 1);

                String stepAction;
                if (isMapped(connection, subName, src)) {
                    log.info("Tabla '{}' ya está mapeada en '{}' (SKIPPED)", src, subName);
                    stepAction = "SKIPPED";
                } else {
                    log.info("Bloqueando suscripción '{}' para añadir mapping {}", subName, src);
                    connection.executeForMessage("lock subscription name \"" + subName + "\"");
                    connection.executeForMessage(
                            "add table mapping" +
                            " sourceSchema " + srcSchema +
                            " sourceTable " + srcTable +
                            " targetSchema " + tgtSchema +
                            " targetTable " + tgtTable);
                    connection.executeForMessage("unlock subscription name \"" + subName + "\"");
                    registry.add(src, subName, sourceName);
                    log.info("Mapping añadido: {} → {}", src, tgt);
                    stepAction = "OK";
                }
                Map<String, Object> step = new LinkedHashMap<>();
                step.put("step",   "mapping.add");
                step.put("table",  src);
                step.put("action", stepAction);
                steps.add(step);
            }
        } finally {
            SubscriptionHelper.disconnectBothDatastores(connection);
        }

        boolean anyError = steps.stream().anyMatch(s -> "ERROR".equals(s.get("action")));
        if (anyError) {
            return CommandResult.error("composite.setup",
                    "Uno o más pasos fallaron; revisa el detalle en 'steps'");
        }

        log.info("composite setup completado ({} pasos)", steps.size());
        return CommandResult.ok("composite.setup", Map.of("steps", steps));
    }

    // -------------------------------------------------------------------------

    private Map<String, Object> setupDatastore(IIDRConnection connection,
                                                DatastoreConfig ds) throws Exception {
        Map<String, Object> step = new LinkedHashMap<>();
        step.put("step", "datastore.create");
        step.put("name", ds.name);

        boolean exists = datastoreExists(connection, ds.name);
        if (exists) {
            log.info("Datastore '{}' ya existe (SKIPPED)", ds.name);
            step.put("action", "SKIPPED");
            return step;
        }

        String cmd = "add access datastore name \"" + ds.name + "\""
                   + " hostname " + ds.host
                   + " port " + ds.port
                   + " multiuser yes";
        if (ds.description != null && !ds.description.isEmpty()) {
            cmd += " description \"" + ds.description + "\"";
        }
        connection.executeForMessage(cmd);
        log.info("Datastore '{}' creado ({}:{})", ds.name, ds.host, ds.port);
        step.put("action", "OK");
        return step;
    }

    private Map<String, Object> setupSubscription(IIDRConnection connection,
                                                   String sourceName, String targetName,
                                                   SubConfig sub) throws Exception {
        Map<String, Object> step = new LinkedHashMap<>();
        step.put("step", "subscription.create");
        step.put("name", sub.name);

        SubscriptionHelper.connectDatastore(connection, sourceName);
        SubscriptionHelper.connectTargetDatastore(connection, targetName);
        try {
            List<Map<String, String>> existing = SubscriptionHelper.listSubscriptions(connection);
            boolean exists = existing.stream().anyMatch(s -> s.get("name").equalsIgnoreCase(sub.name));
            if (exists) {
                log.info("Suscripción '{}' ya existe (SKIPPED)", sub.name);
                step.put("action", "SKIPPED");
                return step;
            }

            String cmd = "add subscription name \"" + sub.name + "\"";
            if (sub.description != null && !sub.description.isEmpty()) {
                cmd += " description \"" + sub.description + "\"";
            }
            connection.executeForMessage(cmd);
            log.info("Suscripción '{}' creada", sub.name);
            step.put("action", "OK");
            return step;

        } finally {
            SubscriptionHelper.disconnectBothDatastores(connection);
        }
    }

    private boolean datastoreExists(IIDRConnection connection, String name) throws Exception {
        ResultStringTable table = connection.execute("list access datastores");
        if (table == null) return false;
        for (int i = 0; i < table.getRowCount(); i++) {
            if (name.equalsIgnoreCase(table.getValueAt(i, "DATASTORE"))) return true;
        }
        return false;
    }

    private boolean isMapped(IIDRConnection connection, String subscription,
                              String fullTable) throws Exception {
        ResultStringTable table = connection.execute(
                "list table mappings name \"" + subscription + "\"");
        if (table == null) return false;
        for (int i = 0; i < table.getRowCount(); i++) {
            if (table.getValueAt(i, "SOURCE TABLE").trim().equalsIgnoreCase(fullTable)) return true;
        }
        return false;
    }
}
