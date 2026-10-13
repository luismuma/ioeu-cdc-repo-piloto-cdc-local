package com.mapfre.cdc.admin.commands.composite;

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
import com.mapfre.cdc.admin.commands.subscription.SubscriptionHelper;

/**
 * Verifica el estado de salud de datastores y suscripciones.
 *
 * Uso: iidr.sh composite health-check [--source <datastore>]
 *
 *   Sin --source: verifica todos los datastores registrados en el Access Server.
 *   Con --source: verifica únicamente ese datastore.
 *
 * Devuelve:
 *   OK    → todos los datastores accesibles y todas las suscripciones en Mirror Continuous
 *   ERROR → alguna suscripción en estado de error, parada, o algún datastore inaccesible
 */
public class CompositeHealthCheck implements Command {

    private static final Logger log = LoggerFactory.getLogger(CompositeHealthCheck.class);

    @Override
    public CommandResult execute(IIDRConnection connection, String[] args) throws Exception {

        Options opts = new Options();
        opts.addOption("s", "source", true, "Datastore a verificar (por defecto: todos)");
        CommandLine cli = new DefaultParser().parse(opts, args);
        String filterSource = cli.getOptionValue("source");

        // Determinar qué datastores verificar
        List<String> datastores = new ArrayList<>();
        if (filterSource != null && !filterSource.isBlank()) {
            datastores.add(filterSource);
        } else {
            ResultStringTable dsTable = connection.execute("list access datastores");
            if (dsTable != null) {
                for (int i = 0; i < dsTable.getRowCount(); i++) {
                    datastores.add(dsTable.getValueAt(i, "DATASTORE"));
                }
            }
        }

        List<Map<String, Object>> dsResults = new ArrayList<>();
        boolean allHealthy = true;

        for (String dsName : datastores) {
            Map<String, Object> dsResult = new LinkedHashMap<>();
            dsResult.put("datastore", dsName);

            List<Map<String, Object>> subResults = new ArrayList<>();
            boolean dsHealthy = true;

            try {
                SubscriptionHelper.connectDatastore(connection, dsName);
                try {
                    List<Map<String, String>> subs = SubscriptionHelper.listSubscriptions(connection);
                    Map<String, Map<String, String>> monitor = SubscriptionHelper.monitorReplication(connection);

                    if (subs.isEmpty()) {
                        log.info("Datastore '{}': sin suscripciones", dsName);
                        dsResult.put("status", "OK");
                        dsResult.put("subscriptions", subResults);
                        dsResults.add(dsResult);
                        continue;
                    }

                    for (Map<String, String> sub : subs) {
                        String subName = sub.get("name");
                        Map<String, String> monEntry = monitor.get(subName);
                        String state   = monEntry != null ? monEntry.getOrDefault("state",   "") : "";
                        String latency = monEntry != null ? monEntry.getOrDefault("latency", "") : "";

                        boolean subHealthy = "Mirror Continuous".equals(state);
                        if (!subHealthy) {
                            dsHealthy  = false;
                            allHealthy = false;
                        }

                        Map<String, Object> subResult = new LinkedHashMap<>();
                        subResult.put("name",    subName);
                        subResult.put("state",   state);
                        subResult.put("latency", latency);
                        subResult.put("healthy", subHealthy);
                        subResults.add(subResult);

                        log.info("  {}/{}: state={} latency={} healthy={}",
                                dsName, subName, state, latency, subHealthy);
                    }

                } finally {
                    SubscriptionHelper.disconnectDatastore(connection);
                }

                dsResult.put("status",        dsHealthy ? "OK" : "ERROR");
                dsResult.put("subscriptions", subResults);

            } catch (Exception e) {
                log.warn("Error accediendo al datastore '{}': {}", dsName, e.getMessage());
                dsResult.put("status", "ERROR");
                dsResult.put("error",  e.getMessage());
                dsResult.put("subscriptions", subResults);
                allHealthy = false;
            }

            dsResults.add(dsResult);
        }

        Map<String, Object> data = new LinkedHashMap<>();
        data.put("overall",    allHealthy ? "OK" : "ERROR");
        data.put("datastores", dsResults);

        if (allHealthy) {
            log.info("Health check OK — todos los datastores y suscripciones en buen estado");
            return CommandResult.ok("composite.health-check", data);
        } else {
            log.warn("Health check ERROR — hay suscripciones en estado anómalo");
            return CommandResult.error("composite.health-check",
                    "Hay suscripciones en estado anómalo o datastores inaccesibles");
        }
    }
}
