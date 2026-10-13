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
 * Operación compuesta: para y elimina suscripción y opcionalmente los datastores.
 * Idempotente: si ya está parada o no existe, continúa sin error.
 *
 * Uso: iidr.sh composite teardown
 *        --subscription <nombre> --source <ds> --target <ds>
 *        [--delete-datastores]
 *
 * Orden de operaciones:
 *   1. Stop mirroring (si activa)
 *   2. Delete subscription
 *   3. (Opcional) Delete datastores si --delete-datastores
 */
public class CompositeTeardown implements Command {

    private static final Logger log = LoggerFactory.getLogger(CompositeTeardown.class);

    @Override
    public CommandResult execute(IIDRConnection connection, String[] args) throws Exception {

        Options opts = new Options();
        opts.addRequiredOption("n", "subscription",      true,  "Nombre de la suscripción");
        opts.addRequiredOption("s", "source",            true,  "Datastore origen");
        opts.addRequiredOption("t", "target",            true,  "Datastore destino");
        opts.addOption(        "d", "delete-datastores", false, "Eliminar también los datastores tras borrar la suscripción");
        CommandLine cli = new DefaultParser().parse(opts, args);
        String subName         = cli.getOptionValue("subscription");
        String sourceName      = cli.getOptionValue("source");
        String targetName      = cli.getOptionValue("target");
        boolean deleteDatastores = cli.hasOption("delete-datastores");

        List<Map<String, Object>> steps = new ArrayList<>();

        // -- 1 & 2: stop + delete subscription --------------------------------
        SubscriptionHelper.connectDatastore(connection, sourceName);
        SubscriptionHelper.connectTargetDatastore(connection, targetName);
        try {
            List<Map<String, String>> existing = SubscriptionHelper.listSubscriptions(connection);
            boolean exists = existing.stream().anyMatch(s -> s.get("name").equalsIgnoreCase(subName));

            if (!exists) {
                log.info("Suscripción '{}' no existe (SKIPPED)", subName);
                steps.add(step("subscription.stop",   subName, "SKIPPED"));
                steps.add(step("subscription.delete", subName, "SKIPPED"));
            } else {
                // Stop if running
                String state = SubscriptionHelper.getState(connection, subName);
                if (SubscriptionHelper.isRunning(state)) {
                    log.info("Parando suscripción '{}' (estado: {})", subName, state);
                    connection.executeForMessage(
                            "end replication filter subscription name \"" + subName
                            + "\" method immediate");
                    steps.add(step("subscription.stop", subName, "OK"));
                } else {
                    log.info("Suscripción '{}' ya está parada (estado: {})", subName, state);
                    steps.add(step("subscription.stop", subName, "SKIPPED"));
                }

                // Delete
                connection.executeForMessage("delete subscription name \"" + subName + "\"");
                log.info("Suscripción '{}' eliminada", subName);
                steps.add(step("subscription.delete", subName, "OK"));
            }
        } finally {
            SubscriptionHelper.disconnectBothDatastores(connection);
        }

        // -- 3: delete datastores (opcional) ----------------------------------
        if (deleteDatastores) {
            steps.add(deleteDatastore(connection, sourceName));
            steps.add(deleteDatastore(connection, targetName));
        }

        log.info("composite teardown completado ({} pasos)", steps.size());
        return CommandResult.ok("composite.teardown", Map.of("steps", steps));
    }

    private Map<String, Object> deleteDatastore(IIDRConnection connection,
                                                 String name) throws Exception {
        ResultStringTable table = connection.execute("list access datastores");
        boolean exists = false;
        if (table != null) {
            for (int i = 0; i < table.getRowCount(); i++) {
                if (name.equalsIgnoreCase(table.getValueAt(i, "DATASTORE"))) {
                    exists = true;
                    break;
                }
            }
        }
        if (!exists) {
            log.info("Datastore '{}' no existe (SKIPPED)", name);
            return step("datastore.delete", name, "SKIPPED");
        }
        connection.executeForMessage("delete access datastore name \"" + name + "\"");
        log.info("Datastore '{}' eliminado", name);
        return step("datastore.delete", name, "OK");
    }

    private static Map<String, Object> step(String stepName, String name, String action) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("step",   stepName);
        m.put("name",   name);
        m.put("action", action);
        return m;
    }
}
