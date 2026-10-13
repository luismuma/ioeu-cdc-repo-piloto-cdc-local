package com.mapfre.cdc.admin.commands.subscription;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mapfre.cdc.admin.IIDRConnection;
import com.mapfre.cdc.admin.commands.Command;
import com.mapfre.cdc.admin.commands.CommandResult;

/**
 * Muestra el detalle y estado de una suscripción.
 *
 * Uso: iidr.sh subscription show --source <datastore> --name <suscripcion>
 *
 * Salida JSON de ejemplo:
 * {
 *   "status": "OK",
 *   "operation": "subscription.show",
 *   "data": {
 *     "name": "DESATST1",
 *     "description": "DESA: TEST subscripion 1",
 *     "sourceDatastore": "IIDR_GR_DB2_E01_AWS_DEV",
 *     "targetDatastore": "IIDR_GR_KFK_R01_AWS_DEV",
 *     "state": "Mirror Continuous",
 *     "latency": "",
 *     "locked": "",
 *     "persistency": "No"
 *   }
 * }
 */
public class SubscriptionShow implements Command {

    private static final Logger log = LoggerFactory.getLogger(SubscriptionShow.class);

    @Override
    public CommandResult execute(IIDRConnection connection, String[] args) throws Exception {

        Options opts = new Options();
        opts.addRequiredOption("s", "source", true, "Nombre del datastore origen");
        opts.addRequiredOption("n", "name",   true, "Nombre de la suscripción");
        CommandLine cli = new DefaultParser().parse(opts, args);
        String source = cli.getOptionValue("source");
        String name   = cli.getOptionValue("name");

        log.info("Mostrando suscripción '{}' en datastore '{}'", name, source);

        SubscriptionHelper.connectDatastore(connection, source);
        try {
            Map<String, String> props = SubscriptionHelper.showSubscription(connection, name);

            if (props.isEmpty()) {
                return CommandResult.error("subscription.show",
                        "Suscripción no encontrada: " + name);
            }

            // Estado de replicación desde monitor replication
            Map<String, Map<String, String>> monitor = SubscriptionHelper.monitorReplication(connection);
            Map<String, String> status = monitor.get(name);

            Map<String, Object> data = new LinkedHashMap<>();
            data.put("name",             props.getOrDefault("Name",              name));
            data.put("description",      props.getOrDefault("Description",       ""));
            data.put("sourceDatastore",  props.getOrDefault("Source Datastore",  ""));
            data.put("targetDatastore",  props.getOrDefault("Target Datastore",  ""));
            data.put("state",            status != null ? status.get("state")   : "");
            data.put("latency",          status != null ? status.get("latency") : "");
            data.put("locked",           props.getOrDefault("Locked By User",    ""));
            data.put("persistency",      props.getOrDefault("Persistency",       ""));
            data.put("transferableWork", props.getOrDefault("Transferable Work", ""));

            return CommandResult.ok("subscription.show", data);

        } finally {
            SubscriptionHelper.disconnectDatastore(connection);
        }
    }
}
