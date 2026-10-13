package com.mapfre.cdc.admin.commands.subscription;

import java.util.List;
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
 * Lista las suscripciones de un datastore origen, incluyendo su estado de replicación.
 *
 * Uso: iidr.sh subscription list --source <datastore>
 *
 * Salida JSON de ejemplo:
 * {
 *   "status": "OK",
 *   "operation": "subscription.list",
 *   "data": [
 *     {
 *       "name": "DESATST1",
 *       "sourceDatastore": "IIDR_GR_DB2_E01_AWS_DEV",
 *       "targetDatastore": "IIDR_GR_KFK_R01_AWS_DEV",
 *       "state": "Mirror Continuous",
 *       "latency": ""
 *     }
 *   ]
 * }
 */
public class SubscriptionList implements Command {

    private static final Logger log = LoggerFactory.getLogger(SubscriptionList.class);

    @Override
    public CommandResult execute(IIDRConnection connection, String[] args) throws Exception {

        Options opts = new Options();
        opts.addRequiredOption("s", "source", true, "Nombre del datastore origen");
        CommandLine cli = new DefaultParser().parse(opts, args);
        String source = cli.getOptionValue("source");

        log.info("Listando suscripciones del datastore '{}'", source);

        SubscriptionHelper.connectDatastore(connection, source);
        try {
            List<Map<String, String>> subscriptions = SubscriptionHelper.listSubscriptions(connection);

            if (subscriptions.isEmpty()) {
                log.info("No se encontraron suscripciones en '{}'", source);
                return CommandResult.ok("subscription.list", subscriptions);
            }

            // Enriquecer con estado de replicación
            Map<String, Map<String, String>> monitor = SubscriptionHelper.monitorReplication(connection);
            for (Map<String, String> sub : subscriptions) {
                Map<String, String> status = monitor.get(sub.get("name"));
                sub.put("state",   status != null ? status.get("state")   : "");
                sub.put("latency", status != null ? status.get("latency") : "");
            }

            log.info("Suscripciones encontradas: {}", subscriptions.size());
            return CommandResult.ok("subscription.list", subscriptions);

        } finally {
            SubscriptionHelper.disconnectDatastore(connection);
        }
    }
}
