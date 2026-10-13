package com.mapfre.cdc.admin.commands.mapping;

import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mapfre.cdc.admin.IIDRConnection;
import com.mapfre.cdc.admin.commands.Command;
import com.mapfre.cdc.admin.commands.CommandResult;
import com.mapfre.cdc.admin.commands.subscription.SubscriptionHelper;

/**
 * Gestiona el estado de replicación de un mapping de tabla.
 *
 * Subcomandos (--action):
 *   park         → Parkea la tabla (Do not replicate)
 *   flag-refresh → Marca la tabla para refresh (carga inicial + mirror)
 *   set-method   → Cambia el método (mirror|refresh); parkea antes si es necesario
 *
 * Uso:
 *   iidr.sh mapping refresh
 *     --source <ds> --target <ds> --subscription <nombre>
 *     --sourceSchema <schema> --sourceTable <tabla>
 *     --action park|flag-refresh|set-method
 *     [--method mirror|refresh]   (solo con set-method)
 */
public class MappingRefresh implements Command {

    private static final Logger log = LoggerFactory.getLogger(MappingRefresh.class);

    @Override
    public CommandResult execute(IIDRConnection connection, String[] args) throws Exception {

        Options opts = new Options();
        opts.addRequiredOption("s",  "source",       true, "Datastore origen");
        opts.addRequiredOption("t",  "target",       true, "Datastore destino");
        opts.addRequiredOption("n",  "subscription", true, "Nombre de la suscripción");
        opts.addRequiredOption("ss", "sourceSchema", true, "Schema de la tabla origen");
        opts.addRequiredOption("st", "sourceTable",  true, "Nombre de la tabla origen");
        opts.addRequiredOption("a",  "action",       true, "Acción: park, flag-refresh, set-method");
        opts.addOption("m", "method", true, "Método de replicación: mirror, refresh (solo con set-method)");
        CommandLine cli = new DefaultParser().parse(opts, args);

        String source       = cli.getOptionValue("source");
        String target       = cli.getOptionValue("target");
        String subscription = cli.getOptionValue("subscription");
        String srcSchema    = cli.getOptionValue("sourceSchema");
        String srcTable     = cli.getOptionValue("sourceTable");
        String action       = cli.getOptionValue("action").toLowerCase();
        String fullTable    = srcSchema + "." + srcTable;

        log.info("mapping refresh --action {} sobre {} en '{}'", action, fullTable, subscription);

        SubscriptionHelper.connectDatastore(connection, source);
        SubscriptionHelper.connectTargetDatastore(connection, target);
        try {
            String state = SubscriptionHelper.getState(connection, subscription);
            if (SubscriptionHelper.isRunning(state)) {
                return CommandResult.error("mapping.refresh",
                        "La suscripción '" + subscription + "' está en estado '" + state +
                        "'. Párala antes de modificar mappings.");
            }

            connection.executeForMessage(
                "lock subscription name \"" + subscription + "\"");
            try {
                connection.executeForMessage(
                    "select subscription name \"" + subscription + "\"");
                connection.executeForMessage(
                    "select table mapping sourceSchema " + srcSchema + " sourceTable " + srcTable);

                switch (action) {
                    case "park":
                        connection.executeForMessage("park table mapping");
                        log.info("Tabla {} parkeada en '{}'", fullTable, subscription);
                        break;

                    case "flag-refresh":
                        connection.executeForMessage("flag refresh");
                        log.info("Tabla {} marcada para refresh en '{}'", fullTable, subscription);
                        break;

                    case "set-method":
                        String method = cli.getOptionValue("method");
                        if (method == null || (!method.equalsIgnoreCase("mirror") && !method.equalsIgnoreCase("refresh"))) {
                            return CommandResult.error("mapping.refresh",
                                    "--method es obligatorio con set-method y debe ser 'mirror' o 'refresh'");
                        }
                        connection.executeForMessage("park table mapping");
                        connection.executeForMessage("modify table mapping method " + method.toLowerCase());
                        log.info("Método de {} cambiado a '{}' en '{}'", fullTable, method, subscription);
                        break;

                    default:
                        return CommandResult.error("mapping.refresh",
                                "Acción desconocida: '" + action + "'. Usa: park, flag-refresh, set-method");
                }
            } finally {
                connection.executeForMessage(
                    "unlock subscription name \"" + subscription + "\"");
            }

            return CommandResult.ok("mapping.refresh",
                    Map.of("table", fullTable, "subscription", subscription, "action", action));

        } finally {
            SubscriptionHelper.disconnectBothDatastores(connection);
        }
    }
}
