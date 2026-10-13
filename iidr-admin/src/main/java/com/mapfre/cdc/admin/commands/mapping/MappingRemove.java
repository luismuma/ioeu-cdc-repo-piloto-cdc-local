package com.mapfre.cdc.admin.commands.mapping;

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
import com.mapfre.cdc.admin.registry.TableSubscriptionRegistry;

/**
 * Elimina el mapping de una tabla de una suscripción.
 * Idempotente: si la tabla no está mapeada devuelve SKIPPED.
 * Actualiza el TableSubscriptionRegistry.
 *
 * Uso:
 *   iidr.sh mapping remove
 *     --source <ds> --target <ds> --subscription <nombre>
 *     --sourceSchema <schema> --sourceTable <tabla>
 */
public class MappingRemove implements Command {

    private static final Logger log = LoggerFactory.getLogger(MappingRemove.class);

    @Override
    public CommandResult execute(IIDRConnection connection, String[] args) throws Exception {

        Options opts = new Options();
        opts.addRequiredOption("s", "source",       true, "Datastore origen");
        opts.addRequiredOption("t", "target",       true, "Datastore destino");
        opts.addRequiredOption("n", "subscription", true, "Nombre de la suscripción");
        opts.addRequiredOption("ss", "sourceSchema", true, "Schema de la tabla origen");
        opts.addRequiredOption("st", "sourceTable",  true, "Nombre de la tabla origen");
        CommandLine cli = new DefaultParser().parse(opts, args);

        String source       = cli.getOptionValue("source");
        String target       = cli.getOptionValue("target");
        String subscription = cli.getOptionValue("subscription");
        String srcSchema    = cli.getOptionValue("sourceSchema");
        String srcTable     = cli.getOptionValue("sourceTable");
        String fullTable    = srcSchema + "." + srcTable;

        log.info("Eliminando mapping {} de suscripción '{}'", fullTable, subscription);

        SubscriptionHelper.connectDatastore(connection, source);
        SubscriptionHelper.connectTargetDatastore(connection, target);
        try {
            // Idempotencia: comprobar si la tabla está mapeada
            if (!isMapped(connection, subscription, fullTable)) {
                log.info("Tabla '{}' no está mapeada en '{}', nada que hacer", fullTable, subscription);
                return CommandResult.skipped("mapping.remove",
                        "La tabla no está mapeada en la suscripción: " + fullTable);
            }

            connection.executeForMessage(
                "lock subscription name \"" + subscription + "\"");

            try {
                connection.executeForMessage(
                    "select table mapping sourceSchema " + srcSchema + " sourceTable " + srcTable);
                connection.executeForMessage("delete table mapping");
            } finally {
                connection.executeForMessage(
                    "unlock subscription name \"" + subscription + "\"");
            }

            // Actualizar registro CSV
            TableSubscriptionRegistry registry =
                new TableSubscriptionRegistry(connection.getRegistryFile());
            registry.remove(fullTable, subscription);

            log.info("Mapping eliminado: {} de '{}'", fullTable, subscription);
            return CommandResult.ok("mapping.remove",
                    Map.of("table", fullTable, "subscription", subscription));

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
            String src = table.getValueAt(i, "SOURCE TABLE").trim();
            if (src.equalsIgnoreCase(fullTable)) return true;
        }
        return false;
    }
}
