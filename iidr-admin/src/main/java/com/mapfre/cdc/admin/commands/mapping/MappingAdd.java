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
 * Añade el mapping de una tabla a una suscripción.
 * Idempotente: si la tabla ya está mapeada devuelve SKIPPED.
 * Actualiza el TableSubscriptionRegistry.
 *
 * Uso:
 *   iidr.sh mapping add
 *     --source <ds> --target <ds> --subscription <nombre>
 *     --sourceSchema <schema> --sourceTable <tabla>
 *     [--targetSchema <schema>] [--targetTable <tabla>]
 *
 * Si no se especifican targetSchema/targetTable se usan los mismos que los del source.
 */
public class MappingAdd implements Command {

    private static final Logger log = LoggerFactory.getLogger(MappingAdd.class);

    @Override
    public CommandResult execute(IIDRConnection connection, String[] args) throws Exception {

        Options opts = new Options();
        opts.addRequiredOption("s", "source",       true, "Datastore origen");
        opts.addRequiredOption("t", "target",       true, "Datastore destino");
        opts.addRequiredOption("n", "subscription", true, "Nombre de la suscripción");
        opts.addRequiredOption("ss", "sourceSchema", true, "Schema de la tabla origen");
        opts.addRequiredOption("st", "sourceTable",  true, "Nombre de la tabla origen");
        opts.addOption("ts", "targetSchema", true, "Schema de la tabla destino (por defecto igual que source)");
        opts.addOption("tt", "targetTable",  true, "Nombre de la tabla destino (por defecto igual que source)");
        CommandLine cli = new DefaultParser().parse(opts, args);

        String source       = cli.getOptionValue("source");
        String target       = cli.getOptionValue("target");
        String subscription = cli.getOptionValue("subscription");
        String srcSchema    = cli.getOptionValue("sourceSchema");
        String srcTable     = cli.getOptionValue("sourceTable");
        String tgtSchema    = cli.getOptionValue("targetSchema", srcSchema);
        String tgtTable     = cli.getOptionValue("targetTable",  srcTable);
        String fullTable    = srcSchema + "." + srcTable;

        log.info("Añadiendo mapping {} → {} en suscripción '{}'", fullTable, tgtSchema + "." + tgtTable, subscription);

        SubscriptionHelper.connectDatastore(connection, source);
        SubscriptionHelper.connectTargetDatastore(connection, target);
        try {
            // Guardar estado previo y parar si está corriendo
            String estadoPrevio = SubscriptionHelper.getState(connection, subscription);
            if (SubscriptionHelper.isRunning(estadoPrevio)) {
                return CommandResult.error("mapping.add",
                        "La suscripción '" + subscription + "' está en estado '" + estadoPrevio +
                        "'. Párala antes de añadir mappings.");
            }

            // Idempotencia: comprobar si la tabla ya está mapeada
            if (isMapped(connection, subscription, fullTable)) {
                log.info("Tabla '{}' ya está mapeada en '{}', nada que hacer", fullTable, subscription);
                return CommandResult.skipped("mapping.add",
                        "La tabla ya está mapeada en la suscripción: " + fullTable);
            }

            connection.executeForMessage(
                "lock subscription name \"" + subscription + "\"");

            try {
                String addCmd = "add table mapping" +
                    " sourceSchema " + srcSchema +
                    " sourceTable " + srcTable;
                if (!tgtSchema.equalsIgnoreCase(srcSchema) || !tgtTable.equalsIgnoreCase(srcTable)) {
                    addCmd += " targetSchema " + tgtSchema + " targetTable " + tgtTable;
                }
                connection.executeForMessage(addCmd);
            } finally {
                connection.executeForMessage(
                    "unlock subscription name \"" + subscription + "\"");
            }

            // Actualizar registro CSV
            TableSubscriptionRegistry registry =
                new TableSubscriptionRegistry(connection.getRegistryFile());
            registry.add(fullTable, subscription, source);

            log.info("Mapping añadido: {} en '{}'", fullTable, subscription);
            return CommandResult.ok("mapping.add",
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
