package com.mapfre.cdc.admin.commands.mapping;

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
import com.mapfre.cdc.admin.registry.TableSubscriptionRegistry;

/**
 * Lista todas las tablas mapeadas en una suscripción.
 *
 * Uso: iidr.sh mapping list --source <ds> --target <ds> --subscription <nombre>
 */
public class MappingList implements Command {

    private static final Logger log = LoggerFactory.getLogger(MappingList.class);

    @Override
    public CommandResult execute(IIDRConnection connection, String[] args) throws Exception {

        Options opts = new Options();
        opts.addRequiredOption("s", "source",       true, "Datastore origen");
        opts.addRequiredOption("t", "target",       true, "Datastore destino");
        opts.addRequiredOption("n", "subscription", true, "Nombre de la suscripción");
        CommandLine cli = new DefaultParser().parse(opts, args);
        String source       = cli.getOptionValue("source");
        String target       = cli.getOptionValue("target");
        String subscription = cli.getOptionValue("subscription");

        log.info("Listando mappings de suscripción '{}'", subscription);

        SubscriptionHelper.connectDatastore(connection, source);
        SubscriptionHelper.connectTargetDatastore(connection, target);
        try {
            ResultStringTable table = connection.execute(
                    "list table mappings name \"" + subscription + "\"");

            List<Map<String, String>> mappings = new ArrayList<>();
            List<String> tableNames = new ArrayList<>();
            if (table != null) {
                for (int i = 0; i < table.getRowCount(); i++) {
                    Map<String, String> m = new LinkedHashMap<>();
                    String srcTable = safeGet(table, i, "SOURCE TABLE").trim();
                    m.put("sourceTable",  srcTable);
                    m.put("target",       safeGet(table, i, "TARGET"));
                    m.put("mappingType",  safeGet(table, i, "MAPPING TYPE"));
                    m.put("method",       safeGet(table, i, "METHOD"));
                    m.put("status",       safeGet(table, i, "STATUS"));
                    mappings.add(m);
                    if (!srcTable.isEmpty()) tableNames.add(srcTable);
                }
            }

            // Sincronizar registro CSV con el estado real de IIDR
            TableSubscriptionRegistry registry =
                    new TableSubscriptionRegistry(connection.getRegistryFile());
            registry.syncSubscription(subscription, source, tableNames);

            log.info("Mappings encontrados: {}", mappings.size());
            return CommandResult.ok("mapping.list", mappings);

        } finally {
            SubscriptionHelper.disconnectBothDatastores(connection);
        }
    }

    private String safeGet(ResultStringTable table, int row, String column) {
        try { return table.getValueAt(row, column); } catch (Exception e) { return ""; }
    }
}
