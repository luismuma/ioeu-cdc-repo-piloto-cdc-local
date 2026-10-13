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
 * Dada una tabla origen, devuelve las suscripciones en las que está mapeada.
 *
 * Uso: iidr.sh mapping find-by-table --table <SCHEMA.TABLA> [--live-check]
 *
 * Por defecto consulta el registro local (rápido).
 * Con --live-check recorre todos los datastores y suscripciones en IIDR,
 * sincroniza el CSV con el estado real y devuelve el resultado actualizado.
 * Útil para el bootstrap inicial o si se han hecho cambios desde la Management Console.
 */
public class MappingFindByTable implements Command {

    private static final Logger log = LoggerFactory.getLogger(MappingFindByTable.class);

    @Override
    public CommandResult execute(IIDRConnection connection, String[] args) throws Exception {

        Options options = new Options();
        options.addRequiredOption("t", "table",      true,  "Tabla origen (SCHEMA.TABLA)");
        options.addOption(        "l", "live-check", false, "Sincronizar registro desde IIDR antes de consultar");

        CommandLine cli = new DefaultParser().parse(options, args);
        String table    = cli.getOptionValue("table").toUpperCase();
        boolean liveCheck = cli.hasOption("live-check");

        TableSubscriptionRegistry registry =
                new TableSubscriptionRegistry(connection.getRegistryFile());

        if (liveCheck) {
            syncRegistryFromIIDR(connection, registry);
        }

        List<TableSubscriptionRegistry.Entry> localEntries = registry.findByTable(table);

        List<Map<String, String>> results = new ArrayList<>();
        for (TableSubscriptionRegistry.Entry e : localEntries) {
            Map<String, String> row = new LinkedHashMap<>();
            row.put("table",        e.table());
            row.put("subscription", e.subscription());
            row.put("datastore",    e.datastore());
            results.add(row);
        }

        return CommandResult.ok("mapping.find-by-table",
                Map.of("table", table, "subscriptions", results));
    }

    /**
     * Itera todos los datastores del Access Server y sincroniza el registro CSV
     * con el estado real de IIDR. Esto cubre:
     *   - Mappings creados desde la Management Console
     *   - Mappings eliminados desde la Management Console
     *   - Bootstrap inicial (CSV vacío)
     */
    private void syncRegistryFromIIDR(IIDRConnection connection,
                                       TableSubscriptionRegistry registry) throws Exception {
        log.info("Sincronizando registro desde IIDR (live-check)...");

        ResultStringTable dsTable = connection.execute("list access datastores");
        if (dsTable == null) {
            log.warn("No se encontraron datastores en el Access Server");
            return;
        }

        int totalSubs = 0;
        int totalMappings = 0;

        for (int d = 0; d < dsTable.getRowCount(); d++) {
            String dsName = dsTable.getValueAt(d, "DATASTORE");
            log.info("Procesando datastore '{}'...", dsName);

            try {
                SubscriptionHelper.connectDatastore(connection, dsName);
            } catch (Exception e) {
                log.warn("No se pudo conectar a '{}' ({}), omitiendo", dsName, e.getMessage());
                continue;
            }

            try {
                List<Map<String, String>> subs = SubscriptionHelper.listSubscriptions(connection);
                for (Map<String, String> sub : subs) {
                    String subName    = sub.get("name");
                    String targetDs   = sub.get("targetDatastore");

                    // Conectar target para poder listar los mappings
                    boolean targetConnected = false;
                    if (targetDs != null && !targetDs.isBlank()) {
                        try {
                            SubscriptionHelper.connectTargetDatastore(connection, targetDs);
                            targetConnected = true;
                        } catch (Exception e) {
                            log.warn("No se pudo conectar target '{}' para suscripción '{}' ({})",
                                    targetDs, subName, e.getMessage());
                        }
                    }

                    try {
                        ResultStringTable mappingTable = connection.execute(
                                "list table mappings name \"" + subName + "\"");
                        List<String> tableNames = new ArrayList<>();
                        if (mappingTable != null) {
                            for (int m = 0; m < mappingTable.getRowCount(); m++) {
                                String srcTable = mappingTable.getValueAt(m, "SOURCE TABLE").trim();
                                if (!srcTable.isEmpty()) tableNames.add(srcTable);
                            }
                        }
                        registry.syncSubscription(subName, dsName, tableNames);
                        totalSubs++;
                        totalMappings += tableNames.size();
                    } finally {
                        if (targetConnected) {
                            try {
                                connection.execute("disconnect datastore context target");
                            } catch (Exception ignored) {}
                        }
                    }
                }
            } finally {
                SubscriptionHelper.disconnectDatastore(connection);
            }
        }

        log.info("Sincronización completada: {} suscripciones, {} mappings", totalSubs, totalMappings);
    }
}
