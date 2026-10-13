package com.mapfre.cdc.admin.commands.datastore;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.replication.cdc.scripting.ResultStringTable;
import com.mapfre.cdc.admin.IIDRConnection;
import com.mapfre.cdc.admin.commands.Command;
import com.mapfre.cdc.admin.commands.CommandResult;

/**
 * Muestra el detalle de un datastore.
 *
 * Uso: iidr.sh datastore show --name <nombre>
 *
 * Salida JSON de ejemplo:
 * {
 *   "status": "OK",
 *   "operation": "datastore.show",
 *   "data": {
 *     "name": "IIDR_GR_DB2_E01_AWS_DEV",
 *     "host": "lesawsew131209.es.mapfre.net",
 *     "port": "11801",
 *     "version": "V11R4M0",
 *     "database": "JDBC",
 *     "type": "Source",
 *     "status": "Not connected"
 *   }
 * }
 */
public class DatastoreShow implements Command {

    private static final Logger log = LoggerFactory.getLogger(DatastoreShow.class);

    @Override
    public CommandResult execute(IIDRConnection connection, String[] args) throws Exception {

        Options opts = new Options();
        opts.addRequiredOption("n", "name", true, "Nombre del datastore");
        CommandLine cli = new DefaultParser().parse(opts, args);
        String name = cli.getOptionValue("name");

        log.info("Buscando datastore '{}'", name);

        ResultStringTable table = connection.execute("list datastores");

        if (table != null) {
            for (int i = 0; i < table.getRowCount(); i++) {
                if (name.equalsIgnoreCase(DatastoreList.safeGet(table, i, "DATASTORE"))) {
                    return CommandResult.ok("datastore.show", DatastoreList.parseRow(table, i));
                }
            }
        }

        return CommandResult.error("datastore.show", "Datastore no encontrado: " + name);
    }
}
