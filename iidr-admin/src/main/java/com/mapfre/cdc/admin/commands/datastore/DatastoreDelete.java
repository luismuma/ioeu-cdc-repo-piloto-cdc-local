package com.mapfre.cdc.admin.commands.datastore;

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

/**
 * Elimina un datastore del Access Server.
 * Idempotente: si no existe devuelve SKIPPED.
 *
 * Uso: iidr.sh datastore delete --name <nombre>
 *
 * Nota: IIDR rechazará la operación si el datastore tiene suscripciones
 * activas. Elimínalas primero con "subscription delete".
 */
public class DatastoreDelete implements Command {

    private static final Logger log = LoggerFactory.getLogger(DatastoreDelete.class);

    @Override
    public CommandResult execute(IIDRConnection connection, String[] args) throws Exception {

        Options opts = new Options();
        opts.addRequiredOption("n", "name", true, "Nombre del datastore");
        CommandLine cli = new DefaultParser().parse(opts, args);
        String name = cli.getOptionValue("name");

        log.info("Eliminando datastore '{}'", name);

        // Idempotencia: comprobar si existe
        ResultStringTable existing = connection.execute("list access datastores");
        boolean exists = false;
        if (existing != null) {
            for (int i = 0; i < existing.getRowCount(); i++) {
                if (name.equalsIgnoreCase(existing.getValueAt(i, "DATASTORE"))) {
                    exists = true;
                    break;
                }
            }
        }

        if (!exists) {
            log.info("Datastore '{}' no existe, nada que hacer", name);
            return CommandResult.skipped("datastore.delete",
                    "El datastore no existe: " + name);
        }

        connection.executeForMessage("delete access datastore name \"" + name + "\"");

        log.info("Datastore '{}' eliminado", name);
        return CommandResult.ok("datastore.delete", Map.of("name", name));
    }
}
