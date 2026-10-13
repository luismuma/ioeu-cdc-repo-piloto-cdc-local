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
 * Registra un nuevo datastore en el Access Server.
 * Idempotente: si ya existe devuelve SKIPPED.
 *
 * Uso: iidr.sh datastore create
 *        --name <nombre> --host <host> --port <puerto> [--description <desc>]
 *
 * Pensado para ejecutarse autenticado como APP-cdcadmin (operación de
 * provisioning). Tras la creación, dar acceso al usuario operacional
 * (típicamente APP-cdcauto) con: iidr.sh datastore grant --name <name> --user APP-cdcauto
 */
public class DatastoreCreate implements Command {

    private static final Logger log = LoggerFactory.getLogger(DatastoreCreate.class);

    @Override
    public CommandResult execute(IIDRConnection connection, String[] args) throws Exception {

        Options opts = new Options();
        opts.addRequiredOption("n", "name", true, "Nombre del datastore");
        opts.addRequiredOption("H", "host", true, "Hostname del agente CDC");
        opts.addRequiredOption("p", "port", true, "Puerto del agente CDC");
        opts.addOption("d", "description", true, "Descripción (opcional)");
        CommandLine cli = new DefaultParser().parse(opts, args);
        String name        = cli.getOptionValue("name");
        String host        = cli.getOptionValue("host");
        String port        = cli.getOptionValue("port");
        String description = cli.getOptionValue("description", "");

        log.info("Creando datastore '{}' ({}:{})", name, host, port);

        // Idempotencia: comprobar si ya existe
        ResultStringTable existing = connection.execute("list access datastores");
        if (existing != null) {
            for (int i = 0; i < existing.getRowCount(); i++) {
                if (name.equalsIgnoreCase(existing.getValueAt(i, "DATASTORE"))) {
                    log.info("Datastore '{}' ya existe, nada que hacer", name);
                    return CommandResult.skipped("datastore.create",
                            "El datastore ya existe: " + name);
                }
            }
        }

        String cmd = "add access datastore name \"" + name + "\"" +
                     " hostname " + host +
                     " port " + port +
                     " multiuser yes";
        if (!description.isEmpty()) cmd += " description \"" + description + "\"";
        connection.executeForMessage(cmd);

        log.info("Datastore '{}' creado", name);
        return CommandResult.ok("datastore.create",
                Map.of("name", name, "host", host, "port", port));
    }
}
