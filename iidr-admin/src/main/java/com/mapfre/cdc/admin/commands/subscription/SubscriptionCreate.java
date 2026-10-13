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
 * Crea una nueva suscripción entre dos datastores.
 * Idempotente: si ya existe devuelve SKIPPED.
 *
 * Uso: iidr.sh subscription create
 *        --source <ds> --target <ds> --name <nombre> [--description <desc>]
 */
public class SubscriptionCreate implements Command {

    private static final Logger log = LoggerFactory.getLogger(SubscriptionCreate.class);

    @Override
    public CommandResult execute(IIDRConnection connection, String[] args) throws Exception {

        Options opts = new Options();
        opts.addRequiredOption("s", "source",      true, "Datastore origen");
        opts.addRequiredOption("t", "target",      true, "Datastore destino");
        opts.addRequiredOption("n", "name",        true, "Nombre de la suscripción");
        opts.addOption("d", "description", true, "Descripción (opcional)");
        CommandLine cli = new DefaultParser().parse(opts, args);
        String source      = cli.getOptionValue("source");
        String target      = cli.getOptionValue("target");
        String name        = cli.getOptionValue("name");
        String description = cli.getOptionValue("description", "");

        log.info("Creando suscripción '{}' ({} → {})", name, source, target);

        SubscriptionHelper.connectDatastore(connection, source);
        SubscriptionHelper.connectTargetDatastore(connection, target);
        try {
            // Idempotencia: comprobar si ya existe
            List<Map<String, String>> existing = SubscriptionHelper.listSubscriptions(connection);
            boolean exists = existing.stream()
                    .anyMatch(s -> s.get("name").equalsIgnoreCase(name));
            if (exists) {
                log.info("Suscripción '{}' ya existe, nada que hacer", name);
                return CommandResult.skipped("subscription.create",
                        "La suscripción ya existe: " + name);
            }

            String cmd = "add subscription name \"" + name + "\"";
            if (!description.isEmpty()) cmd += " description \"" + description + "\"";
            connection.executeForMessage(cmd);

            log.info("Suscripción '{}' creada", name);
            return CommandResult.ok("subscription.create",
                    Map.of("name", name, "sourceDatastore", source, "targetDatastore", target));

        } finally {
            SubscriptionHelper.disconnectBothDatastores(connection);
        }
    }
}
