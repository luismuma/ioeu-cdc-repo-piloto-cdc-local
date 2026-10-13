package com.mapfre.cdc.admin.commands.subscription;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mapfre.cdc.admin.IIDRConnection;
import com.mapfre.cdc.admin.commands.Command;
import com.mapfre.cdc.admin.commands.CommandResult;

/**
 * Arranca la replicación continua de una suscripción.
 * Idempotente: si ya está en Mirror devuelve SKIPPED.
 *
 * Uso: iidr.sh subscription start --source <datastore> --name <suscripcion>
 */
public class SubscriptionStart implements Command {

    private static final Logger log = LoggerFactory.getLogger(SubscriptionStart.class);

    @Override
    public CommandResult execute(IIDRConnection connection, String[] args) throws Exception {

        Options opts = new Options();
        opts.addRequiredOption("s", "source", true, "Nombre del datastore origen");
        opts.addRequiredOption("n", "name",   true, "Nombre de la suscripción");
        CommandLine cli = new DefaultParser().parse(opts, args);
        String source = cli.getOptionValue("source");
        String name   = cli.getOptionValue("name");

        log.info("Arrancando suscripción '{}' en datastore '{}'", name, source);

        SubscriptionHelper.connectDatastore(connection, source);
        try {
            String state = SubscriptionHelper.getState(connection, name);
            log.info("Estado actual de '{}': '{}'", name, state);

            if (SubscriptionHelper.isRunning(state)) {
                log.info("Suscripción '{}' ya está activa ({}), nada que hacer", name, state);
                return CommandResult.skipped("subscription.start",
                        "La suscripción ya está activa: " + state);
            }

            connection.executeForMessage(
                "start mirroring filter subscription name \"" + name + "\"");

            String newState = SubscriptionHelper.getState(connection, name);
            log.info("Estado tras arranque: '{}'", newState);

            return CommandResult.ok("subscription.start",
                    java.util.Map.of("name", name, "state", newState));

        } finally {
            SubscriptionHelper.disconnectDatastore(connection);
        }
    }
}
