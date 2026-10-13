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
 * Para la replicación de una suscripción.
 * Idempotente: si ya está parada devuelve SKIPPED.
 *
 * Uso: iidr.sh subscription stop --source <datastore> --name <suscripcion>
 *
 * El método de parada por defecto es "normal" (espera a que el motor
 * termine de procesar el lote actual). Se puede sobreescribir con --method.
 */
public class SubscriptionStop implements Command {

    private static final Logger log = LoggerFactory.getLogger(SubscriptionStop.class);

    @Override
    public CommandResult execute(IIDRConnection connection, String[] args) throws Exception {

        Options opts = new Options();
        opts.addRequiredOption("s", "source", true, "Nombre del datastore origen");
        opts.addRequiredOption("n", "name",   true, "Nombre de la suscripción");
        opts.addOption("m", "method", true, "Método de parada: normal (def), immediate, abort");
        CommandLine cli = new DefaultParser().parse(opts, args);
        String source = cli.getOptionValue("source");
        String name   = cli.getOptionValue("name");
        String method = cli.getOptionValue("method", "normal");

        log.info("Parando suscripción '{}' en datastore '{}' (método: {})", name, source, method);

        SubscriptionHelper.connectDatastore(connection, source);
        try {
            String state = SubscriptionHelper.getState(connection, name);
            log.info("Estado actual de '{}': '{}'", name, state);

            if (!SubscriptionHelper.isRunning(state)) {
                log.info("Suscripción '{}' ya está parada ({}), nada que hacer", name, state);
                return CommandResult.skipped("subscription.stop",
                        "La suscripción ya está parada: " + (state.isEmpty() ? "Inactive" : state));
            }

            connection.executeForMessage(
                "end replication filter subscription name \"" + name + "\" method " + method);

            String newState = waitForInactive(connection, name, 30, 1000);
            log.info("Estado tras parada: '{}'", newState);

            return CommandResult.ok("subscription.stop",
                    java.util.Map.of("name", name, "state", newState));

        } finally {
            SubscriptionHelper.disconnectDatastore(connection);
        }
    }

    private String waitForInactive(IIDRConnection connection, String name, int maxAttempts, long intervalMs)
            throws Exception {
        for (int i = 0; i < maxAttempts; i++) {
            String state = SubscriptionHelper.getState(connection, name);
            if (!SubscriptionHelper.isRunning(state)) {
                return state.isEmpty() ? "Inactive" : state;
            }
            log.debug("Esperando parada de '{}', estado: '{}' (intento {}/{})", name, state, i + 1, maxAttempts);
            Thread.sleep(intervalMs);
        }
        return SubscriptionHelper.getState(connection, name);
    }
}
