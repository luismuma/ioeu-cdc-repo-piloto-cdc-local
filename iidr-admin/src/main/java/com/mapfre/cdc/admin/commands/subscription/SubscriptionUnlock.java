package com.mapfre.cdc.admin.commands.subscription;

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
 * Desbloquea una suscripción tras edición.
 * Idempotente: si ya está desbloqueada devuelve SKIPPED.
 *
 * Uso: iidr.sh subscription unlock --source <ds> --target <ds> --name <suscripcion>
 */
public class SubscriptionUnlock implements Command {

    private static final Logger log = LoggerFactory.getLogger(SubscriptionUnlock.class);

    @Override
    public CommandResult execute(IIDRConnection connection, String[] args) throws Exception {

        Options opts = new Options();
        opts.addRequiredOption("s", "source", true, "Datastore origen");
        opts.addRequiredOption("t", "target", true, "Datastore destino");
        opts.addRequiredOption("n", "name",   true, "Nombre de la suscripción");
        CommandLine cli = new DefaultParser().parse(opts, args);
        String source = cli.getOptionValue("source");
        String target = cli.getOptionValue("target");
        String name   = cli.getOptionValue("name");

        log.info("Desbloqueando suscripción '{}'", name);

        SubscriptionHelper.connectDatastore(connection, source);
        SubscriptionHelper.connectTargetDatastore(connection, target);
        try {
            Map<String, String> props = SubscriptionHelper.showSubscription(connection, name);
            String lockedBy = props.getOrDefault("Locked By User", "").trim();

            if (lockedBy.isEmpty()) {
                log.info("Suscripción '{}' ya está desbloqueada, nada que hacer", name);
                return CommandResult.skipped("subscription.unlock",
                        "La suscripción no está bloqueada");
            }

            connection.executeForMessage("unlock subscription name \"" + name + "\"");
            log.info("Suscripción '{}' desbloqueada", name);
            return CommandResult.ok("subscription.unlock", Map.of("name", name, "locked", false));

        } finally {
            SubscriptionHelper.disconnectBothDatastores(connection);
        }
    }
}
