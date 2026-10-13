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
 * Bloquea una suscripción para edición exclusiva.
 * Idempotente: si ya está bloqueada por el mismo usuario devuelve SKIPPED.
 *
 * Uso: iidr.sh subscription lock --source <ds> --target <ds> --name <suscripcion>
 */
public class SubscriptionLock implements Command {

    private static final Logger log = LoggerFactory.getLogger(SubscriptionLock.class);

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

        log.info("Bloqueando suscripción '{}'", name);

        SubscriptionHelper.connectDatastore(connection, source);
        SubscriptionHelper.connectTargetDatastore(connection, target);
        try {
            Map<String, String> props = SubscriptionHelper.showSubscription(connection, name);
            String lockedBy = props.getOrDefault("Locked By User", "").trim();

            if (!lockedBy.isEmpty()) {
                log.info("Suscripción '{}' ya está bloqueada por '{}', nada que hacer", name, lockedBy);
                return CommandResult.skipped("subscription.lock",
                        "La suscripción ya está bloqueada por: " + lockedBy);
            }

            connection.executeForMessage("lock subscription name \"" + name + "\"");
            log.info("Suscripción '{}' bloqueada", name);
            return CommandResult.ok("subscription.lock", Map.of("name", name, "locked", true));

        } finally {
            SubscriptionHelper.disconnectBothDatastores(connection);
        }
    }
}
