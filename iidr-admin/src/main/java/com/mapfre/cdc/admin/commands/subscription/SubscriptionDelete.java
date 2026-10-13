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
 * Elimina una suscripción. La para primero si está activa.
 * Idempotente: si no existe devuelve SKIPPED.
 *
 * Uso: iidr.sh subscription delete --source <ds> --target <ds> --name <nombre>
 */
public class SubscriptionDelete implements Command {

    private static final Logger log = LoggerFactory.getLogger(SubscriptionDelete.class);

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

        log.info("Eliminando suscripción '{}'", name);

        SubscriptionHelper.connectDatastore(connection, source);
        SubscriptionHelper.connectTargetDatastore(connection, target);
        try {
            // Idempotencia: comprobar si existe
            List<Map<String, String>> existing = SubscriptionHelper.listSubscriptions(connection);
            boolean exists = existing.stream()
                    .anyMatch(s -> s.get("name").equalsIgnoreCase(name));
            if (!exists) {
                log.info("Suscripción '{}' no existe, nada que hacer", name);
                return CommandResult.skipped("subscription.delete",
                        "La suscripción no existe: " + name);
            }

            // Parar replicación si está activa
            String state = SubscriptionHelper.getState(connection, name);
            if (SubscriptionHelper.isRunning(state)) {
                log.info("Suscripción '{}' activa ({}), parando antes de eliminar", name, state);
                connection.executeForMessage(
                    "end replication filter subscription name \"" + name + "\" method immediate");
            }

            // IIDR exige que la suscripción esté bloqueada antes de borrarla
            // (CHCCLP rc=-2317 "The subscription must be locked before the
            // command can execute"). Bloqueamos justo antes del delete.
            log.info("Bloqueando suscripción '{}' antes de eliminar", name);
            connection.executeForMessage("lock subscription name \"" + name + "\"");

            connection.executeForMessage("delete subscription name \"" + name + "\"");

            log.info("Suscripción '{}' eliminada", name);
            return CommandResult.ok("subscription.delete", Map.of("name", name));

        } finally {
            SubscriptionHelper.disconnectBothDatastores(connection);
        }
    }
}
