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
 * Reinicializa una suscripción (refresh completo de datos desde el source).
 * Equivale a "start refresh" en CHCCLP.
 *
 * Uso: iidr.sh subscription reinit --source <ds> --name <suscripcion>
 */
public class SubscriptionReinit implements Command {

    private static final Logger log = LoggerFactory.getLogger(SubscriptionReinit.class);

    @Override
    public CommandResult execute(IIDRConnection connection, String[] args) throws Exception {

        Options opts = new Options();
        opts.addRequiredOption("s", "source", true, "Datastore origen");
        opts.addRequiredOption("n", "name",   true, "Nombre de la suscripción");
        CommandLine cli = new DefaultParser().parse(opts, args);
        String source = cli.getOptionValue("source");
        String name   = cli.getOptionValue("name");

        log.info("Reinicializando suscripción '{}' (start refresh)", name);

        SubscriptionHelper.connectDatastore(connection, source);
        try {
            connection.executeForMessage(
                "start refresh filter subscription name \"" + name + "\"");

            String newState = SubscriptionHelper.getState(connection, name);
            log.info("Estado tras reinicialización: '{}'", newState);

            return CommandResult.ok("subscription.reinit",
                    Map.of("name", name, "state", newState));

        } finally {
            SubscriptionHelper.disconnectDatastore(connection);
        }
    }
}
