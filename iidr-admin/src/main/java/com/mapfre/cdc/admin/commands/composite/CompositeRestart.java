package com.mapfre.cdc.admin.commands.composite;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mapfre.cdc.admin.IIDRConnection;
import com.mapfre.cdc.admin.commands.Command;
import com.mapfre.cdc.admin.commands.CommandResult;
import com.mapfre.cdc.admin.commands.subscription.SubscriptionHelper;

/**
 * Reinicia una suscripción: stop → espera a Inactive → start → espera a Mirror Continuous.
 * Útil para recuperación de errores y para aplicar cambios de configuración.
 *
 * Uso: iidr.sh composite restart
 *        --subscription <nombre> --source <ds>
 *        [--timeout <segundos>]  (defecto: 60)
 */
public class CompositeRestart implements Command {

    private static final Logger log = LoggerFactory.getLogger(CompositeRestart.class);

    private static final long POLL_INTERVAL_MS = 3_000;

    @Override
    public CommandResult execute(IIDRConnection connection, String[] args) throws Exception {

        Options opts = new Options();
        opts.addRequiredOption("n", "subscription", true, "Nombre de la suscripción");
        opts.addRequiredOption("s", "source",       true, "Datastore origen");
        opts.addOption(        "to", "timeout",     true, "Segundos de espera máximo (defecto: 60)");
        CommandLine cli = new DefaultParser().parse(opts, args);
        String subName    = cli.getOptionValue("subscription");
        String sourceName = cli.getOptionValue("source");
        long timeoutSec   = Long.parseLong(cli.getOptionValue("timeout", "60"));
        long timeoutMs    = timeoutSec * 1_000;

        SubscriptionHelper.connectDatastore(connection, sourceName);
        try {
            // -- Fase 1: stop --------------------------------------------------
            String stateBefore = SubscriptionHelper.getState(connection, subName);
            log.info("Estado inicial de '{}': {}", subName, stateBefore);

            if (SubscriptionHelper.isRunning(stateBefore)) {
                log.info("Parando suscripción '{}'...", subName);
                connection.executeForMessage(
                        "end replication filter subscription name \"" + subName
                        + "\" method immediate");

                // Esperar hasta que deje de estar en ejecución
                String stateAfterStop = waitForState(connection, subName,
                        false, timeoutMs, "stop");
                if (SubscriptionHelper.isRunning(stateAfterStop)) {
                    return CommandResult.error("composite.restart",
                            "Timeout esperando que la suscripción se detenga (estado: "
                            + stateAfterStop + ")");
                }
                log.info("Suscripción '{}' detenida (estado: {})", subName, stateAfterStop);
            } else {
                log.info("Suscripción '{}' ya estaba parada, procediendo directamente al start", subName);
            }

            // -- Fase 2: start -------------------------------------------------
            log.info("Iniciando suscripción '{}'...", subName);
            connection.executeForMessage(
                    "start mirroring filter subscription name \"" + subName + "\"");

            // Esperar hasta Mirror Continuous (o cualquier estado activo)
            String stateAfterStart = waitForState(connection, subName,
                    true, timeoutMs, "start");
            if (!SubscriptionHelper.isRunning(stateAfterStart)) {
                return CommandResult.error("composite.restart",
                        "Timeout esperando que la suscripción arranque (estado: "
                        + stateAfterStart + ")");
            }

            log.info("Suscripción '{}' reiniciada (estado: {})", subName, stateAfterStart);

            Map<String, Object> data = new LinkedHashMap<>();
            data.put("subscription",  subName);
            data.put("stateBefore",   stateBefore);
            data.put("stateAfter",    stateAfterStart);
            return CommandResult.ok("composite.restart", data);

        } finally {
            SubscriptionHelper.disconnectDatastore(connection);
        }
    }

    /**
     * Hace polling del estado de la suscripción hasta que se cumple la condición deseada
     * o se agota el timeout.
     *
     * @param wantRunning  true → esperar a que isRunning(); false → esperar a que !isRunning()
     */
    private String waitForState(IIDRConnection connection, String subName,
                                 boolean wantRunning, long timeoutMs,
                                 String phase) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        String state = "";
        while (System.currentTimeMillis() < deadline) {
            state = SubscriptionHelper.getState(connection, subName);
            boolean running = SubscriptionHelper.isRunning(state);
            log.debug("[{}] Estado de '{}': {}", phase, subName, state);
            if (wantRunning == running) return state;
            Thread.sleep(POLL_INTERVAL_MS);
        }
        return state; // timeout — devolver el último estado conocido
    }
}
