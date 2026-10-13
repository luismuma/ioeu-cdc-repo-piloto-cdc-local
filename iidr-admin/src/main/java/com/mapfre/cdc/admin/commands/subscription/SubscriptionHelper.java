package com.mapfre.cdc.admin.commands.subscription;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.replication.cdc.scripting.ResultStringKeyValues;
import com.ibm.replication.cdc.scripting.ResultStringTable;
import com.mapfre.cdc.admin.IIDRConnection;

/**
 * Utilidades compartidas para los comandos de suscripción.
 *
 * El flujo SYSADMIN en CHCCLP exige:
 *   connect datastore name "<source>";
 *   ... operaciones ...
 *   disconnect datastore;
 *
 * Este helper gestiona ese ciclo y proporciona métodos de parseo comunes.
 */
public class SubscriptionHelper {

    private static final Logger log = LoggerFactory.getLogger(SubscriptionHelper.class);

    /** Conecta al datastore como contexto source (operaciones de suscripción). */
    public static void connectDatastore(IIDRConnection connection, String datastore) throws Exception {
        log.debug("Conectando a datastore '{}' (source)", datastore);
        connection.execute("connect datastore name \"" + datastore + "\" context source");
    }

    /** Conecta también el datastore target (necesario para lock/unlock y mappings). */
    public static void connectTargetDatastore(IIDRConnection connection, String datastore) throws Exception {
        log.debug("Conectando a datastore '{}' (target)", datastore);
        connection.execute("connect datastore name \"" + datastore + "\" context target");
    }

    /** Desconecta ambos datastores (source y target). Ignora errores. */
    public static void disconnectBothDatastores(IIDRConnection connection) {
        for (String ctx : new String[]{"source", "target"}) {
            try {
                connection.execute("disconnect datastore context " + ctx);
            } catch (Exception e) {
                log.debug("Aviso al desconectar datastore {} (ignorado): {}", ctx, e.getMessage());
            }
        }
    }

    /** Desconecta del datastore activo (ignorando errores si ya no había contexto). */
    public static void disconnectDatastore(IIDRConnection connection) {
        try {
            connection.execute("disconnect datastore context source");
        } catch (Exception e) {
            log.debug("Aviso al desconectar datastore (ignorado): {}", e.getMessage());
        }
    }

    /**
     * Ejecuta "list subscriptions" y devuelve una lista de mapas con:
     * name, sourceDatastore, targetDatastore.
     */
    public static List<Map<String, String>> listSubscriptions(IIDRConnection connection) throws Exception {
        ResultStringTable table = connection.execute("list subscriptions");
        List<Map<String, String>> result = new ArrayList<>();
        if (table == null) return result;
        for (int i = 0; i < table.getRowCount(); i++) {
            Map<String, String> sub = new LinkedHashMap<>();
            sub.put("name",            safeGet(table, i, "SUBSCRIPTION"));
            sub.put("sourceDatastore", safeGet(table, i, "SOURCE DATASTORE"));
            sub.put("targetDatastore", safeGet(table, i, "TARGET DATASTORE"));
            result.add(sub);
        }
        return result;
    }

    /**
     * Ejecuta "monitor replication" y devuelve un mapa nombre→{state, latency}.
     */
    public static Map<String, Map<String, String>> monitorReplication(IIDRConnection connection) throws Exception {
        ResultStringTable table = connection.execute("monitor replication");
        Map<String, Map<String, String>> byName = new LinkedHashMap<>();
        if (table == null) return byName;
        for (int i = 0; i < table.getRowCount(); i++) {
            String name = safeGet(table, i, "SUBSCRIPTION");
            Map<String, String> m = new LinkedHashMap<>();
            m.put("state",   safeGet(table, i, "STATE"));
            m.put("latency", safeGet(table, i, "LATENCY"));
            byName.put(name, m);
        }
        return byName;
    }

    /**
     * Ejecuta "show subscription name <name>" y devuelve un mapa con todas
     * las propiedades de la suscripción.
     * El resultado es ResultStringKeyValues (no una tabla columnar).
     */
    public static Map<String, String> showSubscription(IIDRConnection connection, String name) throws Exception {
        ResultStringKeyValues kv = connection.executeForKeyValues("show subscription name \"" + name + "\"");
        Map<String, String> props = new LinkedHashMap<>();
        if (kv == null) return props;
        for (String key : kv.getKeys()) {
            props.put(key, kv.getValue(key));
        }
        return props;
    }

    /**
     * Devuelve el estado actual de una suscripción ("Mirror Continuous", "Inactive", etc.)
     * o cadena vacía si no se encuentra.
     */
    public static String getState(IIDRConnection connection, String subscriptionName) throws Exception {
        Map<String, Map<String, String>> monitor = monitorReplication(connection);
        Map<String, String> entry = monitor.get(subscriptionName);
        return entry != null ? entry.getOrDefault("state", "") : "";
    }

    /** Devuelve true si el estado indica que la replicación está activa. */
    public static boolean isRunning(String state) {
        return state.startsWith("Mirror") || state.startsWith("Refresh");
    }

    public static String safeGet(ResultStringTable table, int row, String column) {
        try {
            return table.getValueAt(row, column);
        } catch (Exception e) {
            return "";
        }
    }
}
