package com.mapfre.cdc.admin;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mapfre.cdc.admin.commands.Command;
import com.mapfre.cdc.admin.commands.CommandResult;
import com.mapfre.cdc.admin.commands.datastore.DatastoreCreate;
import com.mapfre.cdc.admin.commands.datastore.DatastoreDelete;
import com.mapfre.cdc.admin.commands.datastore.DatastoreGrant;
import com.mapfre.cdc.admin.commands.datastore.DatastoreList;
import com.mapfre.cdc.admin.commands.datastore.DatastoreShow;
import com.mapfre.cdc.admin.commands.subscription.SubscriptionCreate;
import com.mapfre.cdc.admin.commands.subscription.SubscriptionDelete;
import com.mapfre.cdc.admin.commands.subscription.SubscriptionList;
import com.mapfre.cdc.admin.commands.subscription.SubscriptionLock;
import com.mapfre.cdc.admin.commands.subscription.SubscriptionReinit;
import com.mapfre.cdc.admin.commands.subscription.SubscriptionSetupKafka;
import com.mapfre.cdc.admin.commands.subscription.SubscriptionShow;
import com.mapfre.cdc.admin.commands.subscription.SubscriptionStart;
import com.mapfre.cdc.admin.commands.subscription.SubscriptionStop;
import com.mapfre.cdc.admin.commands.subscription.SubscriptionUnlock;
import com.mapfre.cdc.admin.commands.mapping.MappingAdd;
import com.mapfre.cdc.admin.commands.mapping.MappingFindByTable;
import com.mapfre.cdc.admin.commands.mapping.MappingList;
import com.mapfre.cdc.admin.commands.mapping.MappingRefresh;
import com.mapfre.cdc.admin.commands.mapping.MappingRemove;
import com.mapfre.cdc.admin.commands.composite.CompositeHealthCheck;
import com.mapfre.cdc.admin.commands.composite.CompositeRestart;
import com.mapfre.cdc.admin.commands.composite.CompositeSetup;
import com.mapfre.cdc.admin.commands.composite.CompositeTeardown;

/**
 * Punto de entrada del CLI iidr-admin.
 *
 * Uso:  iidr.sh <entidad> <operación> [opciones]
 *
 * Ejemplos:
 *   iidr.sh datastore list
 *   iidr.sh datastore create --name MYDS --host db2host --port 11701 --type DB2
 *   iidr.sh subscription start --name MYSUB --source DB2DS
 *   iidr.sh mapping find-by-table --table SCHEMA.CLIENTES
 *   iidr.sh composite setup --config conf/setup.json
 */
public class IIDRAdmin {

    private static final Logger log = LoggerFactory.getLogger(IIDRAdmin.class);

    /** Registro de comandos disponibles: "entidad.operacion" → clase Command */
    private static final Map<String, Class<? extends Command>> COMMANDS = new LinkedHashMap<>();

    static {
        // datastore
        COMMANDS.put("datastore.list",   DatastoreList.class);
        COMMANDS.put("datastore.show",   DatastoreShow.class);
        COMMANDS.put("datastore.create", DatastoreCreate.class);
        COMMANDS.put("datastore.delete", DatastoreDelete.class);
        COMMANDS.put("datastore.grant",  DatastoreGrant.class);

        // subscription
        COMMANDS.put("subscription.list",   SubscriptionList.class);
        COMMANDS.put("subscription.show",   SubscriptionShow.class);
        COMMANDS.put("subscription.create", SubscriptionCreate.class);
        COMMANDS.put("subscription.delete", SubscriptionDelete.class);
        COMMANDS.put("subscription.start",  SubscriptionStart.class);
        COMMANDS.put("subscription.stop",   SubscriptionStop.class);
        COMMANDS.put("subscription.lock",   SubscriptionLock.class);
        COMMANDS.put("subscription.unlock", SubscriptionUnlock.class);
        COMMANDS.put("subscription.reinit", SubscriptionReinit.class);
        COMMANDS.put("subscription.setup-kafka", SubscriptionSetupKafka.class);

        // mapping
        COMMANDS.put("mapping.add",            MappingAdd.class);
        COMMANDS.put("mapping.remove",         MappingRemove.class);
        COMMANDS.put("mapping.list",           MappingList.class);
        COMMANDS.put("mapping.find-by-table",  MappingFindByTable.class);
        COMMANDS.put("mapping.refresh",        MappingRefresh.class);

        // composite
        COMMANDS.put("composite.setup",        CompositeSetup.class);
        COMMANDS.put("composite.teardown",     CompositeTeardown.class);
        COMMANDS.put("composite.restart",      CompositeRestart.class);
        COMMANDS.put("composite.health-check", CompositeHealthCheck.class);
    }

    public static void main(String[] args) {

        if (args.length < 2) {
            printUsage();
            System.exit(1);
        }

        String entity    = args[0].toLowerCase();
        String operation = args[1].toLowerCase();
        String key       = entity + "." + operation;
        String[] cmdArgs = Arrays.copyOfRange(args, 2, args.length);

        Class<? extends Command> cmdClass = COMMANDS.get(key);
        if (cmdClass == null) {
            System.err.println("Comando desconocido: " + key);
            printUsage();
            System.exit(1);
        }

        String propertiesFile = System.getProperty("iidr.properties", "conf/iidr.properties");

        CommandResult result;
        try (IIDRConnection connection = new IIDRConnection(propertiesFile)) {
            connection.connect();
            Command cmd = cmdClass.getDeclaredConstructor().newInstance();
            result = cmd.execute(connection, cmdArgs);
        } catch (Exception e) {
            String errMsg = describeThrowable(e);
            log.error("Error ejecutando {}: {}", key, errMsg, e);
            result = CommandResult.error(key, errMsg);
        }

        OutputFormatter.print(result);
        System.exit(result.getExitCode());
    }

    /**
     * Extrae un mensaje legible de una excepción para el JSON de error.
     * EmbeddedScriptException de CHCCLP tiene getMessage()==null (el texto real
     * vive en getResultCodeAndMessage()); si se usara getMessage() directamente,
     * el JSON saldría con "message": null y el operador no sabría qué pasó.
     * Para el resto de excepciones se usa getMessage(), con fallback al nombre de
     * la clase si también es null.
     */
    private static String describeThrowable(Throwable e) {
        if (e instanceof com.ibm.replication.cdc.scripting.EmbeddedScriptException) {
            com.ibm.replication.cdc.scripting.EmbeddedScriptException ese =
                (com.ibm.replication.cdc.scripting.EmbeddedScriptException) e;
            String rcm = ese.getResultCodeAndMessage();
            if (rcm != null && !rcm.isEmpty()) {
                return rcm;
            }
        }
        String msg = e.getMessage();
        return (msg != null && !msg.isEmpty()) ? msg : e.getClass().getName();
    }

    private static void printUsage() {
        System.err.println("Uso: iidr.sh <entidad> <operación> [opciones]");
        System.err.println();
        System.err.println("Entidades y operaciones disponibles:");
        COMMANDS.keySet().forEach(k -> System.err.println("  " + k.replace(".", " ")));
        System.err.println();
        System.err.println("Ejemplos:");
        System.err.println("  iidr.sh datastore list");
        System.err.println("  iidr.sh subscription start --name MYSUB --source DB2DS");
        System.err.println("  iidr.sh mapping find-by-table --table SCHEMA.CLIENTES");
        System.err.println("  iidr.sh composite setup --config conf/setup.json");
    }
}
