package com.mapfre.cdc.admin.commands.datastore;

import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datamirror.common.util.Encryptor;
import com.ibm.replication.cdc.scripting.ResultStringTable;
import com.mapfre.cdc.admin.IIDRConnection;
import com.mapfre.cdc.admin.commands.Command;
import com.mapfre.cdc.admin.commands.CommandResult;

/**
 * Concede "access connection" sobre un datastore a un usuario del Access Server.
 *
 * Caso de uso típico (provisioning Mapfre): APP-cdcadmin crea el datastore con
 * datastore create; inmediatamente después, datastore grant --user APP-cdcauto
 * para que el usuario operacional pueda gestionar subscriptions sobre él.
 *
 * Uso: iidr.sh datastore grant --name <datastore> --user <username>
 *
 * Credenciales del motor IIDR (env vars OBLIGATORIAS, ya cifradas con
 * IBM Encryptor):
 *   DB_USER_ENC      Usuario técnico del motor IIDR (p.ej. APPCDCL).
 *                    Es el dbUser con el que el AS se autenticará al
 *                    motor en nombre del <username>. El comando CHCCLP
 *                    de IIDR exige dbUser (rc -2004 si falta).
 *   DB_PASSWORD_ENC  Password de DB_USER_ENC.
 *
 * Origen: ambos llegan ya cifrados desde iidr-install (que los ha
 * usado al instalar el motor del datastore). Cifrado con
 * bin/iidr-encrypt.sh (IBM Encryptor de IIDR). Se descifran aquí justo
 * antes de pasarlos al comando CHCCLP.
 *
 * Idempotente: si el usuario ya tiene access connection al datastore,
 * devuelve SKIPPED sin necesidad de las credenciales del motor.
 *
 * Pensado para ejecutarse autenticado como APP-cdcadmin (operación de
 * provisioning), igual que datastore create. Para ello, el invocador
 * exporta IIDR_PROPERTIES_FILE apuntando a un .properties con APP-cdcadmin.
 */
public class DatastoreGrant implements Command {

    private static final Logger log = LoggerFactory.getLogger(DatastoreGrant.class);

    @Override
    public CommandResult execute(IIDRConnection connection, String[] args) throws Exception {

        Options opts = new Options();
        opts.addRequiredOption("n", "name", true, "Nombre del datastore");
        opts.addRequiredOption("u", "user", true, "Usuario al que conceder access connection");
        CommandLine cli = new DefaultParser().parse(opts, args);
        String name = cli.getOptionValue("name");
        String user = cli.getOptionValue("user");

        log.info("Concediendo access connection a usuario '{}' sobre datastore '{}'", user, name);

        // 1. Validar que el datastore existe (con "list access datastores" —
        //    "list datastores" devuelve [] para usuarios sin access connection
        //    a ese ds, lo que falsearía la comprobación).
        if (!datastoreExists(connection, name)) {
            return CommandResult.error("datastore.grant",
                    "El datastore no existe: " + name);
        }

        // 2. Idempotencia: ¿el usuario ya tiene access connection a este datastore?
        //    Nota: NO validamos previamente que el usuario exista. El comando
        //    CHCCLP "list users" da EmbeddedScriptException en este entorno;
        //    si <user> no existe, el "add access connection" final fallará con
        //    un mensaje semántico claro de CHCCLP que se propaga como ERROR.
        if (hasAccessConnection(connection, user, name)) {
            log.info("El usuario '{}' ya tiene access connection a '{}', nada que hacer", user, name);
            return CommandResult.skipped("datastore.grant",
                    "El usuario " + user + " ya tiene access connection al datastore " + name);
        }

        // 3. Resolver credenciales del motor IIDR (dbUser + dbPassword).
        //    Llegan cifradas con IBM Encryptor desde el vault/AAP y se
        //    descifran aquí justo antes de pasarlas a CHCCLP.
        String dbUserEnc     = System.getenv("DB_USER_ENC");
        String dbPasswordEnc = System.getenv("DB_PASSWORD_ENC");
        if (dbUserEnc == null || dbUserEnc.isEmpty()
                || dbPasswordEnc == null || dbPasswordEnc.isEmpty()) {
            return CommandResult.error("datastore.grant",
                    "Faltan las env vars DB_USER_ENC y/o DB_PASSWORD_ENC " +
                    "(credenciales cifradas del motor IIDR — usuario dbUser y password).");
        }
        String dbUser     = Encryptor.decodeAndDecrypt(dbUserEnc);
        String dbPassword = Encryptor.decodeAndDecrypt(dbPasswordEnc);

        // 4. Conceder el access connection. username y dbPassword se delimitan con
        //    IIDRConnection.quoteStringLiteral: CHCCLP no escapa comillas
        //    internas, así que el helper elige " o ' según contenido.
        //    Visto 2026-06-02: pwd con ~"…~| dentro disparaba rc=-2010
        //    "Lexical error" porque la " interna cerraba el string.
        //    username se entrecomilla desde la migración a usuarios LDAP: los
        //    nombres llevan prefijo "APP-" (p.ej. APP-cdcauto) y el guion podría
        //    interpretarse como carácter especial por el lexer CHCCLP.
        //    dbUser sí va crudo: es el usuario técnico del motor DB2 (ej. APPCDCL).
        String cmd = "add access connection username " + IIDRConnection.quoteStringLiteral(user) +
                     " datastore \"" + name + "\"" +
                     " dbUser " + dbUser +
                     " dbPassword " + IIDRConnection.quoteStringLiteral(dbPassword);
        connection.executeForMessage(cmd);

        log.info("Access connection concedido: usuario '{}' → datastore '{}' (dbUser={})",
                user, name, dbUser);
        return CommandResult.ok("datastore.grant",
                Map.of("datastore", name, "user", user, "dbUser", dbUser));
    }

    /**
     * Comprueba si un datastore con el nombre dado está registrado en el AS.
     *
     * Usamos "list access datastores" (no "list datastores") porque éste último
     * devuelve [] para usuarios sin access connection, mientras que el primero
     * devuelve los datastores registrados en el AS visibles para el usuario
     * autenticado. Como datastore grant se ejecuta autenticado como APP-cdcadmin
     * (que sí tiene access connection a los datastores que provisiona), la
     * comprobación es fiable. Mismo comando que usa DatastoreCreate para su
     * propio check de idempotencia.
     */
    private static boolean datastoreExists(IIDRConnection connection, String name) throws Exception {
        ResultStringTable table = connection.execute("list access datastores");
        if (table == null) return false;
        for (int i = 0; i < table.getRowCount(); i++) {
            if (name.equalsIgnoreCase(DatastoreList.safeGet(table, i, "DATASTORE"))) {
                return true;
            }
        }
        return false;
    }

    /**
     * Comprueba si un usuario ya tiene access connection al datastore dado.
     * Usa "list access connections username <user>" y busca el datastore
     * en las filas devueltas.
     */
    private static boolean hasAccessConnection(IIDRConnection connection,
                                               String user,
                                               String datastore) throws Exception {
        ResultStringTable table = connection.execute(
                "list access connections username " + IIDRConnection.quoteStringLiteral(user));
        if (table == null) return false;
        for (int i = 0; i < table.getRowCount(); i++) {
            if (datastore.equalsIgnoreCase(DatastoreList.safeGet(table, i, "DATASTORE"))) {
                return true;
            }
        }
        return false;
    }
}
