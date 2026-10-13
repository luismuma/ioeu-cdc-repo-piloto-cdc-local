package com.mapfre.cdc.admin;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datamirror.common.util.EncryptedDataException;
import com.datamirror.common.util.Encryptor;
import com.ibm.replication.cdc.scripting.EmbeddedScript;
import com.ibm.replication.cdc.scripting.EmbeddedScriptException;
import com.ibm.replication.cdc.scripting.ResultStringKeyValues;
import com.ibm.replication.cdc.scripting.ResultStringTable;

/**
 * Wrapper sobre EmbeddedScript de IBM IIDR.
 *
 * Gestiona el ciclo de vida de la conexión al Access Server:
 * abre el script, autentica, ejecuta comandos y cierra limpiamente.
 * Implementa AutoCloseable para poder usarse en try-with-resources.
 */
public class IIDRConnection implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(IIDRConnection.class);

    private final String host;
    private final int    port;
    private final String user;
    private final String password;
    private final String registryFile;

    private EmbeddedScript script;

    public IIDRConnection(String propertiesFile) throws Exception {
        Properties props = new Properties();
        try (FileInputStream fis = new FileInputStream(propertiesFile)) {
            props.load(fis);
        }
        this.host         = props.getProperty("asHostName");
        this.port         = Integer.parseInt(props.getProperty("asPort", "10101"));
        this.user         = props.getProperty("asUserName");
        this.password     = resolvePassword(props, propertiesFile);
        this.registryFile = props.getProperty("registryFile", "conf/table_subscription_registry.csv");
    }

    public String getRegistryFile() { return registryFile; }

    /**
     * Resuelve la contraseña del properties.
     * Si está en claro, la encripta con el Encryptor de IIDR y sobreescribe el fichero.
     * Si ya está encriptada, la desencripta y la devuelve en claro.
     */
    private static String resolvePassword(Properties props, String propertiesFile) throws Exception {
        String raw = props.getProperty("asPassword");
        try {
            return Encryptor.decodeAndDecrypt(raw);
        } catch (EncryptedDataException e) {
            log.info("Contraseña en claro detectada, encriptando y guardando en {}", propertiesFile);
            String encrypted = Encryptor.encryptAndEncode(raw);
            props.setProperty("asPassword", encrypted);
            try (FileOutputStream fos = new FileOutputStream(propertiesFile)) {
                props.store(fos, "IIDR Access Server connection");
            }
            return raw;
        }
    }

    /**
     * Abre el EmbeddedScript y autentica contra el Access Server.
     */
    public void connect() throws Exception {
        log.info("Conectando a Access Server {}:{}", host, port);
        script = new EmbeddedScript();
        script.open();
        script.execute(
            "connect server hostname " + host +
            " port " + port +
            " username " + quoteStringLiteral(user) +
            " password " + quoteStringLiteral(password)
        );
        log.info("Conectado correctamente");
    }

    /**
     * Devuelve {@code value} como literal CHCCLP entrecomillado, apto para
     * concatenar en un comando que se pase a {@link #execute}.
     *
     * Regla de entrecomillado en CHCCLP (confirmada por el propio mensaje
     * de ayuda del lexer, 2026-06-02):
     * <pre>
     *   Use double quotes around parameter values that contain special
     *   characters or syntax keywords, or around empty strings. If the
     *   parameter value contains double quotes, use single quotes around
     *   the value.
     * </pre>
     *
     * Es decir, CHCCLP **no escapa comillas internas** — hay que elegir
     * el delimitador según el contenido:
     * <ul>
     *   <li>Sin comilla doble → entrecomillar con {@code "..."}.</li>
     *   <li>Con comilla doble pero sin comilla simple → entrecomillar
     *       con {@code '...'}.</li>
     *   <li>Con ambas → no hay forma de representarlo en CHCCLP.
     *       Lanzamos {@link IllegalArgumentException}: el caller debe
     *       cambiar la pwd / valor (caso extremo, raro en credenciales
     *       generadas por política).</li>
     * </ul>
     *
     * El bug que motivó esta función: pwd de {@code tsuser} para PRE
     * con {@code ~"…~|} dentro. La concatenación cruda terminaba el
     * string en la {@code "} interna y disparaba rc=-2010 "Lexical
     * error". Con esta función la pwd se delimita con {@code '...'}
     * y CHCCLP la consume bien.
     *
     * Devuelve el string ya entrecomillado (incluye los delimitadores
     * de apertura y cierre) — los call-sites NO deben volver a añadirlos.
     */
    public static String quoteStringLiteral(String value) {
        if (value == null) return "\"\"";
        boolean hasDouble = value.indexOf('"')  >= 0;
        boolean hasSingle = value.indexOf('\'') >= 0;
        if (hasDouble && hasSingle) {
            throw new IllegalArgumentException(
                "El valor contiene comilla doble y comilla simple a la vez; " +
                "CHCCLP no soporta escapar ninguna. Cambia el valor.");
        }
        if (hasDouble) {
            return "'" + value + "'";
        }
        return "\"" + value + "\"";
    }

    /**
     * Ejecuta un comando CHCCLP y devuelve el resultado como tabla.
     * Devuelve null si el resultado no es una tabla (ej: confirmaciones).
     */
    public ResultStringTable execute(String command) throws EmbeddedScriptException {
        log.debug("Ejecutando: {}", command);
        try {
            script.execute(command);
        } catch (EmbeddedScriptException e) {
            throw describeError(e);
        }
        Object result = script.getResult();
        return (result instanceof ResultStringTable) ? (ResultStringTable) result : null;
    }

    /**
     * Ejecuta un comando CHCCLP y devuelve el resultado como mapa clave-valor.
     * Devuelve null si el resultado no es un ResultStringKeyValues (ej: show subscription).
     */
    public ResultStringKeyValues executeForKeyValues(String command) throws EmbeddedScriptException {
        log.debug("Ejecutando: {}", command);
        try {
            script.execute(command);
        } catch (EmbeddedScriptException e) {
            throw describeError(e);
        }
        Object result = script.getResult();
        return (result instanceof ResultStringKeyValues) ? (ResultStringKeyValues) result : null;
    }

    /**
     * Versión que devuelve el mensaje de resultado en texto plano.
     * Útil para comandos cuyo resultado no es tabular.
     */
    public String executeForMessage(String command) throws EmbeddedScriptException {
        log.debug("Ejecutando: {}", command);
        try {
            script.execute(command);
        } catch (EmbeddedScriptException e) {
            int rc = script.getResultCode();
            String msg = script.getResultMessage();
            if (isInformationalRc(rc)) {
                log.debug("CHCCLP rc={} (informativo): {}", rc, msg);
                return msg != null ? msg : "";
            }
            log.error("CHCCLP error (rc={}): {}", rc, msg);
            throw describeError(e);
        }
        String msg = script.getResultMessage();
        log.debug("CHCCLP rc={}: {}", script.getResultCode(), msg);
        return msg != null ? msg : "";
    }

    /**
     * IIDR usa códigos rc > 0 también para confirmaciones informativas, no solo errores.
     * Estos rc son respuestas normales que no indican fallo.
     */
    private static boolean isInformationalRc(int rc) {
        return rc == 1312   // Mirroring initiated
            || rc == 1314   // End replication initiated
            || rc == 1322;  // The action completed successfully
    }

    /**
     * La EmbeddedScriptException de CHCCLP suele traer getMessage()==null: el
     * rc y el texto reales viven en script.getResultCode()/getResultMessage().
     * Si se propaga cruda, el JSON de salida acaba con "message": null y el
     * operador no sabe qué pasó. Esto la envuelve con "rc=<n>: <texto>" para
     * que el mensaje sea legible aguas arriba (IIDRAdmin → CommandResult).
     * Añade una pista accionable para rc conocidos (p.ej. -2404: tabla no
     * encontrada → típicamente el motor origen DB2 no está disponible).
     */
    private EmbeddedScriptException describeError(EmbeddedScriptException original) {
        int rc = script.getResultCode();
        String msg = script.getResultMessage();
        StringBuilder sb = new StringBuilder("CHCCLP rc=").append(rc);
        if (msg != null && !msg.isEmpty()) {
            sb.append(": ").append(msg);
        }
        if (rc == -2404) {
            sb.append(" (la tabla origen no se encuentra: revisa que exista en el ")
              .append("datastore origen y que su motor DB2 esté arrancado/accesible)");
        }
        // EmbeddedScriptException solo expone el constructor (int, String); la
        // causa original se encadena con initCause (heredado de Throwable) para
        // no perder el stack trace de CHCCLP.
        EmbeddedScriptException enriched = new EmbeddedScriptException(rc, sb.toString());
        enriched.initCause(original);
        return enriched;
    }

    public int getLastResultCode() {
        return script.getResultCode();
    }

    /**
     * Desconecta limpiamente del Access Server y cierra el EmbeddedScript.
     */
    @Override
    public void close() {
        if (script == null) return;
        try {
            script.execute("disconnect server");
        } catch (Exception e) {
            log.debug("Aviso al desconectar del servidor (se puede ignorar): {}", e.getMessage());
        }
        try {
            script.close();
        } catch (Exception e) {
            log.debug("Aviso al cerrar EmbeddedScript: {}", e.getMessage());
        }
        log.info("Conexión cerrada");
    }
}
