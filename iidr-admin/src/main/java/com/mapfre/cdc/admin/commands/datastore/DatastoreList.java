package com.mapfre.cdc.admin.commands.datastore;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.replication.cdc.scripting.ResultStringTable;
import com.mapfre.cdc.admin.IIDRConnection;
import com.mapfre.cdc.admin.commands.Command;
import com.mapfre.cdc.admin.commands.CommandResult;

/**
 * Lista todos los datastores registrados en el Access Server.
 *
 * No requiere opciones adicionales.
 *
 * Salida JSON de ejemplo:
 * {
 *   "status": "OK",
 *   "operation": "datastore.list",
 *   "data": [
 *     { "name": "IIDR_GR_DB2_E01_AWS_DEV", "host": "db2host", "port": "11701",
 *       "type": "DB2", "status": "Available" },
 *     { "name": "IIDR_GR_KAFKA_E01_AWS_DEV", ... }
 *   ]
 * }
 *
 * Este comando sirve como implementación de referencia para los demás.
 */
public class DatastoreList implements Command {

    private static final Logger log = LoggerFactory.getLogger(DatastoreList.class);

    @Override
    public CommandResult execute(IIDRConnection connection, String[] args) throws Exception {

        log.info("Listando datastores del Access Server");

        ResultStringTable table = connection.execute("list datastores");

        if (table == null || table.getRowCount() == 0) {
            log.info("No se encontraron datastores");
            return CommandResult.ok("datastore.list", List.of());
        }

        List<Map<String, String>> datastores = new ArrayList<>();
        for (int i = 0; i < table.getRowCount(); i++) {
            datastores.add(parseRow(table, i));
        }

        log.info("Datastores encontrados: {}", datastores.size());
        return CommandResult.ok("datastore.list", datastores);
    }

    /** Construye el mapa de un datastore a partir de una fila de la tabla CHCCLP. */
    static Map<String, String> parseRow(ResultStringTable table, int row) {
        String hostname = safeGet(table, row, "HOSTNAME");
        String host = hostname;
        String port = "";
        int atIdx = hostname.lastIndexOf('@');
        if (atIdx >= 0) {
            host = hostname.substring(0, atIdx);
            port = hostname.substring(atIdx + 1);
        }
        Map<String, String> ds = new LinkedHashMap<>();
        ds.put("name",     safeGet(table, row, "DATASTORE"));
        ds.put("host",     host);
        ds.put("port",     port);
        ds.put("version",  safeGet(table, row, "SHORT VERSION"));
        ds.put("database", safeGet(table, row, "DATABASE"));
        ds.put("type",     safeGet(table, row, "TYPE"));
        ds.put("status",   safeGet(table, row, "STATUS"));
        return ds;
    }

    /** Extrae un valor de la tabla de forma segura, devolviendo "" si la columna no existe. */
    static String safeGet(ResultStringTable table, int row, String column) {
        try {
            return table.getValueAt(row, column);
        } catch (Exception e) {
            return "";
        }
    }
}
