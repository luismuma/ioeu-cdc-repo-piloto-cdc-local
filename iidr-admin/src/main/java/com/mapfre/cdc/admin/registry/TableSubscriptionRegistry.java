package com.mapfre.cdc.admin.registry;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Registro local tabla → suscripción.
 *
 * Mantiene un fichero CSV simple con la relación entre tablas origen
 * y las suscripciones en las que están mapeadas. Sirve como caché
 * de consulta rápida y como doble verificación frente al estado real
 * del Access Server.
 *
 * Formato del fichero:
 *   schema.tabla , nombre_suscripcion , datastore_origen
 *
 * Ejemplo:
 *   MAPFRE.CLIENTES,SUB_DB2_KAFKA_CLIENTES,IIDR_GR_DB2_E01_AWS_DEV
 *   MAPFRE.PEDIDOS,SUB_DB2_KAFKA_PEDIDOS,IIDR_GR_DB2_E01_AWS_DEV
 *
 * Todas las operaciones son sincronizadas para evitar corrupción
 * en caso de ejecuciones concurrentes.
 */
public class TableSubscriptionRegistry {

    private static final Logger log = LoggerFactory.getLogger(TableSubscriptionRegistry.class);

    private static final String[] HEADERS = { "table", "subscription", "datastore" };

    private static final CSVFormat FORMAT = CSVFormat.DEFAULT
            .builder()
            .setHeader(HEADERS)
            .setSkipHeaderRecord(true)
            .setCommentMarker('#')
            .setTrim(true)
            .get();

    public record Entry(String table, String subscription, String datastore) {}

    private final Path registryFile;

    public TableSubscriptionRegistry(String filePath) throws IOException {
        this.registryFile = Paths.get(filePath);
        if (!Files.exists(registryFile)) {
            log.info("Registro no encontrado, creando: {}", registryFile);
            Files.createDirectories(registryFile.getParent());
            try (CSVPrinter printer = new CSVPrinter(new FileWriter(registryFile.toFile()), FORMAT)) {
                // Escribe solo la cabecera
            }
        }
    }

    /**
     * Añade una entrada al registro.
     * Si ya existe exactamente la misma combinación (tabla + suscripción),
     * no hace nada (idempotente).
     */
    public synchronized void add(String table, String subscription, String datastore)
            throws IOException {
        String tableUpper = table.toUpperCase();
        List<Entry> entries = readAll();
        boolean exists = entries.stream().anyMatch(e ->
                e.table().equalsIgnoreCase(tableUpper) &&
                e.subscription().equalsIgnoreCase(subscription));
        if (exists) {
            log.debug("Entrada ya existe en registro: {} → {}", tableUpper, subscription);
            return;
        }
        try (CSVPrinter printer = new CSVPrinter(
                new FileWriter(registryFile.toFile(), true), CSVFormat.DEFAULT)) {
            printer.printRecord(tableUpper, subscription, datastore);
        }
        log.info("Registro: añadida {} → {} ({})", tableUpper, subscription, datastore);
    }

    /**
     * Elimina todas las entradas de la combinación tabla + suscripción.
     * Si no existe, no hace nada (idempotente).
     */
    public synchronized void remove(String table, String subscription) throws IOException {
        List<Entry> entries = readAll();
        List<Entry> filtered = entries.stream()
                .filter(e -> !(e.table().equalsIgnoreCase(table) &&
                               e.subscription().equalsIgnoreCase(subscription)))
                .collect(Collectors.toList());

        if (filtered.size() == entries.size()) {
            log.debug("Entrada no encontrada en registro para eliminar: {} → {}", table, subscription);
            return;
        }
        writeAll(filtered);
        log.info("Registro: eliminada {} → {}", table.toUpperCase(), subscription);
    }

    /**
     * Devuelve todas las suscripciones en las que está mapeada una tabla.
     * La búsqueda es case-insensitive.
     */
    public synchronized List<Entry> findByTable(String table) throws IOException {
        return readAll().stream()
                .filter(e -> e.table().equalsIgnoreCase(table))
                .collect(Collectors.toList());
    }

    /**
     * Sustituye todas las entradas de una suscripción por la lista proporcionada.
     * Las tablas que ya no aparezcan en IIDR se eliminan; las nuevas se añaden.
     * Esto permite mantener el registro sincronizado con el estado real de IIDR
     * aunque los cambios se hayan hecho desde la Management Console.
     */
    public synchronized void syncSubscription(String subscription, String datastore,
                                               List<String> tables) throws IOException {
        List<Entry> all = readAll();
        // Eliminar entradas previas de esta suscripción
        List<Entry> kept = all.stream()
                .filter(e -> !e.subscription().equalsIgnoreCase(subscription))
                .collect(Collectors.toList());
        // Añadir las entradas actuales
        for (String t : tables) {
            kept.add(new Entry(t.toUpperCase(), subscription, datastore));
        }
        writeAll(kept);
        log.info("Registro: sincronizados {} mappings para suscripción '{}'",
                tables.size(), subscription);
    }

    /**
     * Devuelve todas las entradas del registro.
     */
    public synchronized List<Entry> findAll() throws IOException {
        return readAll();
    }

    // --- Internals ---

    private List<Entry> readAll() throws IOException {
        List<Entry> entries = new ArrayList<>();
        try (FileReader reader = new FileReader(registryFile.toFile())) {
            for (CSVRecord record : FORMAT.parse(reader)) {
                entries.add(new Entry(
                        record.get("table"),
                        record.get("subscription"),
                        record.get("datastore")));
            }
        }
        return entries;
    }

    private void writeAll(List<Entry> entries) throws IOException {
        try (CSVPrinter printer = new CSVPrinter(
                new FileWriter(registryFile.toFile()), FORMAT)) {
            for (Entry e : entries) {
                printer.printRecord(e.table(), e.subscription(), e.datastore());
            }
        }
    }
}
