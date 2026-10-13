package com.mapfre.cdc.admin.commands;

import com.mapfre.cdc.admin.IIDRConnection;

/**
 * Contrato que debe cumplir cualquier comando del CLI.
 * Cada implementación recibe la conexión activa al Access Server
 * y los argumentos restantes de la línea de comandos.
 */
public interface Command {

    /**
     * Ejecuta el comando.
     *
     * @param connection Conexión activa y autenticada al Access Server.
     * @param args       Argumentos específicos del comando (todo lo que viene
     *                   después de "entidad operación" en la línea de comandos).
     * @return Resultado con status OK, SKIPPED o ERROR y payload JSON.
     */
    CommandResult execute(IIDRConnection connection, String[] args) throws Exception;
}
