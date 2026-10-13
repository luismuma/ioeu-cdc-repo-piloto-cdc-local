package com.mapfre.cdc.admin.commands;

/**
 * Resultado de la ejecución de un comando.
 *
 * Status:
 *   OK      → operación ejecutada con éxito          → exit 0
 *   SKIPPED → ya estaba en el estado deseado          → exit 0
 *   ERROR   → fallo real                              → exit 1
 *
 * El campo data se serializa como payload JSON en el stdout
 * (que es lo que Ansible lee). Los logs van a stderr.
 */
public class CommandResult {

    public enum Status { OK, SKIPPED, ERROR }

    private final Status status;
    private final String operation;
    private final Object data;
    private final String message;

    private CommandResult(Status status, String operation, Object data, String message) {
        this.status    = status;
        this.operation = operation;
        this.data      = data;
        this.message   = message;
    }

    // --- Factories ---

    public static CommandResult ok(String operation, Object data) {
        return new CommandResult(Status.OK, operation, data, null);
    }

    public static CommandResult skipped(String operation, String reason) {
        return new CommandResult(Status.SKIPPED, operation, null, reason);
    }

    public static CommandResult error(String operation, String message) {
        return new CommandResult(Status.ERROR, operation, null, message);
    }

    public static CommandResult error(String operation, String message, int errorCode) {
        return new CommandResult(Status.ERROR, operation,
                java.util.Map.of("errorCode", errorCode), message);
    }

    // --- Getters (necesarios para Jackson) ---

    public Status getStatus()    { return status; }
    public String getOperation() { return operation; }
    public Object getData()      { return data; }
    public String getMessage()   { return message; }

    /** Exit code para System.exit(): 0 si OK o SKIPPED, 1 si ERROR. */
    public int getExitCode() {
        return status == Status.ERROR ? 1 : 0;
    }
}
