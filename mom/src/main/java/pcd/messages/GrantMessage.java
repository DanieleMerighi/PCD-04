package pcd.messages;

import java.io.Serializable;

/**
 * Messaggio di concessione del lock.
 * Il middleware invia questo messaggio a un processo per notificare che ha acquisito il lock sulla risorsa.
 */
public class GrantMessage implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String resourceId;
    private final String processId;
    private final boolean granted;
    private final String message;

    public GrantMessage(String resourceId, String processId, boolean granted, String message) {
        this.resourceId = resourceId;
        this.processId = processId;
        this.granted = granted;
        this.message = message;
    }

    public String getResourceId() {
        return resourceId;
    }

    public String getProcessId() {
        return processId;
    }

    public boolean isGranted() {
        return granted;
    }

    public String getMessage() {
        return message;
    }

    @Override
    public String toString() {
        return "GrantMessage{" +
                "resourceId='" + resourceId + '\'' +
                ", processId='" + processId + '\'' +
                ", granted=" + granted +
                ", message='" + message + '\'' +
                '}';
    }
}
