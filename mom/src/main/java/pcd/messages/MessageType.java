package pcd.messages;

/**
 * Enum per i tipi di messaggio nel middleware di lock distribuito.
 */
public enum MessageType {
    ACQUIRE_REQUEST,   // Un processo richiede il lock
    RELEASE_REQUEST,   // Un processo rilascia il lock
    GRANT_MESSAGE,     // Il server concede il lock
    REJECT_MESSAGE,    // Il server nega il lock (in coda)
    ALREADY_HELD       // Il process ha già quel lock
}
