package net.wushilin.doris;

public enum ValidationMode {
    /** No client-side validation. */
    NONE,
    /** Validate only that JSON records are well-formed (no-op for CSV). */
    SYNTAX,
    /** Validate JSON records are well-formed and contain all declared columns. */
    STRICT
}
