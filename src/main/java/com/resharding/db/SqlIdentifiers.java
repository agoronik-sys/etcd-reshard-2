package com.resharding.db;

import java.util.Arrays;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Единственная точка валидации SQL identifiers.
 *
 * <p>Значения данных всегда передаются bind-параметрами, но JDBC не позволяет
 * bind'ить имя таблицы/колонки. Поэтому конфигурационные identifiers сначала
 * ограничиваются безопасным набором символов, затем кавычатся для PostgreSQL.
 */
public final class SqlIdentifiers {

    private static final Pattern SAFE = Pattern.compile("[A-Za-z_][A-Za-z0-9_$]*");

    private SqlIdentifiers() {
    }

    public static String quote(String identifier) {
        if (identifier == null || !SAFE.matcher(identifier).matches()) {
            throw new IllegalArgumentException("Unsafe SQL identifier: " + identifier);
        }
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }

    public static String quoteQualified(String identifier) {
        if (identifier == null || identifier.isBlank()) {
            throw new IllegalArgumentException("SQL identifier must not be blank");
        }
        return Arrays.stream(identifier.split("\\.", -1))
                .map(SqlIdentifiers::quote)
                .collect(Collectors.joining("."));
    }
}
