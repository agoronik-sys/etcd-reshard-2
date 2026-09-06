package com.resharding.migration.ketama;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.TreeMap;
import java.util.zip.CRC32;

/**
 * Consistent hash ring совместимый с nginx upstream {@code hash ... consistent}.
 *
 * <p>Алгоритм (см. {@code ngx_http_upstream_init_chash}):
 * <ul>
 *   <li>CRC32 base hash от {@code server + '\0' + port};</li>
 *   <li>{@code weight * pointsPerServer} точек на server;</li>
 *   <li>каждая следующая точка: CRC32(base + prev_hash_bytes);</li>
 *   <li>lookup: CRC32(key), первый point {@code >= hash} по кольцу.</li>
 * </ul>
 */
public final class NginxKetamaHashRing {

    private final TreeMap<Long, String> circle;

    private NginxKetamaHashRing(TreeMap<Long, String> circle) {
        this.circle = circle;
    }

    /**
     * @param nodes            идентификаторы shard (db1, db2...) — значение, возвращаемое при lookup
     * @param ketamaServerNames строка server для ring (как в nginx upstream), parallel to nodes
     * @param weights          вес server (parallel to nodes)
     * @param pointsPerServer  точек на единицу weight (nginx default 160)
     */
    public static NginxKetamaHashRing build(
            String[] nodes,
            String[] ketamaServerNames,
            int[] weights,
            int pointsPerServer) {

        if (nodes.length != ketamaServerNames.length || nodes.length != weights.length) {
            throw new IllegalArgumentException("nodes, ketamaServerNames and weights must have same length");
        }

        TreeMap<Long, String> circle = new TreeMap<>();
        for (int i = 0; i < nodes.length; i++) {
            addNode(circle, nodes[i], ketamaServerNames[i], weights[i], pointsPerServer);
        }
        if (circle.isEmpty()) {
            throw new IllegalArgumentException("Ketama ring cannot be empty");
        }
        return new NginxKetamaHashRing(circle);
    }

    /**
     * Определяет target shard по значению shard key.
     *
     * @param key значение поля shard key записи
     * @return id shard (db1, db2, ...)
     */
    public String locate(Object key) {
        if (circle.isEmpty()) {
            return null;
        }
        long hash = crc32(String.valueOf(key));
        Map.Entry<Long, String> entry = circle.ceilingEntry(hash);
        if (entry == null) {
            entry = circle.firstEntry();
        }
        return entry.getValue();
    }

    public int size() {
        return circle.size();
    }

    private static void addNode(
            TreeMap<Long, String> circle,
            String nodeId,
            String ketamaServer,
            int weight,
            int pointsPerServer) {

        if (weight <= 0) {
            return;
        }

        String server;
        String port;
        int colon = ketamaServer.lastIndexOf(':');
        if (colon > 0 && ketamaServer.substring(colon + 1).chars().allMatch(Character::isDigit)) {
            server = ketamaServer.substring(0, colon);
            port = ketamaServer.substring(colon + 1);
        } else {
            server = ketamaServer;
            port = "";
        }

        byte[] prevHash = new byte[4];
        int npoints = weight * pointsPerServer;

        for (int i = 0; i < npoints; i++) {
            CRC32 hash = baseHash(server, port);
            hash.update(prevHash);
            long value = hash.getValue();
            circle.put(value, nodeId);
            prevHash = prevHashBytes(value);
        }
    }

    private static CRC32 baseHash(String server, String port) {
        CRC32 crc = new CRC32();
        crc.update(server.getBytes(StandardCharsets.US_ASCII));
        crc.update((byte) 0);
        crc.update(port.getBytes(StandardCharsets.US_ASCII));
        return crc;
    }

    /** nginx little-endian: prev_hash = 4 bytes of CRC32 value */
    private static byte[] prevHashBytes(long value) {
        return new byte[]{
                (byte) (value & 0xff),
                (byte) ((value >> 8) & 0xff),
                (byte) ((value >> 16) & 0xff),
                (byte) ((value >> 24) & 0xff)
        };
    }

    private static long crc32(String key) {
        CRC32 crc = new CRC32();
        crc.update(key.getBytes(StandardCharsets.UTF_8));
        return crc.getValue();
    }
}
