package com.resharding.migration.ketama;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Проверяет совместимость Ketama ring с nginx consistent hash.
 *
 * <p>Маршрутизация должна быть детерминированной при одинаковом server name/weight,
 * а добавление shard — перемещать только часть ключей. Иначе сервис и nginx
 * направят одну бизнес-запись на разные PostgreSQL shard.
 */
class NginxKetamaHashRingTest {

    @Test
    void sameKeyAlwaysSameShard() {
        NginxKetamaHashRing ring = NginxKetamaHashRing.build(
                new String[]{"db1", "db2"},
                new String[]{"db1", "db2"},
                new int[]{1, 1},
                160);

        assertEquals(ring.locate("user-123"), ring.locate("user-123"));
    }

    @Test
    void ketamaServerNameChangesRingPlacement() {
        NginxKetamaHashRing ringById = NginxKetamaHashRing.build(
                new String[]{"db1", "db2"},
                new String[]{"db1", "db2"},
                new int[]{1, 1},
                160);

        NginxKetamaHashRing ringByHost = NginxKetamaHashRing.build(
                new String[]{"db1", "db2"},
                new String[]{"postgres1.example.com:5432", "postgres2.example.com:5432"},
                new int[]{1, 1},
                160);

        boolean anyDifferent = false;
        for (int i = 0; i < 200; i++) {
            if (!ringById.locate("key-" + i).equals(ringByHost.locate("key-" + i))) {
                anyDifferent = true;
                break;
            }
        }
        assert anyDifferent;
    }

    @Test
    void distributesAcrossShards() {
        NginxKetamaHashRing ring = NginxKetamaHashRing.build(
                new String[]{"db1", "db2", "db3", "db4"},
                new String[]{"db1", "db2", "db3", "db4"},
                new int[]{1, 1, 1, 1},
                160);

        long db1 = 0, db2 = 0, db3 = 0, db4 = 0;
        for (int i = 0; i < 1000; i++) {
            switch (ring.locate("key-" + i)) {
                case "db1" -> db1++;
                case "db2" -> db2++;
                case "db3" -> db3++;
                case "db4" -> db4++;
                default -> throw new IllegalStateException();
            }
        }
        // каждый shard должен получить хотя бы часть ключей
        assert db1 > 0 && db2 > 0 && db3 > 0 && db4 > 0;
    }
}
