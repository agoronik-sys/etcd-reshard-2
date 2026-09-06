/**
 * Кластерная координация: Leader Election и регистрация Pod.
 *
 * <p>Количество Worker определяется динамически из активных members в {@code etcd},
 * а не захардкожено в коде.
 */
package com.resharding.cluster;
