/**
 * Доменные модели и перечисления сервиса решардирования.
 *
 * <p>Содержит состояние migration, task, checkpoint и метаданные кластера.
 * Все модели сериализуются в JSON и хранятся в {@code etcd}; бизнес-данные БД
 * в этих классах не представлены.
 *
 * @see com.resharding.etcd
 */
package com.resharding.domain;
