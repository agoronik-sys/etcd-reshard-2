/**
 * Слой доступа к {@code etcd}: ключи, транзакции, репозитории состояния.
 *
 * <p>В {@code etcd} хранится только координационное состояние (Leader, members,
 * migration, checkpoint, active tasks, locks). Бизнес-данные и миллионы диапазонов
 * в {@code etcd} не размещаются.
 */
package com.resharding.etcd;
