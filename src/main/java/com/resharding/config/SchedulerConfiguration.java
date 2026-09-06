package com.resharding.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

/**
 * Разделяет координационные циклы по независимым scheduler threads.
 *
 * <p>Worker tick синхронно выполняет длинную Task. На стандартном единственном
 * Spring scheduler thread он блокировал Leader tick и закрытие DB pools после
 * STOP. Раздельные scheduler гарантируют, что lifecycle и координация продолжают
 * работать независимо от длительности JDBC batch/DDL.
 */
@Configuration
public class SchedulerConfiguration {

    @Bean
    public ThreadPoolTaskScheduler workerTaskScheduler() {
        return scheduler("reshard-worker-");
    }

    @Bean
    public ThreadPoolTaskScheduler leaderTaskScheduler() {
        return scheduler("reshard-leader-");
    }

    @Bean
    public ThreadPoolTaskScheduler lifecycleTaskScheduler() {
        return scheduler("reshard-lifecycle-");
    }

    private ThreadPoolTaskScheduler scheduler(String prefix) {
        ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(1);
        scheduler.setThreadNamePrefix(prefix);
        scheduler.setWaitForTasksToCompleteOnShutdown(true);
        scheduler.setAwaitTerminationSeconds(30);
        return scheduler;
    }
}
