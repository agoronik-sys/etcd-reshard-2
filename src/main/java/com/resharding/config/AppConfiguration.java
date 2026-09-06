package com.resharding.config;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties({
        ReshardingRootProperties.class,
        SegmentsProperties.class,
        MigrationProperties.class,
        EtcdProperties.class,
        InstanceProperties.class
})
public class AppConfiguration {
}
