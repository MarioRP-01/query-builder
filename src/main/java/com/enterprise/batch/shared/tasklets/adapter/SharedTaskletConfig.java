package com.enterprise.batch.shared.tasklets.adapter;

import com.enterprise.batch.shared.tasklets.port.BusinessDateResolver;
import com.enterprise.batch.shared.tasklets.port.HolidayCalendar;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Auto-configuration for shared tasklets.
 */
@Configuration
public class SharedTaskletConfig {

    @Bean
    public HolidayCalendar holidayCalendar() {
        return date -> false;
    }

    @Bean
    public BusinessDateResolver businessDateResolver(HolidayCalendar calendar) {
        return new DefaultBusinessDateResolver(calendar);
    }

    @Bean
    public BusinessDateTasklet businessDateTasklet(BusinessDateResolver resolver) {
        return new BusinessDateTasklet(resolver);
    }
}
