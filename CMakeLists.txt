idf_component_register(SRCS "esteira_industrial.c"
                    INCLUDE_DIRS "."
                    PRIV_REQUIRES esp_timer esp_driver_gpio esp_driver_touch_sens spi_flash esp_wifi esp_event esp_netif nvs_flash lwip)