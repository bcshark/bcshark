CREATE DATABASE IF NOT EXISTS market_index DEFAULT CHARSET utf8 COLLATE utf8_general_ci; 

CREATE TABLE `market_ticks` (
	  `id` int(11) NOT NULL AUTO_INCREMENT,
	  `time` bigint(18) DEFAULT NULL,
	  `open` decimal(18,9) DEFAULT NULL,
	  `close` decimal(18,9) DEFAULT NULL,
	  `high` decimal(18,9) DEFAULT NULL,
	  `low` decimal(18,9) DEFAULT NULL,
	  `amount` decimal(18,9) DEFAULT NULL,
	  `volume` decimal(18,9) DEFAULT NULL,
	  `count` int(11) DEFAULT NULL,
	  `market` varchar(45) DEFAULT NULL,
	  `symbol` varchar(45) DEFAULT NULL,
	  `period` varchar(45) DEFAULT NULL,
	  PRIMARY KEY (`id`),
	  UNIQUE KEY `id_UNIQUE` (`id`),
	  KEY `IDX_TIMESTAMP` (`time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

