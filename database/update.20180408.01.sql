ALTER TABLE `market_index`.`market_ticks` 
ADD COLUMN `period` VARCHAR(45) NULL AFTER `symbol`;
