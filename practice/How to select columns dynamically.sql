SET @@group_concat_max_len = 5000000;

SET @sql = NULL;
SELECT
  GROUP_CONCAT(concat('`', column_name, '`')
  ) INTO @sql

  FROM 
    INFORMATION_SCHEMA.COLUMNS 
  WHERE 
    TABLE_NAME='triangle' 
AND
ordinal_position > 2
  ORDER BY 
    ORDINAL_POSITION  
  ;

SET @sql = CONCAT('SELECT ', @sql, ' FROM ticketmaster.triangle');

PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;



