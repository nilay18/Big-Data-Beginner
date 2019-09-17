create external table temp_1gb_raw (
  line string
);
LOAD DATA INPATH 'data_32GB.txt' OVERWRITE INTO TABLE temp_1gb_raw;

create external table dead123 (
  line string
);
LOAD DATA INPATH 'data_64GB.txt' OVERWRITE INTO TABLE dead123;
