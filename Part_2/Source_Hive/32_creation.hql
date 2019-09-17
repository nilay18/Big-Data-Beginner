create table raw_32gb_temp(
line string
);
LOAD DATA INPATH 'data_32GB.txt' OVERWRITE INTO TABLE raw_32gb_temp;

CREATE TABLE final_word_count_32_first AS
SELECT word, count(1) AS count FROM
(SELECT explode(split(line, '\\s')) AS word FROM raw_32gb_temp ) w
where length(w.word) >3
GROUP BY w.word
ORDER BY w.word;

select * from final_word_count_32GB_first
order by count desc
limit 100;
