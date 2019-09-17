CREATE TABLE final_word_count_32_first_display AS
SELECT word, count(1) AS count FROM
(SELECT explode(split(line, '\\s')) AS word FROM raw_32gb_temp ) w
GROUP BY w.word
ORDER BY w.word;

select * from final_word_count_32_first_display
where length(word) >3
order by count desc
limit 100;
