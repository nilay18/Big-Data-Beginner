set hive.exec.parallel=true;

CREATE TABLE final_word_count_64_first_parallel AS
SELECT word, count(1) AS count FROM
(SELECT explode(split(line, '\\s')) AS word FROM dead123 ) w
where length(w.word) >3
GROUP BY w.word
ORDER BY w.word;

select * from final_word_count_64_first_parallel
order by count desc
limit 100;
