with m as (select date_part('hour', date) as hour from messages where chat_id={chat_id} and sender_id={sender_id}), c as (select hour, count(hour) as count from m group by hour) select ((hour+3)::decimal)%24 as hour,count::decimal/sum(count) over () as normalized_cnt from c order by hour;