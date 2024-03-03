
-- 1. Extract `P_ID`, `Dev_ID`, `PName`, and `Difficulty_level` of all players at Level 0.

SELECT 
    p.p_id, l.dev_id, p.pname as player_name, l.difficulty
FROM
    level_details2 l
        JOIN
    player_details p ON p.p_id = l.p_id
WHERE
    l.level = 0
GROUP BY p.p_id , l.dev_id , p.pname , l.difficulty;

-- 2. Find `Level1_code`wise average `Kill_Count` where `lives_earned` is 2, and at least 3 stages are crossed.

SELECT 
    distinct L1_code, avg(l.kill_count) over(partition by L1_code)
FROM
    level_details2 l
        JOIN
    player_details p ON l.p_id = p.p_id
where l.lives_earned = 2 and l.stages_crossed >= 3;

-- 3. Find the total number of stages crossed at each difficulty level for Level 2 with players using `zm_series` devices. 
-- Arrange the result in decreasing order of the total number of stages crossed.

SELECT 
    dev_id,
    COUNT(stages_crossed) AS count_stages_crossed,
    difficulty
FROM
    level_details2
WHERE
    level = 2 AND dev_id LIKE 'zm%'
GROUP BY dev_id , difficulty
ORDER BY count_stages_crossed DESC;

-- 4. Extract `P_ID` and the total number of unique dates for those players who have played games on multiple days.

SELECT 
    p_id,
    COUNT(DISTINCT (timestamp)) AS total_num_of_unique_days
FROM
    level_details2
GROUP BY p_id
HAVING total_num_of_unique_days > 1
ORDER BY p_id;

-- 5. Find `P_ID` and levelwise sum of `kill_counts` where `kill_count` is greater than the average kill count for Medium difficulty.

select p_id, level, sum(kill_count) over(partition by level) from level_details2 where kill_count > (select avg(kill_count) from level_details2 where difficulty = 'medium');

-- 6. Find `Level` and its corresponding `Level_code`wise sum of lives earned, excluding Level0. Arrange in ascending order of level.

SELECT 
    level, SUM(lives_earned)
FROM
    level_details2
WHERE
    level != 0
GROUP BY level
ORDER BY level ASC;

-- 7. Find the top 3 scores based on each `Dev_ID` and rank them in increasing order using `Row_Number`. Display the difficulty as well.

select dev_id , score , difficulty, ranking from 
(select dev_id , score, difficulty, row_number() over( partition by dev_id order by score desc) as ranking  from level_details2) as T1 
where ranking <= 3;

-- 8. Find the `first_login` datetime for each device ID.

select dev_id , first_login from 
(select dev_id , timestamp as first_login, rank() over (partition by dev_id order by timestamp) as ranking from level_details2) as T1
where ranking = 1;

-- 9. Find the top 5 scores based on each difficulty level and rank them in increasing order using `Rank`. Display `Dev_ID` as well.

select difficulty, score , dev_id, ranking from 
(select dev_id , score, difficulty, rank() over( partition by difficulty order by score desc) as ranking  from level_details2) as T1 
where ranking <= 5;

-- 10. Find the device ID that is first logged in (based on `start_datetime`) for each player (`P_ID`). Output should contain player ID, device ID, and first login datetime.

select p_id , dev_id, first_loggedin_datetime from 
(select p_id , dev_id , timestamp as first_loggedin_datetime , rank() over(partition by P_id order by timestamp) as ranking from level_details2) as T1
where ranking = 1;

-- 11. For each player and date, determine how many `kill_counts` were played by the player so far.

-- a) Using window functions
select p_id, timestamp, sum(kill_count) over(partition by p_id order by timestamp) as count from level_details2;

-- b) Without window functions
SELECT 
    p_id, timestamp, kill_count
FROM
    (SELECT 
        P_ID,
            timestamp,
            kill_count,
            (SELECT 
                    SUM(Kill_Count)
                FROM
                    level_details2 AS t2
                WHERE
                    t2.P_ID = t1.P_ID
                        AND DATE(t2.timestamp) = DATE(t1.timestamp)) AS total_kill_counts
    FROM
        level_details2 AS t1
    ORDER BY P_ID , timestamp) AS T3;

-- 12. Find the cumulative sum of stages crossed over `start_datetime` for each `P_ID`,excluding the most recent `start_datetime`.

select p_id , timestamp, stages_crossed, sum(stages_crossed) over(partition by p_id order by timestamp desc) from
(select p_id, timestamp , stages_crossed, ranking from
(select p_id , timestamp, stages_crossed, row_number() over(partition by p_id  order by timestamp desc) as ranking
from level_details2 group by p_id , timestamp, stages_crossed) t1
group by p_id , timestamp, stages_crossed) T2
where ranking > 1;

-- 13. Extract the top 3 highest sums of scores for each `Dev_ID` and the corresponding `P_ID`.

select dev_id, p_id, sum_of_scores, ranking from
(select dev_id, p_id, sum(score) as sum_of_scores, rank() over(partition by dev_id order by sum(score) desc) as ranking 
from level_details2 group by dev_id , p_id) as T1
where ranking<=3;

-- 14. Find players who scored more than 50% of the average score, scored by the sum of scores for each `P_ID`.

select p_id, dev_id, score, new_score from
(select p_id , dev_id, score, sum(score) over(partition by p_id) as sum_of_score, 
	avg(score) over(partition by p_id), (0.5*avg(score) over(partition by p_id)) as new_score from level_details2
	group by p_id, dev_id, score) T1
where score > new_score;
									
-- 15. Create a stored procedure to find the top `n` `headshots_count` based on each `Dev_ID` and rank them in increasing order using `Row_Number`. Display the difficulty as well.
call get_top_n_HeadshotCount;