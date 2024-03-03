CREATE DEFINER=`root`@`localhost` PROCEDURE `get_top_n_HeadshotCount`()
BEGIN
select dev_id , headshots_count, difficulty, row_number() over(partition by dev_id order by headshots_count desc) from level_details2;
END