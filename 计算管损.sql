-- ��ȡ���µ�һ��
drop function if exists func_getSeasonFirstDay;
delimiter $$
create function func_getSeasonFirstDay()
RETURNS bigint
begin    
     declare curYear int;
		 declare curMon int;
		 declare curDay int;
		 declare startYear int;
		 set curYear=year(now());
		 set curMon=month(now());
		 set curDay=day(now());
		 if (curMon>10 or (curMon=10 and curDay>=15)) then
		     set startYear=curYear;
		 else
		     set startYear=curYear-1;
		 end if;
		 return UNIX_TIMESTAMP(concat(startYear,'-',10,'-',15,' 00:00:00'))*1000;
end$$;
delimiter ;
-- select FROM_UNIXTIME(func_getSeasonFirstDay()/1000)

-- ��ȡ���µ�һ��
drop function if exists func_getMonthFirstDay;
delimiter $$
create function func_getMonthFirstDay()
RETURNS bigint
begin    
     return UNIX_TIMESTAMP(concat(year(now()),'-',month(now()),'-',1,' 00:00:00'))*1000;
end$$;
delimiter ;
-- select FROM_UNIXTIME(func_getMonthFirstDay()/1000)

-- ��ȡָ��ƫ�����һ��ʱ���
drop function if exists func_getDayFirstTimePoint;
delimiter $$
create function func_getDayFirstTimePoint(curDate DATE,off int)
RETURNS bigint
begin
    declare needDay date;
		set needDay=timestampadd(day,off,curDate);
    return UNIX_TIMESTAMP(concat(year(needDay),'-',month(needDay),'-',day(needDay),' 00:00:00'))*1000;
end$$;
delimiter ;
-- select FROM_UNIXTIME(func_getDayFirstTimePoint(CURRENT_DATE(),0)/1000)

-- ��ȡָ��ƫ�������һ��ʱ���
drop function if exists func_getDayLastTimePoint;
delimiter $$
create function func_getDayLastTimePoint(curDate DATE,off int)
RETURNS bigint
begin
    declare needDay date;
		set needDay=timestampadd(day,off,curDate);
    return UNIX_TIMESTAMP(concat(year(needDay),'-',month(needDay),'-',day(needDay),' 23:59:59'))*1000;
end$$;
delimiter ;
-- select FROM_UNIXTIME(func_getDayLastTimePoint(CURRENT_DATE(),-1)/1000)


-- [proc ]ָ�����һ����ֵ
DROP PROCEDURE IF EXISTS proc_getDayFirstPointValue;
delimiter $$
create PROCEDURE proc_getDayFirstPointValue(IN dayStart bigint,IN plcId int,IN pointId int,OUT retVal double)
begin
 DECLARE CONTINUE HANDLER FOR SQLEXCEPTION
 set @fval=0;
 set @sql_select=concat('select EVERY_CLOCK_VALUE into @fval from histdb.rtdata_day_',plcId,' where POINT_ID=',pointId,' and EVERY_CLOCK_TIME>=',dayStart,'  order by EVERY_CLOCK_TIME asc limit 1');
 PREPARE stmt FROM @sql_select;
 EXECUTE stmt;
 deallocate prepare stmt;
 set retVal=ifnull(@fval,0);
end$$
delimiter ;

-- set @ret=0.0;
-- call proc_getDayFirstPointValue(func_getDayFirstTimePoint(CURRENT_DATE(),-1),51,18361864,@ret);
-- select @ret;
-- 

-- [func ]ָ�����һ����ֵ
-- DROP FUNCTION IF EXISTS func_getDayFirstPointValue;
-- delimiter $$
-- create FUNCTION func_getDayFirstPointValue(dayStart bigint,plcId int,pointId int)
-- returns double
-- begin
--  declare ret double;
--  call proc_getDayFirstPointValue(dayStart,plcId,pointId,ret);
--  if ret is null then
--      set ret=0;
--  end if;
--  return ret;
-- end$$
-- delimiter ;
-- 
-- select func_getDayFirstPointValue(func_getDayFirstTimePoint(CURRENT_DATE(),-1),5,18361864)


-- �������µĵ�ֵ
DROP PROCEDURE IF EXISTS proc_getTodayLastPointValue;
delimiter $$
create PROCEDURE proc_getTodayLastPointValue(IN plcId int,IN pointId int,OUT retVal double)
begin
 DECLARE CONTINUE HANDLER FOR SQLEXCEPTION
 set @fval=0;
 set @sql_select=concat('select FINAL_VAL into @fval from histdb.rtdata_',plcId,' where POINT_ID=',pointId,' order by updatetime desc limit 1');
 PREPARE stmt FROM @sql_select;
 EXECUTE stmt;
 deallocate prepare stmt;
 set retVal=ifnull(@fval,0);
end$$
delimiter ;

-- set @ret=0.0;
-- call proc_getTodayLastPointValue(15,18361864,@ret);
-- select @ret;
-- 11316493



-- ��ȡ���µ�һ����ֵ
DROP PROCEDURE IF EXISTS proc_getMonthFirstPointValue;
delimiter $$
create PROCEDURE proc_getMonthFirstPointValue(IN plcId int,IN pointId int,OUT retVal double)
begin
 DECLARE CONTINUE HANDLER FOR SQLEXCEPTION
 set @fval=0;
 set @sql_select=concat('select EVERY_CLOCK_VALUE into @fval from histdb.rtdata_day_',plcId,' where POINT_ID=',pointId,' and EVERY_CLOCK_TIME>=',func_getMonthFirstDay(),'  order by EVERY_CLOCK_TIME asc limit 1');
 PREPARE stmt FROM @sql_select;
 EXECUTE stmt;
 deallocate prepare stmt;
 set retVal=ifnull(@fval,0);
end$$
delimiter ;

-- set @ret=0.0;
-- call proc_getMonthFirstPointValue(5,18361864,@ret);
-- select @ret;
-- 9542272


-- ͳ������ ���� ����������
DROP PROCEDURE IF EXISTS proc_getAllGuanSunPercent;
delimiter $$
create PROCEDURE proc_getAllGuanSunPercent()
begin
    declare plcId int;
		declare pointId int;
		declare isTotal int;
		declare curDate DATE;
		declare dayGS double default 0;
		declare monthGS double default 0;
		declare seasonGS double default 0;
		declare ret1 double default 0;
		declare ret2 double default 0;
		
		declare total_diff_day double default 0;-- ���ۼƲ�
		declare total_diff_mon double default 0;-- ���ۼƲ�
		declare total_diff_sea double default 0;-- �¼��Ʋ�

		declare sum_start_day double default 0;
		declare sum_end_day double default 0;
		
		declare sum_start_mon double default 0;
		declare sum_end_mon double default 0;
		
		declare sum_start_sea double default 0;
		declare sum_end_sea double default 0;
 
		declare flag int default 0;
    declare cur CURSOR for select b.PLC_ID,b.POINT_ID,a.is_total from temporary_tag_name_guansun a inner join datapointconfig b on a.tag_name=b.tag_name;
		declare continue handler for not found set flag = 1;
		set curDate=CURRENT_DATE();
		
		open cur;
		lp:loop
		
		    fetch cur into plcId,pointId,isTotal;
				if flag=1 then
				    leave lp;
				end if;
				-- ------------------------------------------------����-------------------------------------------------
				set ret1=0;
				set ret2=0;
				-- ���յ�һ��
				call proc_getDayFirstPointValue(func_getDayFirstTimePoint(curDate,-1),plcId,pointId,ret1);
				-- �������һ��(Ҳ���ǽ��յ�һ��)
		    call proc_getDayFirstPointValue(func_getDayFirstTimePoint(curDate,0),plcId,pointId,ret2);
				-- �����ۼƲ�
				if isTotal=1 then
				    set total_diff_day=ret2-ret1;
				else
				   -- ���յ�һ�����
					 set sum_start_day=sum_start_day + ret1;
					 -- �������һ�����
					 set sum_end_day=sum_end_day + ret2;
				end if;
			
				-- -------------------------------------------------����---------------------------------------------------
				set ret1=0;
				set ret2=0;
				-- ��������ֵ
        call proc_getTodayLastPointValue(plcId,pointId,ret1);
				-- ���µ�һ��
				call proc_getMonthFirstPointValue(plcId,pointId,ret2);
				if isTotal=1 then
				    -- �����ۼƲ�
				    set total_diff_mon=ret1-ret2;
				else
				    -- ��������ֵ���
				    set sum_end_mon=sum_end_mon + ret1;
					  -- ���µ�һ�����
					  set sum_start_mon=sum_start_mon + ret2;
				end if;
				
				-- -------------------------------------------------����---------------------------------------------------
				set ret1=0;
				set ret2=0;
				-- ��������ֵ
        call proc_getTodayLastPointValue(plcId,pointId,ret1);
				-- ������һ��
				call proc_getDayFirstPointValue(func_getSeasonFirstDay(),plcId,pointId,ret2);
				if isTotal=1 then
				    -- �����ۼƲ�
				    set total_diff_sea=ret1-ret2;
				else
				    -- ��������ֵ���
				    set sum_end_sea=sum_end_sea + ret1;
					  -- ������һ�����
					  set sum_start_sea=sum_start_sea + ret2;
				end if;
		
		end loop;
		close cur;
		-- �������չ�����
		if total_diff_day=0 then
			   set dayGS=0;	
		else 
				 set dayGS=((total_diff_day-(sum_end_day-sum_start_day))/total_diff_day)*100;
		end if;
		-- ���㵱�¹�����
		if total_diff_mon=0 then
			   set monthGS=0;	
		else 
				 set monthGS=((total_diff_mon-(sum_end_mon-sum_start_mon))/total_diff_mon)*100;
		end if;
		-- ���㵱��������
		if total_diff_sea=0 then
			   set seasonGS=0;	
		else 
				 set seasonGS=((total_diff_sea-(sum_end_sea-sum_start_sea))/total_diff_sea)*100;
		end if;
		select dayGS,monthGS,seasonGS;
end$$
delimiter ;

call proc_getAllGuanSunPercent();