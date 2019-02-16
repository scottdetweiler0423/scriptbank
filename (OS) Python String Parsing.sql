/****** Script for SelectTopNRows command from SSMS  ******/
/*
--Simple example of charindex
select top 1000 * from TEST_VIEW;
with t as (select 'LD-23DSP-1430' as val)
select t.*,
       LEFT(val, charindex('-', val) - 1),
   SUBSTRING(val, charindex('-', val)+1, len(val) - CHARINDEX('-', reverse(val)) - charindex('-', val)),
       REVERSE(LEFT(reverse(val), charindex('-', reverse(val)) - 1))
from t;
*/

--choosing db
USE [GAAPIData] 
GO

--insert latest month's data into full table
insert into OS_python_dropoff_table
select * from OS_python_dropoff_table_Jan2019

/*
create view "ICARNCAR_python_dropoff_table" as
select * from [dbo].[ICAR_python_dropoff_table]
union all
select * from [dbo].[NCAR_python_dropoff_table]
*/


--choosing db
USE [GAAPIData] 
GO
--creating delivery flag view
ALTER VIEW "python_TEST_VIEW" AS
SELECT [account]
	  ,[dateHourMinute]
      ,CASE WHEN [eventAction] like 'VIN%' then 'Delivery'
		ELSE 'Other' END AS Delivery_Status
	  ,[eventAction]
      ,[eventCategory]
      ,[eventLabel]
      ,[mobileDeviceModel]
      ,[sessionCount]
      ,[operatingSystemVersion]
      ,[totalEvents]
      ,[uniqueEvents]
	  FROM [GAAPIData].[dbo].[OS_python_dropoff_table] 
GO

--Parsing Event Action when delivery is other
alter view "python_VIEW_1" as 
with a as (select a.eventAction as eventAction from [GAAPIData].[dbo].[python_TEST_VIEW] as a) --this table must match 'from' table
select a.*, case when a.Delivery_Status = 'Other' then
       LEFT(eventAction, charindex('|', eventAction) - 1)
	   else null end as App_Version,
	   case when a.Delivery_Status = 'Other' then
		substring(eventAction, charindex('|', eventAction)+1, len(eventAction) - CHARINDEX('|', reverse(eventAction)) - charindex('|', eventAction))
		else null end as Event_Name1,
		case when a.Delivery_Status = 'Other' then
       REVERSE(LEFT(reverse(eventAction), charindex('|', reverse(eventAction)) - 1))
	   else null end as Opp_Key
from [GAAPIData].[dbo].[python_TEST_VIEW] as a
;
GO

--parsing Event Name and Error.  Deliveries are null
alter view "python_VIEW_2" as 
with a as (select a.Event_Name1 as Event_Name from [GAAPIData].[dbo].[python_VIEW_1] as a)
select a.*, 
       LEFT(Event_Name1, charindex('|', Event_Name1) - 1) as Event_Name,
		REVERSE(LEFT(reverse(Event_Name1), charindex('|', reverse(Event_Name1)) -1)) as Error
from [GAAPIData].[dbo].[python_VIEW_1] as a
GO
--parsing Event Category into dealer ID and sales consultant
alter view "python_VIEW_3" as 
with a as (select a.eventCategory as eventCategory from [GAAPIData].[dbo].[python_VIEW_2] as a)
select a.*, case when a.Delivery_Status = 'Other'then 
       LEFT(eventCategory, abs(charindex('|', eventCategory) - 1))
	   else null end as Dealer_ID,
	   case when a.Delivery_Status = 'Other'then
		REVERSE(LEFT(reverse(eventCategory), abs(charindex('|', reverse(eventCategory)) -1))) 
		else null end as SalesConsultant
from [GAAPIData].[dbo].[python_VIEW_2] as a
GO

--parsing eventAction when Delivery Status is Delivery
alter view "python_VIEW_4" as 
with a as (select a.eventAction as eventAction from [GAAPIData].[dbo].[python_VIEW_3] as a)
select a.*, case when a.Delivery_Status = 'Delivery' then
       LEFT(eventAction, charindex('|', eventAction) - 1)
	   else null end as VIN,
	   case when a.Delivery_Status = 'Delivery' then
		substring(eventAction, charindex('|', eventAction)+1, len(eventAction) - CHARINDEX('|', reverse(eventAction)) - charindex('|', eventAction))
		else null end as MiddleStuff,
		case when a.Delivery_Status = 'Delivery' then
       REVERSE(LEFT(reverse(eventAction), charindex('|', reverse(eventAction)) - 1))
	   else null end as DeliveryConsultant
from [GAAPIData].[dbo].[python_VIEW_3] as a
GO

--parsing MiddleStuff when Delivery Status is Delivery
alter view "python_VIEW_5" as 
with a as (select a.MiddleStuff as MiddleStuff from [GAAPIData].[dbo].[python_VIEW_4] as a)
select a.*, case when a.Delivery_Status = 'Delivery' then
       LEFT(MiddleStuff, charindex('|', MiddleStuff) - 1)
	   else null end as Delivery_OppKey,
	   case when a.Delivery_Status = 'Delivery' then
		substring(MiddleStuff, charindex('|', MiddleStuff)+1, len(MiddleStuff) - CHARINDEX('|', reverse(MiddleStuff)) - charindex('|', MiddleStuff))
		else null end as MiddleStuff2,
		case when a.Delivery_Status = 'Delivery' then
       REVERSE(LEFT(reverse(MiddleStuff), charindex('|', reverse(MiddleStuff)) - 1))
	   else null end as FP_Duration
from [GAAPIData].[dbo].[python_VIEW_4] as a
GO

--parsing MiddleStuff2 when Delivery Status is Delivery
alter view "python_VIEW_6" as 
with a as (select a.MiddleStuff2 as MiddleStuff2 from [GAAPIData].[dbo].[python_VIEW_5] as a)
select a.*, case when a.Delivery_Status = 'Delivery' then
       LEFT(MiddleStuff2, charindex('|', MiddleStuff2) - 1)
	   else null end as Delivery_DealerID,
	   case when a.Delivery_Status = 'Delivery' then
		substring(MiddleStuff2, charindex('|', MiddleStuff2)+1, len(MiddleStuff2) - CHARINDEX('|', reverse(MiddleStuff2)) - charindex('|', MiddleStuff2))
		else null end as DeliveryDuration,
		case when a.Delivery_Status = 'Delivery' then
       REVERSE(LEFT(reverse(MiddleStuff2), charindex('|', reverse(MiddleStuff2)) - 1))
	   else null end as FS_Duration
from [GAAPIData].[dbo].[python_VIEW_5] as a
GO

--combining delivery columns and other columns
alter view "python_VIEW_7" as 
with a as (select a.MiddleStuff2 as MiddleStuff2 from [GAAPIData].[dbo].[python_VIEW_6] as a)
select a.*, case when a.Delivery_Status = 'Other' then a.Opp_Key
	   else a.Delivery_OppKey end as OppKey,
	   case when a.Delivery_Status = 'Other' then a.Dealer_ID
			else a.Delivery_DealerID end as DealerID,
	   case when a.Delivery_Status = 'Other' then a.SalesConsultant
			else a.DeliveryConsultant end as Sales_Consultant
from [GAAPIData].[dbo].[python_VIEW_6] as a
GO

--parsing key value pairs for Opp Key, Dealer ID, and Sales Consultant
alter view "python_VIEW_8" as 
with a as (select a.OppKey, a.DealerID, a.Sales_Consultant, a.FS_Duration, a.FP_Duration, a.VIN, a.Event_Name, a.DeliveryDuration from [GAAPIData].[dbo].[python_VIEW_7] as a)
select a.*, 
       REVERSE(LEFT(reverse(OppKey), charindex(':', reverse(OppKey)) -1)) as OppKey_Final,
	   REVERSE(LEFT(reverse(DealerID), abs(charindex(':', reverse(DealerID)) -1))) as DealerID_Final,
	   REVERSE(LEFT(reverse(Sales_Consultant), abs(charindex(':', reverse(Sales_Consultant)) -1))) as SalesConsultant_Final,
	   REVERSE(LEFT(reverse(VIN), abs(charindex(':', reverse(VIN)) -1))) as VIN_Final,
	   REVERSE(LEFT(reverse(Event_Name), abs(charindex(':', reverse(Event_Name)) -1))) as EventName,
	   --concat(a.EventName, '|', a.eventLabel) as eventString,
	   case when FP_Duration like '%invalid%' then null else LTRIM(RTRIM(RIGHT(a.FP_Duration, 9))) end as FPDuration_Final,
		case when FS_Duration like '%invalid%' then null else LTRIM(RTRIM(RIGHT(a.FS_Duration, 9))) end as FSDuration_Final,
	   case when DeliveryDuration like '%invalid%' then null else LTRIM(RTRIM(RIGHT(a.DeliveryDuration, 9))) end as DeliveryDuration_Final
from [GAAPIData].[dbo].[python_VIEW_7] as a
GO

--parsing out FP Duration, FS Duration, Delivery Duration and converting to int
alter view "python_VIEW_9" as 
with a as (select a.FPDuration_Final, a.FSDuration_Final, a.DeliveryDuration_Final from [GAAPIData].[dbo].[python_VIEW_8] as a)
select a.*,
       CONVERT(INT, LEFT(a.FPDuration_Final, abs(charindex(':', a.FPDuration_Final) - 1)))*60*60 as FP_Hours,
		CONVERT(INT, SUBSTRING(a.FPDuration_Final, abs(charindex(':', FPDuration_Final)+1), len(a.FPDuration_Final) - abs(CHARINDEX(':', reverse(a.FPDuration_Final))) - abs(charindex(':', FPDuration_Final))))*60 as FP_Min,
       CONVERT(INT, REVERSE(LEFT(reverse(a.FPDuration_Final), abs(charindex(':', reverse(a.FPDuration_Final)) - 1)))) as FP_Sec,
	   CONVERT(INT, LEFT(a.FSDuration_Final, abs(charindex(':', a.FSDuration_Final) - 1)))*60*60 as FS_Hours,
		CONVERT(INT, SUBSTRING(a.FSDuration_Final, abs(charindex(':', FSDuration_Final)+1), len(a.FSDuration_Final) - abs(CHARINDEX(':', reverse(a.FSDuration_Final))) - abs(charindex(':', FSDuration_Final))))*60 as FS_Min,
       CONVERT(INT, REVERSE(LEFT(reverse(a.FSDuration_Final), abs(charindex(':', reverse(a.FSDuration_Final)) - 1)))) as FS_Sec,
	    CONVERT(INT, LEFT(a.DeliveryDuration_Final, abs(charindex(':', a.DeliveryDuration_Final) - 1)))*60*60 as DD_Hours,
		CONVERT(INT, SUBSTRING(a.DeliveryDuration_Final, abs(charindex(':', DeliveryDuration_Final)+1), len(a.DeliveryDuration_Final) - abs(CHARINDEX(':', reverse(a.DeliveryDuration_Final))) - abs(charindex(':', DeliveryDuration_Final))))*60 as DD_Min,
       CONVERT(INT, REVERSE(LEFT(reverse(a.DeliveryDuration_Final), abs(charindex(':', reverse(a.DeliveryDuration_Final)) - 1)))) as DD_Sec
from [GAAPIData].[dbo].[python_VIEW_8] as a;
GO


--keeping only necessary columns and formatting date
alter view "python_VIEW_10_test" as 
select Account
	  ,[dateHourMinute]
	  ,CONVERT(DATETIME, STUFF(STUFF(STUFF(STUFF(dateHourMinute,11,0,':'), 9, 0, ' '), 5, 0, '-'), 8, 0, '-')) AS FormattedDate 
      ,[Delivery_Status]
      ,[eventAction]
      ,[eventCategory]
      ,[mobileDeviceModel]
      ,[sessionCount]
      ,[operatingSystemVersion]
	  ,case when [operatingSystemVersion] like '4%' then 'Android'
			when [operatingSystemVersion] like '5%' then 'Android'
			when [operatingSystemVersion] like '6%' then 'Android'
			when [operatingSystemVersion] like '7%' then 'Android'
			when [operatingSystemVersion] like '8%' then 'Android'
			when [operatingSystemVersion] = '9' then 'Android'
			when [operatingSystemVersion] = 'P' then 'Android'
			when [operatingSystemVersion] like '%Not Set%' then 'Blackberry'
			when [operatingSystemVersion] like '4.%' then 'Android'
			else 'iOS' end as 'OperatingSystem'
      ,[totalEvents]
      ,[uniqueEvents]
      ,[App_Version]
      ,[EventName]
	  ,[eventLabel]
	  ,[Error]
      ,[VIN_Final]
      ,[OppKey_Final]
      ,LTRIM(RTRIM([DealerID_Final])) as DealerID_Final
      ,[SalesConsultant_Final]
	  ,FP_Hours+FP_Min+FP_Sec as FPDuration
	  ,FS_Hours+FS_Min+FS_Sec as FSDuration
	  ,DD_Hours+DD_Min+DD_Sec as DDuration
  FROM [GAAPIData].[dbo].[python_VIEW_9]
  GO

--Creating view with data from April 2018 to date for Simbu
alter view "ICARNCAR_04012018_01292019"
as select *
from GAAPIData.[dbo].[python_VIEW_10_test]
where formattedDate >= '2018-04-01'

use GAAPIData
create view "VIEW_10" as 
select * from python_VIEW_10

delete from WeeklyTable

--creating master table (cancelled because it took too long to run)
select * into ICAR_NCAR_GoogleAnalyticsMaster
from python_VIEW_10


insert into GoogleAnalyticsMaster
select * from python_VIEW_10


--sending python_VIEW_10 to WeeklyTable
insert into WeeklyTable
select * from python_VIEW_10

use GAAPIData
--run from here 11/19/18
select top 1000 *
from WeeklyTable

select top 1000 * 
from GAAPIData.dbo.GoogleAnalyticsMaster 
where Delivery_Status = 'Other' and eventAction like '%VIN:%'

delete from GoogleAnalyticsMaster
where FormattedDate > '2018-09-30'

use GAAPIData
select top 1000 * from GoogleAnalyticsMaster
	where FormattedDate > '2018-09-30'

--tomorrow need to refresh entire extract with correct october data and delete october data from current master

  --insert weekly table into GoogleAnalyticsMaster which has all GA data since Jan 1, 2018 for all accounts
insert into GoogleAnalyticsMaster -- now has data from 1/1/2018 to 10/31/2018
select * from WeeklyTable

select top 1000 * from GoogleAnalyticsMaster
where Delivery_Status = 'Other' and eventAction like '%VIN:%'

create view "VIEW_10" as
select * from GoogleAnalyticsMaster

use GAAPIData
select account, sum(cast(uniqueEvents as int)) as UE
from NCAR_python_dropoff_table
group by account




delete from GoogleAnalyticsMaster
where cast(formatteddate as date) = '2018-09-30'

select max(formatteddate), min(formatteddate)
from GoogleAnalyticsMaster 






select max(formatteddate), min(formatteddate)
from GoogleAnalyticsMaster

--cleaning out python_dropoff_table and WeeklyTable
delete from WeeklyTable
delete from python_dropoff_table

select top 10 * from GAAPIData.dbo.WeeklyTable where Account = 'n'

use GAAPIData
--dropping all views
drop view [dbo].[python_TEST_VIEW]
drop view [dbo].[python_VIEW_1]
drop view [dbo].[python_VIEW_2]
drop view [dbo].[python_VIEW_3]
drop view [dbo].[python_VIEW_4]
drop view [dbo].[python_VIEW_5]
drop view [dbo].[python_VIEW_6]
drop view [dbo].[python_VIEW_7]
drop view [dbo].[python_VIEW_8]
drop view [dbo].[python_VIEW_9]
drop view [dbo].[python_VIEW_10]


  







  
