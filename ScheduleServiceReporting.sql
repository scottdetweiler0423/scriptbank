/****** Script for SelectTopNRows command from SSMS  ******/
use GAAPIData


--Creating dataset for Tableau
select 



--adding schedule service flag to schedule service vin table
ALTER TABLE [GAAPIData].[dbo].[ScheduleServiceVINs_DealerID]
  ADD ScheduleServiceFlag varchar;

update [GAAPIData].[dbo].[ScheduleServiceVINs_DealerID]
set [ScheduleServiceFlag] = 1


--trimming DealerIDs from Google Analytics where VIN was delivered and schedule service was initiated
alter view "SS_TrimmedDealerIDs" as 
select ltrim(rtrim([DealerID Final])) as TrimDealerID, a.[Schedule Service VINs], a.scheduleserviceflag, a.[Delivery Date]
from GAAPIData.[dbo].[ScheduleServiceVINs_DealerID] as a
where --month(cast(a.[Delivery Date] as date)) = 10
a.[Schedule Service VINs] like '%3N1AB7AP9JY278344%'
 or a.[Schedule Service VINs] like  '%JN1BJ1CP1JW157848%'
 or a.[Schedule Service VINs] like  '%KNMAT2MT0JP615023%'

/* creating view with only DealerLogix VINs by joining Enrollement table 
with ScheduleServiceVINs from GA */
alter view "SS_DealerLogixVINs" as 
select TrimDealerID, a.[Schedule Service VINs], a.scheduleserviceflag, a.[Delivery Date], b.vendor
from GAAPIData.[dbo].[SS_TrimmedDealerIDs] as a
inner join 
(
select ltrim(rtrim([Dealer ID])) as DealerID, Vendor
from [GAAPIData].dbo.[EnrolledDealers_011419]
) b
on a.TrimDealerID = b.DealerID
where b.vendor = 'DealerLogix'
and cast(a.[Delivery Date] as date) between '2018-10-01' and '2018-10-31' --Only considering VINs delivered after feature was enabled for DealerLogix


select month(cast(a.[Delivery Date] as date)) as mth, count(distinct a.[Schedule Service VINs])
from SS_DealerLogixVINs as a
group by month(cast(a.[Delivery Date] as date))

select cast(a.[Delivery Date] as date), a.[Schedule Service VINs]
from SS_DealerLogixVINs as a
where 
a.[Schedule Service VINs] like '%3N1AB7AP9JY278344%'
 or a.[Schedule Service VINs] like  '%JN1BJ1CP1JW157848%'
 or a.[Schedule Service VINs] like  '%KNMAT2MT0JP615023%'

 select count(distinct a.[Schedule Service VINs])
from SS_DealerLogixVINs as a
 



/*
select a.[Delivery Date], count(distinct a.[Schedule Service VINs])
from DealerLogixVINs as a
--where a.scheduleserviceflag = 1
group by  a.[Delivery Date]

----Generating views just on DealerLogix VINs------
select count(distinct [DealerID Final])
from SS_DealerLogixVINs 
*/

--Generating table of DealerLogix Service data with full VIN
drop table FirstServiceReport_011319_v1

select * into "FirstServiceReport_011319_v1" --transformed table
from
(
select *, coalesce(case when len(vin) = 17 then vin else null end
					,case when len(dmsVehicleId) = 17 then dmsVehicleId else null end
					,case when len(dmsVehId) = 17 then dmsVehId else null end) as 'fullVin'
		, coalesce(case when len(utcCreatedOn) = 23 then utcCreatedOn else null end
					,case when len(utcLastModifiedOn) = 23 then utcLastModifiedOn else null end) as 'CreateDate'
from [GAAPIData].dbo.[FirstServiceReport_011319] as a -- raw data from Alex
where coalesce(case when len(vin) = 17 then vin else null end
					,case when len(dmsVehicleId) = 17 then dmsVehicleId else null end
					,case when len(dmsVehId) = 17 then dmsVehId else null end) is not null
) z



--Joining DealerLogix VINs from Google Analytics with vins in Service data
alter view "SS_ServiceReport_011419_v1" as
select *
from FirstServiceReport_011319_v1 as a
inner join
(
select ltrim(rtrim([Schedule Service VINs])) as SS_VIN_Trim, [ScheduleServiceFlag], [Delivery Date]
from [GAAPIData].dbo.[SS_DealerLogixVINs]
) z
on a.fullVin = z.SS_VIN_Trim

--275
select count(distinct fullvin)
from SS_ServiceReport_011419_v1


/*get the first schedule date (utcCreatedOn) for all VINs 
where the schedule date is between June 7th and Dec 31st*/
--alter view "SS_ScheduleReport" as 
select CreatetoService, count(distinct fullVin)
from testview
group by CreatetoService


alter view "testview" as 

--select month(cast(x.[apptdatetime] as date)), x.noShow1, count(distinct x.fullvin)
select count(distinct x.fullvin)
from
(
select a.appointmentOrigin
		,a.fullVin
		,a.[Delivery Date]
		,a.CreateDate
		--,z.minCreateDate
		,a.utcCreatedOn
		,a.utcLastModifiedOn
		,a.[apptDateTime]
		,datediff(day, cast(a.[Delivery Date] as date), cast(a.CreateDate as date)) as DeliverytoCreate
		,datediff(day, cast(a.[CreateDate] as date), cast(a.apptDateTime as date)) as CreatetoService
		,a.[ScheduleServiceFlag]
		--,min(a.noshow1) as noShow
		,noShow1
FROM [GAAPIData].[dbo].[SS_ServiceReport_011419_v1] as a
/*
--where a.appointmentOrigin = 'BAS'
) x

where x.appointmentOrigin = 'BAS' 
		and cast(x.CreateDate as date) between '2018-10-01' and '2018-10-31'
		and cast(x.apptdatetime as date) between '2018-10-01' and '2018-12-31'
*/
right join
(
select fullVin, min(CreateDate) as minCreateDate
from [GAAPIData].[dbo].[SS_ServiceReport_011419_v1]
group by fullVin
) z
on a.fullVin = z.fullVin
and a.CreateDate = z.minCreateDate
where appointmentOrigin = 'BAS'
	and cast(a.CreateDate as date) between '2018-10-01' and '2018-10-31'
	--and cast(a.apptdatetime as date) between '2018-10-01' and '2018-12-31'
--order by fullVin
) x
group by noShow1
--group by month(cast(x.[apptdatetime] as date)), x.noShow1
--where len(utcCreatedOn) = 23

3N1CP5CU6JL516133
3N1CP5CU6JL516133

3N1CN7AP4JL804733
3N1CN7AP4JL804733
	
where datediff(day, cast(a.[Delivery Date] as date), cast(a.CreateDate as date)) >= 0
 and datediff(day, cast(a.[CreateDate] as date), cast(a.apptDateTime as date)) >= 0 
 and  cast(a.CreateDate as date) between '2018-06-07' and '2018-12-31'
 and cast(a.apptdatetime as date) between '2018-06-07' and '2018-12-31'
 order by a.fullVin

select z.noShow, count(z.fullvin)
from
(
select fullvin, min(noshow1) as noShow
from testview
group by fullvin
) z
group by noshow

--and datediff(day, cast(a.[CreateDate] as date), cast(a.apptDateTime as date)) < 0
--and cast(CreateDate as date) > cast([Delivery Date] as date)

/*get the first schedule date (utcCreatedOn) for all VINs 
where the schedule date is between June 7th and Dec 31st*/
alter view "SS_ScheduleReport" as 
select z.[fullVin]
      ,z.[min_schedule_dt]
      ,a.[ScheduleServiceFlag]
      ,a.[Delivery Date]
      ,min(a.[noShow1]) as 'noShow'
FROM [GAAPIData].[dbo].[SS_ServiceReport_011419_v1] as a
right join 
(
select fullVin, min(CreateDate) as min_schedule_dt
  FROM [GAAPIData].[dbo].[SS_ServiceReport_011419_v1]
  where CreateDate is not null
  group by fullVin
)z
on a.fullVin = z.fullVin
and a.CreateDate = z.min_schedule_dt
where cast(z.min_schedule_dt as date) between '2018-06-07' and '2018-12-31'
--and a.first_appt_dt between '2018-01-01' and '2018-12-31'
group by z.fullVin ,z.min_schedule_dt, a.[ScheduleServiceFlag], a.[Delivery Date]
--where z.min_dt_flg is not null

/*
/*Find all VINs where their first schedule date is the same or close to delivery date, 
and service appointment was initiated in the app (i.e. ScheduleServiceFlag = 1) */
create view "SS_DlvrytoSched" as
SELECT a.fullVin
	  ,z.DlvryDate
	  ,a.min_schedule_dt
	  ,a.[ScheduleServiceFlag]
	  ,a.noshow
  FROM [GAAPIData].[dbo].[SS_ScheduleReport] as a
left join 
(
select ltrim(VIN_Final) as VIN_trim, min(FormattedDate) as DlvryDate
from [GAAPIData].dbo.[python_VIEW_10_test] -- Google Analytics full dataset
group by ltrim(VIN_Final)
) z
on a.fullVin = z.VIN_trim
--where abs(datediff(day, cast(z.DlvryDate as date), cast(a.min_schedule_dt as date))) < 7
--where cast(z.DlvryDate as date) = cast(a.min_schedule_dt as date)
where a.[ScheduleServiceFlag] = 1
*/

--Checking number of VINs grouped by number of days from delivery to schedule
select abs(datediff(day, cast(a.[Delivery Date] as date), cast(min_schedule_dt as date))) as DlvrytoSched, noShow, count(fullvin)
from SS_ScheduleReport as a
group by abs(datediff(day, cast(a.[Delivery Date] as date), cast(min_schedule_dt as date))), noShow


--------------Checking counts-------------

--872,602 VINs in service report
select count(distinct fullvin) 
from [FirstServiceReport_011319_v1]

/*5914 VINs with initiated service scheduling in 
GoogleAnalytics ICAR/NCAR from DealerLogix dealerships in 2018*/
/*3317 VINs initiated service scheduling in 
GoogleAnalytics ICAR/NCAR from DealerLogix dealerships after June 7th, 2018*/
select count(distinct a.[Schedule Service VINs])
from SS_DealerLogixVINs as a

--828 DealerLogix initiated service VINs are in First Service Report
select count(distinct fullVin)
from SS_ServiceReport_011419_v1

--348 VINs had their first create date in 2018 after June 7th
--713
select count(fullVin) 
from SS_ScheduleReport

/*Of the VINs where their schedule date was within two days of delivery, 
how many showed up for service?*/
--622/713
SELECT noshow, count(fullVin)
  FROM [GAAPIData].[dbo].[SS_ScheduleReport]
  group by noshow






/*
--333 VINs had their schedule within 2 days of delivery, and they were initiated in app
select count(fullVin)
from dlvrytosched_alldatediff
*/