SELECT

personnel_no,
courier_id,
courier_name,
DATE(tgl_tugas) pod_date,

branch_no,
branch_name,
kanwil_name,
province_name,
city_name,
work_location,
branch_level,

-- courier_status,
-- enabled,
waybill_source,
express_type,

-- SUM(pickup) AS pickup,
SUM(pod) AS pod,

FROM (

SELECT *

FROM `datamart_idexp.dashboard_courier_incentive`
WHERE DATE(tgl_tugas) BETWEEN '2023-12-01' AND '2024-01-07'
AND province_name IN ('SUMATERA SELATAN','LAMPUNG','BENGKULU','JAMBI')
AND pod >0
ORDER BY tgl_tugas ASC
)

GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13
