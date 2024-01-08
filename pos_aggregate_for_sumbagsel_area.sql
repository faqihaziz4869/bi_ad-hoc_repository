WITH cx_regular_checking AS (
SELECT 

ww.waybill_no,
ww.order_no,
ww.ecommerce_order_no,
ww.pickup_branch_name,
DATETIME(ww.shipping_time,'Asia/Jakarta') shipping_time,
DATETIME(ww.pod_record_time,'Asia/Jakarta') pod_record_time,
CASE
      WHEN ww.pod_branch_name IS NOT NULL THEN pod_branch_name
      WHEN ww.pod_branch_name IS NULL THEN mb.branch_name
      END AS th_destination,

ww.delivery_branch_name,
ww.pod_branch_name,
t0.option_name AS waybill_source,
ww.parent_shipping_cleint vip_username,
ww.vip_customer_name sub_account,
rd16.option_name AS void_status,

rd1.option_name AS return_status,
DATETIME(rr.return_record_time,'Asia/Jakarta') return_regist_time,
rr.return_branch_name return_register_branch,
t5.return_type AS remarks_return,
DATETIME(rr.return_confirm_record_time,'Asia/Jakarta') return_confirm_time,
rc.option_name AS return_confirm_status,
rr.return_shipping_fee,
pu3.pulau AS return_area, 
fk.kanwil_name return_area_kanwil,
DATETIME(rr.return_pod_record_time,'Asia/Jakarta') return_pod_record_time,
ww.sender_name, --tambahan kolom

ww.recipient_province_name,
ww.recipient_city_name,
ww.recipient_district_name,
fkd.kanwil_name kanwil_delivery,


FROM `datawarehouse_idexp.waybill_waybill` ww
LEFT OUTER JOIN `dev_idexp.masterdata_branch_coverage_th` mb ON ww.recipient_district_id = mb.district_id
LEFT OUTER JOIN `datawarehouse_idexp.waybill_return_bill` rr ON ww.waybill_no = rr.waybill_no
AND DATE(rr.update_time,'Asia/Jakarta') >= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -62 DAY))
LEFT OUTER JOIN `datawarehouse_idexp.system_option` rd16 ON rd16.option_value = ww.void_flag AND rd16.type_option = 'voidFlag'
LEFT OUTER JOIN `datawarehouse_idexp.system_option` rd1 ON rd1.option_value = ww.return_flag AND rd1.type_option = 'returnFlag'
LEFT OUTER JOIN `datawarehouse_idexp.system_option` t0 ON t0.option_value = ww.waybill_source AND t0.type_option = 'waybillSource'
LEFT OUTER JOIN `datawarehouse_idexp.system_option` t1 ON t1.option_value = ww.waybill_status AND t1.type_option = 'waybillStatus'
LEFT OUTER JOIN `datawarehouse_idexp.system_option` rc ON rc.option_value = rr.return_confirm_status AND rc.type_option = 'returnConfirmStatus'
LEFT OUTER JOIN `grand-sweep-324604.datawarehouse_idexp.return_type` t5 ON rr.return_type_id = t5.id AND t5.deleted=0
LEFT OUTER JOIN `datamart_idexp.masterdata_city_mapping_area_island_new` pu3 ON rr.recipient_city_name = pu3.city and rr.recipient_province_name = pu3.province --Return_area_register, 
LEFT OUTER JOIN `datamart_idexp.masterdata_facility_to_kanwil` fk ON rr.return_branch_name = fk.branch_name
LEFT OUTER JOIN `datamart_idexp.mapping_kanwil_area` fkd ON ww.recipient_province_name = fkd.province_name


WHERE DATE(ww.shipping_time,'Asia/Jakarta') >= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -76 DAY))

QUALIFY ROW_NUMBER() OVER (PARTITION BY ww.waybill_no ORDER BY ww.update_time DESC)=1

),

last_pos_photo as(
  SELECT
        ps.waybill_no,
        MAX(ps.problem_reason) OVER (PARTITION BY ps.waybill_no ORDER BY ps.operation_time DESC) AS last_pos_reason,
        MAX(DATETIME(ps.operation_time,'Asia/Jakarta')) OVER (PARTITION BY ps.waybill_no ORDER BY ps.operation_time DESC) AS last_pos_attempt,
        MAX(ps.operation_user_name) OVER (PARTITION BY ps.waybill_no ORDER BY ps.operation_time DESC) last_pos_courier_name,
        MAX(ps.operation_user_id) OVER (PARTITION BY ps.waybill_no ORDER BY ps.operation_time DESC) last_pos_courier_id,
        MAX(ps.operation_branch_name) OVER (PARTITION BY ps.waybill_no ORDER BY ps.operation_time DESC) last_pos_branch_name,
        MAX(ps.operation_branch_id) OVER (PARTITION BY ps.waybill_no ORDER BY ps.operation_time DESC) last_pos_branch_id,
        MAX(prt.option_name) OVER (PARTITION BY ps.waybill_no ORDER BY ps.operation_time DESC) last_pos_type,

        -- MAX(sc.photo_url) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time DESC) AS last_pos_photo_url,

              FROM `datawarehouse_idexp.waybill_problem_piece` ps
              -- LEFT OUTER JOIN `datawarehouse_idexp.waybill_waybill_line` sc ON ps.waybill_no = sc.waybill_no
              --     AND DATE(sc.record_time,'Asia/Jakarta') >= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -76 DAY)) AND sc.operation_type IN ('18') 
              --     AND sc.problem_type NOT IN ('02')
              -- LEFT join `grand-sweep-324604.datawarehouse_idexp.res_problem_package` t4 on sc.problem_code = t4.code and t4.deleted = '0'
              -- LEFT OUTER JOIN `datawarehouse_idexp.system_option` t1 ON t1.option_value = sc.problem_type AND t1.type_option = 'problemType'
              LEFT OUTER JOIN `datawarehouse_idexp.system_option` prt ON ps.problem_type  = prt.option_value AND prt.type_option = 'problemType'
              WHERE DATE(ps.operation_time,'Asia/Jakarta') >= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -76 DAY))

              AND ps.problem_type NOT IN ('02')

              QUALIFY ROW_NUMBER() OVER (PARTITION BY ps.waybill_no ORDER BY ps.operation_time DESC)=1
         ),


join_waybill_and_pos AS (
SELECT 

  waybill_no,
  shipping_time,
  order_no,
  ecommerce_order_no,
  pickup_branch_name,
  th_destination,
  delivery_branch_name,
  pod_branch_name,
  pod_record_time,
  waybill_source,
  vip_username,
  sub_account,
  void_status,

  last_pos_reason,
  -- last_pos_photo_url,
  last_pos_attempt,
  last_pos_type,
  last_pos_courier_name,
  sender_name, --tambahan kolom
  last_pos_branch_name,
  last_pos_branch_id,
  
recipient_province_name,
recipient_city_name,
recipient_district_name,
kanwil_delivery,
branch_no,
province_name,
city_name,
work_location,
personnel_no,

FROM (
  SELECT 

  cx.waybill_no,
  cx.shipping_time,
  cx.order_no,
  cx.ecommerce_order_no,
  cx.pickup_branch_name,
  CASE 
      WHEN cx.th_destination IS NULL THEN cx.delivery_branch_name
      ELSE cx.th_destination
      END AS th_destination,
      
  cx.delivery_branch_name,
  cx.pod_branch_name,
  cx.pod_record_time,
  cx.waybill_source,
  cx.vip_username,
  cx.sub_account,
  cx.void_status,
  cx.sender_name, --tambahan kolom

  ps.last_pos_reason,
  -- ps.last_pos_photo_url,
  ps.last_pos_attempt,
  ps.last_pos_type,
  ps.last_pos_courier_name,
  ps.last_pos_branch_name,
  ps.last_pos_branch_id,

  cx.recipient_province_name,
cx.recipient_city_name,
cx.recipient_district_name,
kw.kanwil_name kanwil_delivery,
fk.branch_no,
fk.province_name,
fk.city_name,
fk.work_location,
cb.personnel_no,



FROM cx_regular_checking cx
LEFT OUTER JOIN last_pos_photo ps ON cx.waybill_no = ps.waybill_no
LEFT OUTER JOIN `datamart_idexp.masterdata_facility_to_kanwil` fk ON ps.last_pos_branch_id = fk.id
LEFT OUTER JOIN `datamart_idexp.mapping_kanwil_area` kw ON fk.province_name = kw.province_name
LEFT OUTER JOIN `datamart_idexp.masterdata_courier_to_branch` cb ON ps.last_pos_courier_id = cb.id

)
WHERE DATE(last_pos_attempt) BETWEEN '2023-12-01' AND '2024-01-07'
AND province_name IN ('SUMATERA SELATAN','LAMPUNG','BENGKULU','JAMBI')
)

SELECT 
DATE(last_pos_attempt) pos_attempt,
last_pos_courier_name pos_courier_name,
personnel_no courier_oms_id,
last_pos_branch_name pos_branch_name,
province_name,
kanwil_delivery kanwil_name,
city_name,
last_pos_reason,
COUNT(waybill_no) total_awb,


FROM join_waybill_and_pos

GROUP BY 1,2,3,4,5,6,7,8

