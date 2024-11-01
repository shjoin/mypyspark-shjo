CREATE OR REPLACE PACKAGE BODY DWHSTG.pkg_st_news_etl
AS
  --
      --<cref>20140624-US7034-SS
    gv_application_id                    ex_error_log.application_id%TYPE := 400;
    --</cref>20140624-US7034-SS
    gc_rfnd_only_cncl_type            NUMBER := 40;
    gc_rfnd_one_iss_cncl_type         NUMBER := 60;
    gc_rfnd_prnt_renewal_cncl_type    NUMBER := 110;
    gv_rnw_bill_override							NUMBER;

   -----------------------------------------------------------------
   -- Function Name: fn_find_max_batch_ctl_id
   --
   -- Created by: Kristen Markarian
   --
   -- Objective: This function will return max etl_batch_ctl_id
   --
   --
   -- Modifications:
   -- Date:        Created By:                Description:
   -- ------------ ----------------------- -------------------------
   -- 02/27/2014         Created.
   -----------------------------------------------------------------
   --
   FUNCTION fn_find_max_batch_ctl_id (
      pv_application_id_i   IN ex_error_log.application_id%TYPE,
      pv_process_name_i     IN ex_error_log.process_name%TYPE,
      pv_function_name_i    IN ex_error_log.function_name%TYPE,
      pv_source_system_id   IN NUMBER)
      RETURN NUMBER
   IS
      lv_batch_ctl_id   NUMBER;
   BEGIN
      p_step := 1;
      p_message :=
         'Capture MAX (etl_batch_ctl_id) from TR_ETL_BATCH_CTL table ..';

      --DBMS_OUTPUT.put_line (p_message || CHR (10));

      SELECT MAX (etl_batch_ctl_id)
        INTO lv_batch_ctl_id
        FROM ls_etl_batch_ctl
       WHERE source_system_id = pv_source_system_id AND status_id = 0;

      RETURN lv_batch_ctl_id;
   EXCEPTION
      WHEN OTHERS
      THEN
         p_error_message := SQLERRM;
         p_error_id :=
            pkg_error.fn_insert_error_log (pv_application_id_i,
                                           pv_process_name_i,
                                           pv_function_name_i,
                                           '1',
                                           SUBSTR (SQLERRM, 1, 4000),
                                           NULL,
                                           NULL,
                                           NULL);
         raise_application_error (
            -20099,
               '['
            || SUBSTR (p_error_message, 5, 5)
            || '], ['
            || TRIM (p_error_id)
            || '] Step '
            || p_step
            || ': '
            || p_message);
   END fn_find_max_batch_ctl_id;

   ---
   --
   FUNCTION fn_find_run_date (
      pv_application_id_i   IN ex_error_log.application_id%TYPE,
      pv_process_name_i     IN ex_error_log.process_name%TYPE,
      pv_function_name_i    IN ex_error_log.function_name%TYPE,
      pv_date_flag          IN VARCHAR2,
      pv_source_system_id   IN NUMBER)
      RETURN DATE
   IS
      lv_run_date   DATE;
   BEGIN
      p_step := 1;
      p_message :=
         'Capture either start or end date from TR_ETL_BATCH_CTL table ..';
      DBMS_OUTPUT.put_line (p_message || CHR (10));

      IF pv_date_flag = 'S'
      THEN
         SELECT process_start_date
           INTO lv_run_date
           FROM ls_etl_batch_ctl
          WHERE etl_batch_ctl_id =
                   fn_find_max_batch_ctl_id (500,
                                             'dwhk002',
                                             'fn_find_max_batch_ctl_id',
                                             pv_source_system_id);
      ELSE
         SELECT process_end_date
           INTO lv_run_date
           FROM ls_etl_batch_ctl
          WHERE etl_batch_ctl_id =
                   fn_find_max_batch_ctl_id (500,
                                             'dwhk002',
                                             'fn_find_max_batch_ctl_id',
                                             pv_source_system_id);
      END IF;

      RETURN lv_run_date;
   EXCEPTION
      WHEN OTHERS
      THEN
         p_error_message := SQLERRM;
         p_error_id :=
            pkg_error.fn_insert_error_log (pv_application_id_i,
                                           pv_process_name_i,
                                           pv_function_name_i,
                                           '1',
                                           SUBSTR (SQLERRM, 1, 4000),
                                           NULL,
                                           NULL,
                                           NULL);
         raise_application_error (
            -20099,
               '['
            || SUBSTR (p_error_message, 5, 5)
            || '], ['
            || TRIM (p_error_id)
            || '] Step '
            || p_step
            || ': '
            || p_message);
   END fn_find_run_date;
--
   -----------------------------------------------------------------
   -- Function Name: FN_GET_MAPS_MERCHANT_ORDER_NO
   --
   -- Created by: Brian Wolf
   --
   -- Objective: This function will return the MAPS_MERCHANT_ORDER_NO.
   --
   --
   -- Modifications:
   -- Date:        Created By:                Description:
   -- ------------ ----------------------- -------------------------
   -- 06/6/2018   Brian Wolf        Created.
   -----------------------------------------------------------------

FUNCTION FN_GET_MAPS_MERCHANT_ORDER_NO
(
    pv_mp_trans_id_i  IN st_billing_response.mp_trans_id%TYPE,
    pv_bill_attempt_no_i IN st_billing_response.bill_attempt_no%TYPE,
    pv_application_id_i   IN ex_error_log.application_id%TYPE,
    pv_process_name_i     IN ex_error_log.process_name%TYPE,
    pv_function_name_i    IN ex_error_log.function_name%TYPE
)
return varchar2 is
    lv_maps_merchant_order_no       VARCHAR2(20);
begin
      p_message :='Derive the MAPS_MERCHANT_ORDER_NO';
      p_step := 1;
    IF nvl(pv_mp_trans_id_i,0) = 0 then
              lv_maps_merchant_order_no := null;
    ELSE
                  lv_maps_merchant_order_no := (SUBSTR(pv_mp_trans_id_i,-7) ||
                                 chr(pv_bill_attempt_no_i+64) ||
                                 '-' ||
                                 LPAD((SUBSTR(pv_mp_trans_id_i,1,(LENGTH(pv_mp_trans_id_i))-7)),7,0)
                   );
   END IF;

    return lv_maps_merchant_order_no;

EXCEPTION
   WHEN OTHERS THEN
   p_error_message := SQLERRM;
         p_error_id :=
            pkg_error.fn_insert_error_log (pv_application_id_i,
                                           pv_process_name_i,
                                           pv_function_name_i,
                                           '1',
                                           SUBSTR (SQLERRM, 1, 4000),
                                           NULL,
                                           NULL,
                                           NULL);
         raise_application_error (
            -20099,
               '['
            || SUBSTR (p_error_message, 5, 5)
            || '], ['
            || TRIM (p_error_id)
            || ']'
            || '] Step '
            || p_step
            || ': '
            || p_message);
   END FN_GET_MAPS_MERCHANT_ORDER_NO;



   -----------------------------------------------------------------
   -- Procedure Name: prc_ins_st_order_line
   --
   -- Created by: Kristen Markarian
   --
   -- Objective: This procedure will insert new order data into st_order_line
   --
   --
   -- Modifications:
   -- Date:        Created By:                Description:
   -- ------------ ----------------------- -------------------------
   -- 03/03/2014              Created
   -- 11/24/2016   Manozkumaar              US9544 - Added site_id column
   -- 04/06/2020   LewisC                  SBO-20012 - Added bill_interval_desc column
   -- 04/16/2020   LewisC                  Modified for SBO-20192
   -----------------------------------------------------------------
   --

   PROCEDURE prc_ins_st_order_line (
      pv_application_id_i   IN ex_error_log.application_id%TYPE,
      pv_process_name_i     IN ex_error_log.process_name%TYPE,
      pv_function_name_i    IN ex_error_log.function_name%TYPE)
   AS
      lv_start_date   DATE;
      lv_end_date     DATE;

   BEGIN
      --
      p_step := 1;
      p_message := 'Capture start date from TR_ETL_BATCH_CTL table ..';
      DBMS_OUTPUT.put_line (p_message || CHR (10));

      lv_start_date :=
         fn_find_run_date (500,
                           'DWHK002',
                           'fn_find_max_batch_ctl_id',
                           'S',
                           1);
      p_step := 2;
      p_message := 'Capture end date from TR_ETL_BATCH_CTL table ..';
      DBMS_OUTPUT.put_line (p_message || CHR (10));

      lv_end_date :=
         fn_find_run_date (102,
                           'DWHK002',
                           'fn_find_max_batch_ctl_id',
                           'E',
                           1);
      --
      p_step := 3;
      p_message := 'truncate st_line_item table ..';
      DBMS_OUTPUT.put_line (p_message || CHR (10));

      EXECUTE IMMEDIATE 'truncate table ST_ORDER_LINE';

      p_step := 4;
      p_message := 'Populating staging table with new order/item info ..';
      DBMS_OUTPUT.put_line (p_message || CHR (10));

      INSERT INTO st_order_line (order_id,
                                 affiliate_cd,
                                 alt_order_id,
                                 cust_id,
                                 order_request_id,
                                 order_date,
                                 source_cd,
                                 line_item,
                                 order_request_line_item,
                                 external_ref_id,
                                 original_offer_id,
                                 oe_user_id,
                                 oe_batch_id,
                                 oe_customer_no,
                                 oe_order_no,
                                 vendor_acct_num,
                                 cno_flag,
                                 source_system_id,
                                 create_date,
                                 site_id,
                                 BILL_INTERVAL_DESC,
                                 BILL_INTERVAL_PYMT_AMT)
         (SELECT o.order_id,
                 o.affiliate_cd,
                 o.alt_order_id,
                 o.cust_id,
                 o.dm_order_id,
                 o.order_date,
                 o.source_cd,
                 o.line_item,
                 o.dm_line_item,
                 o.external_ref_id,
                 o.offer_id,
                 o.oe_user_id,
                 o.oe_batch_id,
                 o.oe_customer_no,
                 o.oe_order_no,
                 o.vendor_acct_num,
                 DECODE (li.delayed_order_id, o.order_id, 1, 0),
                 1,
                 SYSDATE,
                 o.oe_site_id,
                 RS_NSS_PKG_BILLING_TBL.FN_GET_BILLING_INTERVAL_DESC (pv_order_id   => o.order_id,
                                                                      pv_line_item  => o.line_item,
                                                                      pv_appflag    => 'DWH'),
                 Fn_Get_Payment_Amt ( pv_order_id_i          => o.order_id,
                                      pv_line_item_i         => o.line_item,
                                      pv_bill_yr_i  	     => 1,
                                      pv_offer_id_i          => o.offer_id,
                                      pv_campaign_id_i       => o.campaign_id,
                                      pv_application_id_i    => pv_application_id_i,
                                      pv_process_name_i      => pv_process_name_i,
                                      pv_function_name_i     => pv_function_name_i,
                                      pv_subscription_type   => 'N'  -- Assume new order for new order line record
                                     )
            FROM (SELECT o.order_id,
                         o.affiliate_cd,
                         oe.alt_order_id,
                         o.cust_id,
                         oe.dm_order_id,
                         o.order_date,
                         o.source_cd,
                         ol.line_item,
                         ole.dm_line_item,
                         ole.external_ref_id,
                         ol.offer_id,
                         o.oe_user_id,
                         o.oe_batch_id,
                         o.oe_customer_no,
                         o.oe_order_no,
                         ol.vendor_acct_num,
                         o.oe_site_id,
                         rns.campaign_id
                    FROM rs_nss_order o,
                         rs_nss_order_line ol,
                         rs_nss_order_extension oe,
                         rs_nss_order_line_extension ole,
                         rs_nss_source rns
                   WHERE     o.order_id = oe.order_id(+)
                         AND o.order_id = ol.order_id
                         AND o.source_cd = rns.source_cd
                         AND ol.order_id = ole.order_id(+)
                         AND ol.line_item = ole.line_item(+)
                         AND o.order_date > lv_start_date
                         AND o.order_date <= lv_end_date) o,
                 rs_linked_items li
           WHERE o.order_id = li.delayed_order_id(+));
   EXCEPTION
      WHEN OTHERS
      THEN
         p_error_message := SQLERRM;
         p_error_id :=
            pkg_error.fn_insert_error_log (pv_application_id_i,
                                           pv_process_name_i,
                                           pv_function_name_i,
                                           '1',
                                           SUBSTR (SQLERRM, 1, 4000),
                                           NULL,
                                           NULL,
                                           NULL);
         raise_application_error (
            -20099,
               '['
            || SUBSTR (p_error_message, 5, 5)
            || '], ['
            || TRIM (p_error_id)
            || '] Step '
            || p_step
            || ': '
            || p_message);
   END prc_ins_st_order_line;

   -----------------------------------------------------------------
   -- Procedure Name: prc_ins_st_cancel_line
   --
   -- Created by: Kristen Markarian
   --
   -- Objective: This procedure will insert new order data into st_cancel_line
   --
   --
   -- Modifications:
   -- Date:        Created By:             Description:
   -- ------------ ----------------------- -------------------------
   -- 03/03/2014        Created
   -- 08/07/2015   Krishan Kumar Tripathi  Added column to calculate the days till an offer was active
   -- 08/26/2016   Jonathan Smith          US11187 - Add order request order and line
   -----------------------------------------------------------------
   --

   PROCEDURE prc_ins_st_cancel_line (
      pv_application_id_i   IN ex_error_log.application_id%TYPE,
      pv_process_name_i     IN ex_error_log.process_name%TYPE,
      pv_function_name_i    IN ex_error_log.function_name%TYPE)
   AS
      lv_start_date   DATE;
      lv_end_date     DATE;
   BEGIN
      --
      p_step := 1;
      p_message := 'Capture start date from TR_ETL_BATCH_CTL table ..';
      DBMS_OUTPUT.put_line (p_message || CHR (10));

      lv_start_date :=
         fn_find_run_date (500,
                           'DWHK002',
                           'fn_find_max_batch_ctl_id',
                           'S',
                           1);

      p_step := 2;
      p_message := 'Capture end date from TR_ETL_BATCH_CTL table ..';
      DBMS_OUTPUT.put_line (p_message || CHR (10));

      lv_end_date :=
         fn_find_run_date (102,
                           'DWHK002',
                           'fn_find_max_batch_ctl_id',
                           'E',
                           1);
      --
      p_step := 3;
      p_message := 'truncate st_line_item table ..';
      DBMS_OUTPUT.put_line (p_message || CHR (10));

      EXECUTE IMMEDIATE 'truncate table ST_CANCEL_LINE';

      p_step := 4;
      p_message := 'Populating staging table with new order/item info ..';
      DBMS_OUTPUT.put_line (p_message || CHR (10));

      INSERT INTO st_cancel_line (order_id,
                                  line_item,
                                  cancel_date,
                                  cancel_reason_cd,
                                  cancel_type_cd,
                                  ds_process_cancel,
                                  source_system_id,
                                  order_request_order_id,
                                  order_request_line_item,
                                  create_date)
         SELECT ol.order_id,
                ol.line_item,
                ol.cancel_date,
                ol.cancel_reason_cd,
                ol.cancel_type_cd,
                TRUNC (ol.cancel_date) - TRUNC (o.order_date),
                1,
                oe.dm_order_id,
                ole.dm_line_item,
                SYSDATE
           FROM rs_nss_order_line ol,
                rs_nss_order o,
                rs_nss_order_extension oe,
                rs_nss_order_line_extension ole
          WHERE ol.order_id = o.order_id
                AND o.order_id = oe.order_id(+)
                AND ol.order_id = ole.order_id(+)
                AND ol.line_item = ole.line_item(+)
                AND ol.cancel_date > lv_start_date
                AND ol.cancel_date <= lv_end_date;
   EXCEPTION
      WHEN OTHERS
      THEN
         p_error_message := SQLERRM;
         p_error_id :=
            pkg_error.fn_insert_error_log (pv_application_id_i,
                                           pv_process_name_i,
                                           pv_function_name_i,
                                           '1',
                                           SUBSTR (SQLERRM, 1, 4000),
                                           NULL,
                                           NULL,
                                           NULL);
         raise_application_error (
            -20099,
               '['
            || SUBSTR (p_error_message, 5, 5)
            || '], ['
            || TRIM (p_error_id)
            || '] Step '
            || p_step
            || ': '
            || p_message);
   END prc_ins_st_cancel_line;

   -----------------------------------------------------------------
   -- Procedure Name: prc_ins_st_fulfill_sched
   --
   -- Created by: Kristen Markarian
   --
   -- Objective: This procedure will insert into st_fulfill_sched
   --
   --
   -- Modifications:
   -- Date:        Created By:                Description:
   -- ------------ ----------------------- -------------------------
   -- 03/03/2014              Created
   -----------------------------------------------------------------
   --

   PROCEDURE prc_ins_st_fulfill_sched (
      pv_application_id_i   IN ex_error_log.application_id%TYPE,
      pv_process_name_i     IN ex_error_log.process_name%TYPE,
      pv_function_name_i    IN ex_error_log.function_name%TYPE)
   AS

      lv_start_date   DATE;
      lv_end_date     DATE;
   BEGIN
      --
      p_step := 1;
      p_message := 'Capture start date from TR_ETL_BATCH_CTL table ..';
      DBMS_OUTPUT.put_line (p_message || CHR (10));

      lv_start_date :=
         fn_find_run_date (500,
                           'DWHK003',
                           'fn_find_max_batch_ctl_id',
                           'S',
                           1);

      p_step := 2;
      p_message := 'Capture end date from TR_ETL_BATCH_CTL table ..';
      DBMS_OUTPUT.put_line (p_message || CHR (10));

      lv_end_date :=
         fn_find_run_date (102,
                           'DWHK003',
                           'fn_find_max_batch_ctl_id',
                           'E',
                           1);
      --
      p_step := 3;
      p_message := 'truncate st_fulfillment_sched table ..';
      DBMS_OUTPUT.put_line (p_message || CHR (10));

      EXECUTE IMMEDIATE 'truncate table ST_FULFILLMENT_SCHEDULE';

      p_step := 4;
      p_message :=
         'Populating staging table with new scheduled fulfillment info ..';
      DBMS_OUTPUT.put_line (p_message || CHR (10));

      INSERT INTO st_fulfillment_schedule (order_id,
                                           line_item,
                                           addr_cleansed_flag,
                                           cust_contract_term,
                                           combo_ff_sequence,
                                           offer_id,
                                           publisher_term,
                                           source_cd,
                                           cancel_not_ff,
                                           renewal_date,
                                           renewal_first_bill_date,
                                           sched_fulfillment_date,
                                           trans_date,
                                           trans_type,
                                           cno_flag,
                                           source_system_id,
                                           create_date,
										   price_notify_date)
         (SELECT f.order_id,
                 f.line_item,
                 f.addr_cleansed_flag,
                 f.bill_yr,
                 f.combo_ff_sequence,
                 f.offer_id,
                 f.publisher_term,
                 f.source_cd,
                 f.cancel_not_ff,
                 f.renewal_date,
                 f.renewal_first_bill_date,
                 f.sched_fulfillment_date,
                 f.trans_date,
                 f.trans_type,
                 DECODE (li.delayed_order_id, f.order_id, 1, 0),
                 1,
                 SYSDATE,
				 f.price_notify_date
            FROM (SELECT fs.order_id,
                         fs.line_item,
                         fs.addr_cleansed_flag,
                         fs.bill_yr,
                         fs.combo_ff_sequence,
                         fs.offer_id,
                         fs.publisher_term,
                         o.source_cd,
                         fs.cancel_not_ff,
                         fs.renewal_date,
                         fs.renewal_first_bill_date,
                         fs.sched_fulfillment_date,
                         fs.trans_date,
                         CASE
                            WHEN fs.trans_type = 'T'
                            THEN
                               'O'
                            WHEN fs.trans_type = 'U'
                            THEN
                               'R'
                            WHEN     fs.trans_type IN ('O', 'R')
                                 AND fs2.trans_type IN ('T', 'U')
                            THEN
                               'T'
                            ELSE
                               fs.trans_type
                         END
                            trans_type,
							fs.price_notify_date
                    FROM rs_nss_fulfillment_schedule fs,
                         rs_nss_fulfillment_schedule fs2,
                         rs_nss_order o
                   WHERE     fs.trans_date > lv_start_date
                         AND fs.trans_date <= lv_end_date
                         AND fs.order_id = o.order_id
                         AND fs.trans_type <> 'C'
                         AND fs.order_id = fs2.order_id(+)
                         AND fs.line_item = fs2.line_item(+)
                         AND fs.bill_yr = fs2.bill_yr(+)
                         AND FS2.TRANS_TYPE(+) IN ('T', 'U')
                  UNION ALL
                  SELECT fs.order_id,
                         fs.line_item,
                         fs.addr_cleansed_flag,
                         fs.bill_yr,
                         fs.combo_ff_sequence,
                         fs.offer_id,
                         fs.publisher_term,
                         o.source_cd,
                         fs.cancel_not_ff,
                         fs.renewal_date,
                         fs.renewal_first_bill_date,
                         fs.sched_fulfillment_date,
                         fs.trans_date,
                         CASE
                            WHEN    fs.trans_date < fs2.restart_date
                                 OR fs2.restart_date IS NULL
                            THEN
                               'C'
                            ELSE
                               'X'
                         END
                            trans_type,
							fs.price_notify_date
                    FROM rs_nss_fulfillment_schedule fs,
                         (SELECT t.order_id,
                                 t.line_item,
                                 t.bill_yr,
                                 t.trans_date AS orig_date,
                                 s.trans_date AS restart_date
                            FROM rs_nss_fulfillment_schedule t,
                                 rs_nss_fulfillment_schedule s
                           WHERE     t.order_id = s.order_id(+)
                                 AND t.line_item = s.line_item(+)
                                 AND t.bill_yr = s.bill_yr(+)
                                 AND t.trans_type IN ('T', 'U')
                                 AND s.trans_type(+) IN ('O', 'R')) fs2,
                         rs_nss_order o
                   WHERE     fs.trans_date > lv_start_date
                         AND fs.trans_date <= lv_end_date
                         AND fs.order_id = o.order_id
                         AND fs.trans_type = 'C'
                         AND fs.order_id = fs2.order_id(+)
                         AND fs.line_item = fs2.line_item(+)
                         AND fs.bill_yr = fs2.bill_yr(+)) f,
                 rs_linked_items li
           WHERE f.order_id = li.delayed_order_id(+));
   EXCEPTION
      WHEN OTHERS
      THEN
         p_error_message := SQLERRM;
         p_error_id :=
            pkg_error.fn_insert_error_log (pv_application_id_i,
                                           pv_process_name_i,
                                           pv_function_name_i,
                                           '1',
                                           SUBSTR (SQLERRM, 1, 4000),
                                           NULL,
                                           NULL,
                                           NULL);
         raise_application_error (
            -20099,
               '['
            || SUBSTR (p_error_message, 5, 5)
            || '], ['
            || TRIM (p_error_id)
            || '] Step '
            || p_step
            || ': '
            || p_message);
   END prc_ins_st_fulfill_sched;


   -----------------------------------------------------------------
   -- Procedure Name: prc_ins_st_fulfill_sent
   --
   -- Created by: Kristen Markarian
   --
   -- Objective: This procedure will insert into st_fulfill_sent
   --
   --
   -- Modifications:
   -- Date:        Created By:                Description:
   -- ------------ ----------------------- -------------------------
   -- 03/03/2014              Created
   -- 12/08/2016   Manozkumaar              Modified for adding flexind in table for US11598
   -- 01/29/2019   RAF                      Added current_term_notification_type
   -- 04/09/2019   Marc Simpson				Removed date condition for current_term_notification_type
   -- 04/10/2019   RAF                      Elinimate multiple current_term_not_type records from being returned
   -----------------------------------------------------------------
   --

   PROCEDURE prc_ins_st_fulfill_sent (
      pv_application_id_i   IN ex_error_log.application_id%TYPE,
      pv_process_name_i     IN ex_error_log.process_name%TYPE,
      pv_function_name_i    IN ex_error_log.function_name%TYPE)
   AS
      lv_start_date   DATE;
      lv_end_date     DATE;
   BEGIN
      --
      p_step := 1;
      p_message := 'Capture start date from TR_ETL_BATCH_CTL table ..';
      DBMS_OUTPUT.put_line (p_message || CHR (10));

      lv_start_date :=
         fn_find_run_date (500,
                           'DWHK003',
                           'fn_find_max_batch_ctl_id',
                           'S',
                           1);

      p_step := 2;
      p_message := 'Capture end date from TR_ETL_BATCH_CTL table ..';
      DBMS_OUTPUT.put_line (p_message || CHR (10));

      lv_end_date :=
         fn_find_run_date (102,
                           'DWHK003',
                           'fn_find_max_batch_ctl_id',
                           'E',
                           1);
      --
      p_step := 3;
      p_message := 'truncate st_fulfillment table ..';
      DBMS_OUTPUT.put_line (p_message || CHR (10));

      EXECUTE IMMEDIATE 'truncate table ST_FULFILLMENT';

      p_step := 4;
      p_message :=
         'Populating staging table with new sent fulfillment info ..';
      DBMS_OUTPUT.put_line (p_message || CHR (10));

      INSERT INTO st_fulfillment (order_id,
                                  line_item,
                                  cancel_issues,
                                  cancel_not_ff,
                                  cleared_ind,
                                  cust_contract_term,
                                  offer_id,
                                  net_remit_amt,
                                  source_cd,
                                  publisher_term,
                                  trans_date,
                                  trans_type,
                                  sent_fulfillement_date,
                                  expected_term_start_date,
                                  source_system_id,
                                  original_offer_id,
                                  fulfillment_id,
                                  create_date,
                                  current_term_flexbill_ind,
                                  current_term_model_type)
         (SELECT fs.order_id,
                 fs.line_item,
                 fs.cancel_issues,
                 fs.cancel_not_ff,
                 fs.cleared_ind,
                 fs.bill_yr,
                 fs.offer_id,
                 fs.net_remit_amt,
                 o.source_cd,
                 fs.publisher_term,
                 fs.trans_date,
                 CASE
                    WHEN fs.trans_type = 'T'
                    THEN
                       'O'
                    WHEN fs.trans_type = 'U'
                    THEN
                       'R'
                    WHEN     fs.trans_type IN ('O', 'R')
                         AND fs2.trans_type IN ('T', 'U')
                    THEN
                       'T'
                    ELSE
                       fs.trans_type
                 END,
                 fs.sent_fulfillement_date,
                 fs.expected_term_start_date,
                 1,
                 fs2.offer_id,
                 fs.fulfillment_id,
                 SYSDATE,
                 (SELECT fs3.RNW_FLEX_BILL_INTERVAL_RENEW
                 FROM rs_nss_fulfillment_schedule fs3
                WHERE     fs.order_id = fs3.order_id
                      AND fs.line_item = fs3.line_item
                      AND (fs.bill_yr - 1) = fs3.bill_yr
                      AND fs3.trans_type in ('O','R')
                      AND fs3.RNW_FLEX_BILL_INTERVAL_RENEW IS NOT NULL)
                 AS RNW_FLEX_BILL_INTERVAL_RENEW,
                (SELECT fs4.notification_type
                  FROM  rs_nss_fulfillment_schedule fs4
                 WHERE  fs.order_id  = fs4.order_id
                  AND   fs.line_item = fs4.line_item
                  AND   (fs.bill_yr - 1) = fs4.bill_yr
                  AND   fs4.trans_type in ('O','R')) current_term_model_type
            FROM rs_nss_fulfillment_schedule fs,
                 rs_nss_fulfillment_schedule fs2,
                 rs_nss_order o
           WHERE     fs.sent_fulfillement_date > lv_start_date
                 AND fs.sent_fulfillement_date <= lv_end_date
                 AND fs.trans_type <> 'C'
                 AND NVL (fs.cancel_not_ff, 0) <> 1
                 AND fs.order_id = fs2.order_id(+)
                 AND fs.line_item = fs2.line_item(+)
                 AND fs.bill_yr = fs2.bill_yr(+)
                 AND fs2.trans_type(+) IN ('T', 'U')
                 AND fs.order_id = o.order_id
          UNION ALL
          SELECT fs.order_id,
                 fs.line_item,
                 fs.cancel_issues,
                 fs.cancel_not_ff,
                 fs.cleared_ind,
                 fs.bill_yr,
                 fs.offer_id,
                 fs.net_remit_amt,
                 o.source_cd,
                 fs.publisher_term,
                 fs.trans_date,
                 CASE
                    WHEN    fs.sent_fulfillement_date < fs2.restart_date
                         OR fs2.restart_date IS NULL
                    THEN
                       'C'
                    ELSE
                       'X'
                 END,
                 fs.sent_fulfillement_date,
                 fs.expected_term_start_date,
                 1,
                 NULL,
                 fs.fulfillment_id,
                 SYSDATE,
                 (SELECT fs3.RNW_FLEX_BILL_INTERVAL_RENEW
                 FROM rs_nss_fulfillment_schedule fs3
                WHERE     fs.order_id = fs3.order_id
                      AND fs.line_item = fs3.line_item
                      AND (fs.bill_yr - 1) = fs3.bill_yr
                      AND fs3.trans_type in ('O','R')
                      AND fs3.RNW_FLEX_BILL_INTERVAL_RENEW IS NOT NULL)
                 AS RNW_FLEX_BILL_INTERVAL_RENEW,
                (SELECT fs4.notification_type
                  FROM  rs_nss_fulfillment_schedule fs4
                 WHERE  fs.order_id  = fs4.order_id
                  AND   fs.line_item = fs4.line_item
                  AND   (fs.bill_yr - 1) = fs4.bill_yr
                  AND   fs4.trans_type in ('O','R')) current_term_model_type
            FROM rs_nss_fulfillment_schedule fs,
                 (SELECT t.order_id,
                         t.line_item,
                         t.bill_yr,
                         t.sent_fulfillement_date AS orig_date,
                         s.sent_fulfillement_date AS restart_date
                    FROM rs_nss_fulfillment_schedule t,
                         rs_nss_fulfillment_schedule s
                   WHERE     t.order_id = s.order_id(+)
                         AND t.line_item = s.line_item(+)
                         AND t.bill_yr = s.bill_yr(+)
                         AND t.trans_type IN ('T', 'U')
                         AND s.trans_type(+) IN ('O', 'R')) fs2,
                 rs_nss_order o
           WHERE     fs.sent_fulfillement_date > lv_start_date
                 AND fs.sent_fulfillement_date <= lv_end_date
                 AND fs.trans_type = 'C'
                 AND NVL (fs.cancel_not_ff, 0) <> 1
                 AND fs.order_id = fs2.order_id(+)
                 AND fs.line_item = fs2.line_item(+)
                 AND fs.bill_yr = fs2.bill_yr(+)
                 AND fs.order_id = o.order_id
          UNION ALL
          SELECT fs.order_id,
                 fs.line_item,
                 fs.cancel_issues,
                 fs.cancel_not_ff,
                 fs.cleared_ind,
                 fs.bill_yr,
                 fs.offer_id,
                 fs.net_remit_amt,
                 o.source_cd,
                 fs.publisher_term,
                 fs.trans_date,
                 'F',
                 fs.sent_fulfillement_date,
                 fs.expected_term_start_date,
                 1,
                 NULL,
                 fs.fulfillment_id,
                 SYSDATE,
                 (SELECT fs3.RNW_FLEX_BILL_INTERVAL_RENEW
                 FROM rs_nss_fulfillment_schedule fs3
                WHERE     fs.order_id = fs3.order_id
                      AND fs.line_item = fs3.line_item
                      AND (fs.bill_yr - 1) = fs3.bill_yr
                      AND fs3.trans_type in ('O','R')
                      AND fs3.RNW_FLEX_BILL_INTERVAL_RENEW IS NOT NULL)
                 AS RNW_FLEX_BILL_INTERVAL_RENEW,
                (SELECT fs4.notification_type
                  FROM  rs_nss_fulfillment_schedule fs4
                 WHERE  fs.order_id  = fs4.order_id
                  AND   fs.line_item = fs4.line_item
                  AND   (fs.bill_yr - 1) = fs4.bill_yr
                  AND   fs4.trans_type in ('O','R')) current_term_model_type
            FROM rs_nss_fulfillment_schedule fs,
                 rs_nss_order o,
                 rs_nss_order_line ol
           WHERE     fs.sent_fulfillement_date > lv_start_date
                 AND fs.sent_fulfillement_date <= lv_end_date
                 AND fs.trans_type = 'C'
                 AND fs.cancel_not_ff = 1
                 AND fs.order_id = o.order_id
                 AND fs.order_id = ol.order_id
                 AND fs.line_item = ol.line_item
                 AND ol.cancel_type_cd = 100);
   EXCEPTION
      WHEN OTHERS
      THEN
         p_error_message := SQLERRM;
         p_error_id :=
            pkg_error.fn_insert_error_log (pv_application_id_i,
                                           pv_process_name_i,
                                           pv_function_name_i,
                                           '1',
                                           SUBSTR (SQLERRM, 1, 4000),
                                           NULL,
                                           NULL,
                                           NULL);
         raise_application_error (
            -20099,
               '['
            || SUBSTR (p_error_message, 5, 5)
            || '], ['
            || TRIM (p_error_id)
            || '] Step '
            || p_step
            || ': '
            || p_message);
   END prc_ins_st_fulfill_sent;

   -----------------------------------------------------------------
   -- Procedure Name: prc_ins_st_fulfill_backout
   --
   -- Created by: Kristen Markarian
   --
   -- Objective: This procedure will insert into st_fulfill_backout
   --
   --
   -- Modifications:
   -- Date:        Created By:                Description:
   -- ------------ ----------------------- -------------------------
   -- 05/12/2014              Created
   -----------------------------------------------------------------
   --

   PROCEDURE prc_ins_st_fulfill_backout (
      pv_application_id_i   IN ex_error_log.application_id%TYPE,
      pv_process_name_i     IN ex_error_log.process_name%TYPE,
      pv_function_name_i    IN ex_error_log.function_name%TYPE)
   AS
      lv_start_date   DATE;
      lv_end_date     DATE;
   BEGIN
      --
      p_step := 1;
      p_message := 'Capture start date from TR_ETL_BATCH_CTL table ..';
      DBMS_OUTPUT.put_line (p_message || CHR (10));

      lv_start_date :=
         fn_find_run_date (500,
                           'DWHK003',
                           'fn_find_max_batch_ctl_id',
                           'S',
                           1);

      p_step := 2;
      p_message := 'Capture end date from TR_ETL_BATCH_CTL table ..';
      DBMS_OUTPUT.put_line (p_message || CHR (10));

      lv_end_date :=
         fn_find_run_date (102,
                           'DWHK003',
                           'fn_find_max_batch_ctl_id',
                           'E',
                           1);
      --
      p_step := 3;
      p_message := 'truncate st_fulfill_backout table ..';
      DBMS_OUTPUT.put_line (p_message || CHR (10));

      EXECUTE IMMEDIATE 'truncate table ST_FULFILLMENT_BACKOUT';

      p_step := 4;
      p_message :=
         'Populating staging table with new fulfillment backout info ..';
      DBMS_OUTPUT.put_line (p_message || CHR (10));

      INSERT INTO st_fulfillment_backout (order_id,
                                          line_item,
                                          fulfillment_backout_date,
                                          backout_sent_ff_date,
                                          cancel_not_ff,
                                          cleared_ind,
                                          cust_contract_term,
                                          offer_id,
                                          net_remit_amt,
                                          publisher_term,
                                          trans_date,
                                          trans_type,
                                          source_system_id,
                                          create_date)
         (SELECT fs.order_id,
                 fs.line_item,
                 fs.fulfillment_backout_date,
                 fs.backout_sent_ff_date,
                 fs.cancel_not_ff,
                 fs.cleared_ind,
                 fs.bill_yr,
                 fs.offer_id,
                 fs.net_remit_amt,
                 fs.publisher_term,
                 fs.trans_date,
                 CASE
                    WHEN fs.trans_type = 'T'
                    THEN
                       'O'
                    WHEN fs.trans_type = 'U'
                    THEN
                       'R'
                    WHEN     fs.trans_type IN ('O', 'R')
                         AND fs2.trans_type IN ('T', 'U')
                    THEN
                       'T'
                    ELSE
                       fs.trans_type
                 END,
                 1,
                 SYSDATE
            FROM rs_nss_fulfillment_schedule fs,
                 rs_nss_fulfillment_schedule fs2
           WHERE     fs.fulfillment_backout_date > lv_start_date
                 AND fs.fulfillment_backout_date <= lv_end_date
                 AND NVL (fs.cancel_not_ff, 0) <> 1
                 AND fs.order_id = fs2.order_id(+)
                 AND fs.line_item = fs2.line_item(+)
                 AND fs.bill_yr = fs2.bill_yr(+)
                 AND fs2.trans_type(+) IN ('T', 'U'));
   EXCEPTION
      WHEN OTHERS
      THEN
         p_error_message := SQLERRM;
         p_error_id :=
            pkg_error.fn_insert_error_log (pv_application_id_i,
                                           pv_process_name_i,
                                           pv_function_name_i,
                                           '1',
                                           SUBSTR (SQLERRM, 1, 4000),
                                           NULL,
                                           NULL,
                                           NULL);
         raise_application_error (
            -20099,
               '['
            || SUBSTR (p_error_message, 5, 5)
            || '], ['
            || TRIM (p_error_id)
            || '] Step '
            || p_step
            || ': '
            || p_message);
   END prc_ins_st_fulfill_backout;

   -----------------------------------------------------------------
   -- Procedure Name: prc_ins_st_billing_schedule
   --
   -- Created by: Duane Diniz
   --
   -- Objective: This procedure will insert billing schedule records into st_billing_schedule
   --
   --
   -- Modifications:
   -- Date:        Created By:                Description:
   -- ------------ ----------------------- -------------------------
   -- 03/04/2014   Duane Diniz               Created.

   -- 08/27/2015   Chandan Taneja             true_mp_id column addition to billing schedule from card_type
   -- 06/20/2016   Anju Jain                 true_mp_id column to use merchant Id in case of AMEX . Others continue to use true mp Id from card_type
   -- 12/05/2016   Sachin Singh              US11487 - Added token column.
   -- 02/01/2017   Sachin Singh              US11844 - Added loyalty_seq check to get true_mp_id for loyalty cards.
   -- 06/28/2018   RAF                       Added 3 additional fields to support RTB reporting
   -- 05/11/2018   Roopesha Brammachary      Modified for US14131 - Added billing_schedule_id
   -- 10/22/2018   RAF                       Added billing_interval

   -----------------------------------------------------------------
   --

   PROCEDURE prc_ins_st_billing_schedule (
      pv_application_id_i   IN ex_error_log.application_id%TYPE,
      pv_process_name_i     IN ex_error_log.process_name%TYPE,
      pv_function_name_i    IN ex_error_log.function_name%TYPE)
   AS
      lv_start_date   DATE;
      lv_end_date     DATE;
   BEGIN
      --
      p_step := 1;
      p_message := 'Capture start date from TR_ETL_BATCH_CTL table ..';
      DBMS_OUTPUT.put_line (p_message || CHR (10));

      lv_start_date :=
         fn_find_run_date (500,
                           'DWHK002',
                           'fn_find_max_batch_ctl_id',
                           'S',
                           1);

      p_step := 2;
      p_message := 'Capture end date from TR_ETL_BATCH_CTL table ..';
      DBMS_OUTPUT.put_line (p_message || CHR (10));

      lv_end_date :=
         fn_find_run_date (102,
                           'DWHK002',
                           'fn_find_max_batch_ctl_id',
                           'E',
                           1);
      --
      p_step := 3;
      p_message := 'truncate ST_BILLING_SCHEDULE table ..';
      DBMS_OUTPUT.put_line (p_message || CHR (10));

      EXECUTE IMMEDIATE 'truncate table ST_BILLING_SCHEDULE';

      p_step := 4;
      p_message :=
         'Populating staging table with new billing schedule info ..';
      DBMS_OUTPUT.put_line (p_message || CHR (10));

      INSERT INTO st_billing_schedule
          (order_id,
           line_item,
           cust_contract_term,
           charge_no,
           charge_ind,
           trans_type,
           pmt_amt,
           source_create_date,
           sched_billing_date,
           acct_seq,
           loyalty_seq,
           cust_id,
           event_no,
           bill_attempt_no,
           merchant_order_no,
           merchant_id,
           recov_attempt_no,
           recyc_attempt_no,
           source_system_id,
           source_cd,
           true_mp_id,
           token,
           create_date,
           auth_cd,
           external_trans_id,
           associated_charge_no,
           billing_schedule_id,
           billing_interval)
      SELECT
         bs.order_id,
         bs.line_item,
         decode(bs.bill_yr,-1,1,bs.bill_yr),
           nvl(bs.associated_charge_no,bs.charge_no),
           bs.charge_ind,
           bs.trans_type,
           bs.pmt_amt,
           bs.create_date,
           bs.sched_billing_date,
           bs.acct_seq,
           bs.loyalty_seq,
           bs.cust_id,
           bs.event_no,
           bs.bill_attempt_no,
           bs.merchant_order_no,
           bs.merchant_id,
           bs.recov_attempt_no,
           bs.recyc_attempt_no,
           1,
           o.source_cd,
           decode(bs.merchant_id,240,bs.merchant_id,ct.true_mp_id), --true_mp_id
           ans.cc_token,
           sysdate,
           bs.auth_cd,
           bs.external_trans_id,
           bs.associated_charge_no,
           bs.billing_schedule_id,
           bs.billing_interval
       FROM
          rs_nss_billing_schedule bs,
          rs_nss_acct_no_store ans,
          rs_nss_order o,
          RS_NSS_CUSTOMER_ALIAS CA,
          RS_NSS_CARD_TYPE     CT
      WHERE
              bs.create_date >  lv_start_date
          AND bs.create_date <= lv_end_date
          AND bs.acct_seq = ans.acct_seq(+)
          AND bs.cust_id = ans.cust_id(+)
          AND bs.order_id = o.order_id
          AND bs.cust_id=ca.cust_id(+)
          AND NVL(bs.acct_seq, bs.loyalty_seq) =ca.acct_seq(+)
          AND ca.card_type_cd=ct.card_type_cd(+);

   EXCEPTION
      WHEN OTHERS
      THEN
         p_error_message := SQLERRM;
         p_error_id :=
            pkg_error.fn_insert_error_log (pv_application_id_i,
                                           pv_process_name_i,
                                           pv_function_name_i,
                                           '1',
                                           SUBSTR (SQLERRM, 1, 4000),
                                           NULL,
                                           NULL,
                                           NULL);
         raise_application_error (
            -20099,
               '['
            || SUBSTR (p_error_message, 5, 5)
            || '], ['
            || TRIM (p_error_id)
            || '] Step '
            || p_step
            || ': '
            || p_message);
   END prc_ins_st_billing_schedule;


   -----------------------------------------------------------------
   -- Procedure Name: prc_ins_st_billing_submit
   --
   -- Created by: Duane Diniz
   --
   -- Objective: This procedure will insert billing schedule submission records into st_billing_submission
   --
   --
   -- Modifications:
   -- Date:        Created By:                Description:
   -- ------------ ----------------------- -------------------------
   -- 03/04/2014   Duane Diniz               Created.
   -- 08/27/2015   Chandan Taneja            true_mp_id column addition to billing submission from card_type
   -- 06/20/2016   Anju Jain                 true_mp_id column to use merchant Id in case of AMEX . Others continue to use true mp Id from card_type
   -- 02/01/2017   Sachin Singh              US11844 - Added loyalty_seq check to get true_mp_id for loyalty cards.
   -- 06/28/2018   RAF                       Added 3 additional fields to support RTB reporting
   -- 05/11/2018   Roopesha Brammachary      Modified for US14131 - Added billing_schedule_id
   -----------------------------------------------------------------
   --

   PROCEDURE prc_ins_st_billing_submit (
      pv_application_id_i   IN ex_error_log.application_id%TYPE,
      pv_process_name_i     IN ex_error_log.process_name%TYPE,
      pv_function_name_i    IN ex_error_log.function_name%TYPE)
   AS
      lv_start_date   DATE;
      lv_end_date     DATE;
   BEGIN
      --
      p_step := 1;
      p_message := 'Capture start date from TR_ETL_BATCH_CTL table ..';
      DBMS_OUTPUT.put_line (p_message || CHR (10));

      lv_start_date :=
         fn_find_run_date (500,
                           'DWHK002',
                           'fn_find_max_batch_ctl_id',
                           'S',
                           1);

      p_step := 2;
      p_message := 'Capture end date from TR_ETL_BATCH_CTL table ..';
      DBMS_OUTPUT.put_line (p_message || CHR (10));

      lv_end_date :=
         fn_find_run_date (102,
                           'DWHK002',
                           'fn_find_max_batch_ctl_id',
                           'E',
                           1);
      --
      p_step := 3;
      p_message := 'truncate ST_BILLING_SUBMISSION table ..';
      DBMS_OUTPUT.put_line (p_message || CHR (10));

      EXECUTE IMMEDIATE 'truncate table ST_BILLING_SUBMISSION';

      p_step := 4;
      p_message :=
         'Populating staging table with new billing schedule submission info ..';
      DBMS_OUTPUT.put_line (p_message || CHR (10));

      INSERT INTO st_billing_submission
          (order_id,
           line_item,
           cust_contract_term,
           charge_no,
           charge_ind,
           cust_id,
           trans_type,
           submission_date,
           sched_billing_date,
           event_no,
           cond_deposit_action,
           cancel_type_cd,
           cancel_date,
           bill_attempt_no,
           recov_attempt_no,
           recyc_attempt_no,
           pmt_amt,
           source_system_id,
           source_cd,
           true_mp_id,
           token,
           create_date,
           auth_cd,
           external_trans_id,
           associated_charge_no,
           merchant_order_no,
           billing_schedule_id)
      SELECT
         bs.order_id,
         bs.line_item,
         decode(bs.bill_yr,-1,1,bs.bill_yr),
           bs.charge_no,
           bs.charge_ind,
           bs.cust_id,
           bs.trans_type,
           bs.sub_date,
           bs.sched_billing_date,
           bs.event_no,
           nvl(bs.orig_cond_deposit_action,bs.cond_deposit_action),
           bs.cancel_type_cd,
           ol.cancel_date,
           bs.bill_attempt_no,
           bs.recov_attempt_no,
           bs.recyc_attempt_no,
           bs.pmt_amt,
           1,
           o.source_cd,
           decode(bs.merchant_id,240,bs.merchant_id,ct.true_mp_id),
           ans.cc_token,
           sysdate,
           bs.auth_cd,
           bs.external_trans_id,
           bs.associated_charge_no,
           bs.merchant_order_no,
           bs.billing_schedule_id
       FROM
          rs_nss_billing_schedule bs,
          rs_nss_acct_no_store ans,
          rs_nss_order_line ol,
          rs_nss_order o,
          RS_NSS_CUSTOMER_ALIAS CA,
          RS_NSS_CARD_TYPE     CT
      WHERE
                    bs.sub_date >  lv_start_date
          AND bs.sub_date <= lv_end_date
          AND bs.acct_seq = ans.acct_seq(+)
          AND bs.cust_id = ans.cust_id(+)
          AND bs.order_id = o.order_id
          AND bs.order_id = ol.order_id (+)
          AND bs.line_item = ol.line_item (+)
          AND bs.cust_id=ca.cust_id(+)
          AND NVL(bs.acct_seq, bs.loyalty_seq) =ca.acct_seq(+)
          AND ca.card_type_cd=ct.card_type_cd(+);

   EXCEPTION
      WHEN OTHERS
      THEN
         p_error_message := SQLERRM;
         p_error_id :=
            pkg_error.fn_insert_error_log (pv_application_id_i,
                                           pv_process_name_i,
                                           pv_function_name_i,
                                           '1',
                                           SUBSTR (SQLERRM, 1, 4000),
                                           NULL,
                                           NULL,
                                           NULL);
         raise_application_error (
            -20099,
               '['
            || SUBSTR (p_error_message, 5, 5)
            || '], ['
            || TRIM (p_error_id)
            || '] Step '
            || p_step
            || ': '
            || p_message);
   END prc_ins_st_billing_submit;

   -----------------------------------------------------------------
   -- Procedure Name: prc_ins_st_billing_response
   --
   -- Created by: Duane Diniz
   --
   -- Objective: This procedure will insert billing schedule response records into st_billing_submission
   --
   --
   -- Modifications:
   -- Date:        Created By:                Description:
   -- ------------ ----------------------- -------------------------

   -- 03/04/2014   Duane Diniz             Created.
   -- 08/27/2014   Chandan Taneja          true_mp_id column addition to billing schedule from card_type
   -- 06/20/2016   Anju Jain               true_mp_id column to use merchant Id in case of AMEX . Others continue to use true mp Id from card_type
   -- 09/06/2016   Jonathan Smith          Change logic for Declines to deal with Final HDs only - US 10821
   -- 08/26/2016   Jonathan Smith          US11189 - Add order request order and line
   -- 02/01/2017   Sachin Singh            US11844 - Added loyalty_seq check to get true_mp_id for loyalty cards.
   -- 05/22/2018   Brian Wolf              US13454 - Adding MP_TRANS_ID and MAPS_MERCHANT_ORDER_NO
   -- 06/28/2018   RAF                     Added 3 additional fields to support RTB reporting
   -- 05/11/2018   Roopesha Brammachary    Modified for US14131 - Added billing_schedule_id
   -----------------------------------------------------------------
   --

   PROCEDURE prc_ins_st_billing_response (
      pv_application_id_i   IN ex_error_log.application_id%TYPE,
      pv_process_name_i     IN ex_error_log.process_name%TYPE,
      pv_function_name_i    IN ex_error_log.function_name%TYPE)
   AS
      lv_start_date   DATE;
      lv_end_date     DATE;
   BEGIN
      --
      p_step := 1;
      p_message := 'Capture start date from TR_ETL_BATCH_CTL table ..';
      DBMS_OUTPUT.put_line (p_message || CHR (10));

      lv_start_date :=
         fn_find_run_date (500,
                           'DWHK002',
                           'fn_find_max_batch_ctl_id',
                           'S',
                           1);

      p_step := 2;
      p_message := 'Capture end date from TR_ETL_BATCH_CTL table ..';
      DBMS_OUTPUT.put_line (p_message || CHR (10));

      lv_end_date :=
         fn_find_run_date (102,
                           'DWHK002',
                           'fn_find_max_batch_ctl_id',
                           'E',
                           1);
      --
      p_step := 3;
      p_message := 'truncate ST_BILLING_RESPONSE table ..';
      DBMS_OUTPUT.put_line (p_message || CHR (10));

      EXECUTE IMMEDIATE 'truncate table ST_BILLING_RESPONSE';

      p_step := 4;
      p_message :=
         'Populating staging table with new billing schedule response info ..';
      DBMS_OUTPUT.put_line (p_message || CHR (10));

      INSERT INTO st_billing_response
          (order_id,
           line_item,
           cust_contract_term,
           charge_no,
           charge_ind,
           cust_id,
           trans_type,
           sched_billing_date,
           event_no,
           cond_deposit_action,
           deposit_flag,
           advice_cd,
           act_merchant_cd,
           appr_status_cd,
           appr_status_date,
           pre_edit_act_merchant_cd,
           pre_edit_appr_status_cd,
           pre_edit_appr_status_date,
           bill_attempt_no,
           recov_attempt_no,
           recyc_attempt_no,
           pmt_amt,
           source_system_id,
           source_cd,
           true_mp_id,
           mp_trans_id,
           maps_merchant_order_no,
           order_request_order_id,
           order_request_line_item,
           token,
           create_date,
           auth_cd,
           external_trans_id,
           associated_charge_no,
           merchant_order_no,
           billing_schedule_id)
      SELECT
         bs.order_id,
         bs.line_item,
         decode(bs.bill_yr,-1,1,bs.bill_yr),
           bs.charge_no,
           bs.charge_ind,
           bs.cust_id,
           bs.trans_type,
           bs.sched_billing_date,
           bs.event_no,
           bs.cond_deposit_action,
           bs.deposit_flag,
           bs.advice_cd,
           bs.act_merchant_cd,
           -- US 10821 - Need to have a different appr_status_cd for Interim HDs to re-class
         CASE
            WHEN bs.trans_type IN('CHARGE','PAUTH','REFUND') THEN
                decode(bs.pre_edit_appr_status_cd,null,DECODE(bs.appr_status_cd, 'HD', 'IHD', bs.appr_status_cd),'CB')
            ELSE
                decode(bs.pre_edit_appr_status_cd,null,bs.appr_status_cd,'CB')
            END,
         bs.appr_status_date,
           bs.pre_edit_act_merchant_cd,
           bs.pre_edit_appr_status_cd,
           bs.pre_edit_appr_status_date,
           bs.bill_attempt_no,
           bs.recov_attempt_no,
           bs.recyc_attempt_no,
           bs.pmt_amt,
           1,
           o.source_cd,
         decode(bs.merchant_id,240,bs.merchant_id,ct.true_mp_id),
         bs.mp_trans_id,
         FN_GET_MAPS_MERCHANT_ORDER_NO(bs.mp_trans_id, bs.bill_attempt_no,500,'pkg_etl_news.prc_ins_st_billing_response','FN_GET_MAPS_MERCHANT_ORDER_NO'), --MAPS_MERCHANT_ORDER_NO
         -- US11189
         oe.dm_order_id,
         ole.dm_line_item,
         ans.cc_token,
         sysdate,
         bs.auth_cd,
         bs.external_trans_id,
         bs.associated_charge_no,
         bs.merchant_order_no,
         bs.billing_schedule_id
       FROM
          rs_nss_billing_schedule bs,
          rs_nss_acct_no_store ans,
          rs_nss_order o,
          RS_NSS_CUSTOMER_ALIAS CA,
          RS_NSS_CARD_TYPE     CT,
          rs_nss_order_extension oe,
          rs_nss_order_line_extension ole
      WHERE
              bs.appr_status_date >  lv_start_date
          AND bs.appr_status_date <= lv_end_date
          AND bs.acct_seq = ans.acct_seq(+)
          AND bs.cust_id = ans.cust_id(+)
          AND bs.order_id = o.order_id
          AND bs.cust_id=ca.cust_id(+)
          AND NVL(bs.acct_seq, bs.loyalty_seq) =ca.acct_seq(+)
          AND ca.card_type_cd=ct.card_type_cd(+)
          AND NOT (
                bs.appr_status_cd = 'HD'
            AND bs.trans_type IN('CHARGE','PAUTH','REFUND')
            AND bs.hd_final_date IS NOT NULL
            )-- US 10821 - exclude the final HDs
          AND o.order_id = oe.order_id(+)
          AND bs.order_id = ole.order_id(+)
          AND bs.line_item = ole.line_item(+)
          ;

      p_step := 5;
      p_message :=
         'Populating staging table with new FINAL HD billing schedule response info ..';
      DBMS_OUTPUT.put_line (p_message || CHR (10));

      INSERT INTO st_billing_response
          (order_id,
           line_item,
           cust_contract_term,
           charge_no,
           charge_ind,
           cust_id,
           trans_type,
           sched_billing_date,
           event_no,
           cond_deposit_action,
           deposit_flag,
           advice_cd,
           act_merchant_cd,
           appr_status_cd,
           appr_status_date,
           pre_edit_act_merchant_cd,
           pre_edit_appr_status_cd,
           pre_edit_appr_status_date,
           bill_attempt_no,
           recov_attempt_no,
           recyc_attempt_no,
           pmt_amt,
           source_system_id,
           source_cd,
           true_mp_id,
         order_request_order_id,
         order_request_line_item,
         mp_trans_id,
         maps_merchant_order_no,
         token,
         create_date,
         auth_cd,
         external_trans_id,
         associated_charge_no,
         merchant_order_no,
         billing_schedule_id)
      SELECT
         bs.order_id,
         bs.line_item,
         decode(bs.bill_yr,-1,1,bs.bill_yr),
           bs.charge_no,
           bs.charge_ind,
           bs.cust_id,
           bs.trans_type,
           bs.sched_billing_date,
           bs.event_no,
           bs.cond_deposit_action,
           bs.deposit_flag,
           bs.advice_cd,
           bs.act_merchant_cd,
         bs.appr_status_cd,
           bs.hd_final_date, -- US 10821
           bs.pre_edit_act_merchant_cd,
           bs.pre_edit_appr_status_cd,
           bs.pre_edit_appr_status_date,
           bs.bill_attempt_no,
           bs.recov_attempt_no,
           bs.recyc_attempt_no,
           bs.pmt_amt,
           1,
           o.source_cd,
           decode(bs.merchant_id,240,bs.merchant_id,ct.true_mp_id),
         -- US11189
         oe.dm_order_id,
         ole.dm_line_item,
         bs.mp_trans_id,
         FN_GET_MAPS_MERCHANT_ORDER_NO(bs.mp_trans_id, bs.bill_attempt_no,500,'pkg_etl_news.prc_ins_st_billing_response','FN_GET_MAPS_MERCHANT_ORDER_NO'), --MAPS_MERCHANT_ORDER_NO
         ans.cc_token,
         sysdate,
         bs.auth_cd,
         bs.external_trans_id,
         bs.associated_charge_no,
         bs.merchant_order_no,
         bs.billing_schedule_id
       FROM
          rs_nss_billing_schedule bs,
          rs_nss_acct_no_store ans,
          rs_nss_order o,
          RS_NSS_CUSTOMER_ALIAS CA,
          RS_NSS_CARD_TYPE     CT,
          rs_nss_order_extension oe,
          rs_nss_order_line_extension ole
      WHERE
              bs.hd_final_date >  lv_start_date -- Only final HDs - US 10821
          AND bs.hd_final_date <= lv_end_date -- Only final HDs - US 10821
          AND bs.acct_seq = ans.acct_seq(+)
          AND bs.cust_id = ans.cust_id(+)
          AND bs.order_id = o.order_id
          AND bs.cust_id=ca.cust_id(+)
          AND NVL(bs.acct_seq, bs.loyalty_seq) =ca.acct_seq(+)
          AND ca.card_type_cd=ct.card_type_cd(+)
          AND bs.appr_status_cd = 'HD' -- HDs with hd_final_date not null (date range above) - US 10821
          AND bs.trans_type IN('CHARGE','PAUTH','REFUND')
          -- US 10821 - exclude the final HDs
          AND o.order_id = oe.order_id(+)
          AND bs.order_id = ole.order_id(+)
          AND bs.line_item = ole.line_item(+)
          ;

   EXCEPTION
      WHEN OTHERS
      THEN
         p_error_message := SQLERRM;
         p_error_id :=
            pkg_error.fn_insert_error_log (pv_application_id_i,
                                           pv_process_name_i,
                                           pv_function_name_i,
                                           '1',
                                           SUBSTR (SQLERRM, 1, 4000),
                                           NULL,
                                           NULL,
                                           NULL);
         raise_application_error (
            -20099,
               '['
            || SUBSTR (p_error_message, 5, 5)
            || '], ['
            || TRIM (p_error_id)
            || '] Step '
            || p_step
            || ': '
            || p_message);
   END prc_ins_st_billing_response;

   -----------------------------------------------------------------
   -- Procedure Name: prc_ins_st_linked_item
   --
   -- Created by : Krishan Kumar Tripathi
   --
   -- Objective: This procedure will insert linked_item records into st_linked_item
   --
   --
   -- Modifications:
   -- Date:        Created By:                Description:
   -- ------------ ----------------------- -------------------------
   -- 01/09/2015   KTripathi                           Created.
   -----------------------------------------------------------------
   --

   PROCEDURE prc_ins_st_linked_item (
      pv_application_id_i   IN ex_error_log.application_id%TYPE,
      pv_process_name_i     IN ex_error_log.process_name%TYPE,
      pv_function_name_i    IN ex_error_log.function_name%TYPE)
   AS
      lv_start_date   DATE;
      lv_end_date     DATE;
   BEGIN
      --
      p_step := 1;
      p_message := 'Capture start date from TR_ETL_BATCH_CTL table ..';
      DBMS_OUTPUT.put_line (p_message || CHR (10));

      lv_start_date :=
         fn_find_run_date (500,
                           'DWHK002',
                           'fn_find_max_batch_ctl_id',
                           'S',
                           1);

      p_step := 2;
      p_message := 'Capture end date from TR_ETL_BATCH_CTL table ..';
      DBMS_OUTPUT.put_line (p_message || CHR (10));

      lv_end_date :=
         fn_find_run_date (102,
                           'DWHK002',
                           'fn_find_max_batch_ctl_id',
                           'E',
                           1);
      --
      p_step := 3;
      p_message := 'truncate ST_LINKED_ITEM table ..';
      DBMS_OUTPUT.put_line (p_message || CHR (10));

      EXECUTE IMMEDIATE 'truncate table st_linked_items';

      p_step := 4;
      p_message := 'Populating staging table with linked item info ..';
      DBMS_OUTPUT.put_line (p_message || CHR (10));

      INSERT INTO st_linked_items (ORIG_ORDER_ID,
                                   ORIG_LINE_ITEM,
                                   DELAYED_ORDER_ID,
                                   DELAYED_LINE_ITEM,
                                   LINKED_DATE,
                                   ACT_CUST_RDNC_FEE,
                                   cust_contract_term,
                                   CANCEL_TYPE_CD)
         SELECT ORIG_ORDER_ID,
                ORIG_LINE_ITEM,
                DELAYED_ORDER_ID,
                DELAYED_LINE_ITEM,
                LINKED_DATE,
                ACT_CUST_RDNC_FEE,
                BILL_YR,
                CANCEL_TYPE_CD
           FROM DWHSTG.RS_LINKED_ITEMS
          WHERE LINKED_DATE > lv_start_date AND LINKED_DATE <= lv_end_date;
   EXCEPTION
      WHEN OTHERS
      THEN
         p_error_message := SQLERRM;
         p_error_id :=
            pkg_error.fn_insert_error_log (pv_application_id_i,
                                           pv_process_name_i,
                                           pv_function_name_i,
                                           '1',
                                           SUBSTR (SQLERRM, 1, 4000),
                                           NULL,
                                           NULL,
                                           NULL);
         raise_application_error (
            -20099,
               '['
            || SUBSTR (p_error_message, 5, 5)
            || '], ['
            || TRIM (p_error_id)
            || '] Step '
            || p_step
            || ': '
            || p_message);
   END prc_ins_st_linked_item;

   -----------------------------------------------------------------
   -- Procedure Name: prc_ins_st_jit_offer_event
   --
   -- Created by: Krishan Kumar Tripathi
   --
   -- Objective: This procedure will insert jit_offer_event records into st_jit_offer_event
   -- Modifications:
   -- Date:        Created By:                Description:
   -- ------------ ----------------------- -------------------------
   -- 01/09/2015   KTripathi                        Created.
   -----------------------------------------------------------------

   PROCEDURE prc_ins_st_jit_offer_event (
      pv_application_id_i   IN ex_error_log.application_id%TYPE,
      pv_process_name_i     IN ex_error_log.process_name%TYPE,
      pv_function_name_i    IN ex_error_log.function_name%TYPE)
   AS
      lv_start_date   DATE;
      lv_end_date     DATE;
   BEGIN
      --
      p_step := 1;
      p_message := 'Capture start date from TR_ETL_BATCH_CTL table ..';
      DBMS_OUTPUT.put_line (p_message || CHR (10));

      lv_start_date :=
         fn_find_run_date (500,
                           'DWHK003',
                           'fn_find_max_batch_ctl_id',
                           'S',
                           1);

      p_step := 2;
      p_message := 'Capture end date from TR_ETL_BATCH_CTL table ..';
      DBMS_OUTPUT.put_line (p_message || CHR (10));

      lv_end_date :=
         fn_find_run_date (102,
                           'DWHK003',
                           'fn_find_max_batch_ctl_id',
                           'E',
                           1);
      --
      p_step := 3;
      p_message := 'truncate st_jit_offer_event table ..';
      DBMS_OUTPUT.put_line (p_message || CHR (10));

      EXECUTE IMMEDIATE 'truncate table st_jit_offer_event';

      p_step := 4;
      p_message := 'Populating staging table with jit offer info ..';
      DBMS_OUTPUT.put_line (p_message || CHR (10));

      INSERT INTO st_jit_offer_event (ORDER_ID,
                                      LINE_ITEM,
                                      cust_contract_term,
                                      ORIG_OFFER_ID,
                                      NEW_OFFER_ID,
                                      ACTION_DATE)
         SELECT ORDER_ID,
                LINE_ITEM,
                BILL_YR,
                ORIG_OFFER_ID,
                NEW_OFFER_ID,
                ACTION_DATE
           FROM DWHSTG.RS_JIT_OFFER_EVENT
          WHERE ACTION_DATE > lv_start_date AND ACTION_DATE <= lv_end_date;
   EXCEPTION
      WHEN OTHERS
      THEN
         p_error_message := SQLERRM;
         p_error_id :=
            pkg_error.fn_insert_error_log (pv_application_id_i,
                                           pv_process_name_i,
                                           pv_function_name_i,
                                           '1',
                                           SUBSTR (SQLERRM, 1, 4000),
                                           NULL,
                                           NULL,
                                           NULL);
         raise_application_error (
            -20099,
               '['
            || SUBSTR (p_error_message, 5, 5)
            || '], ['
            || TRIM (p_error_id)
            || '] Step '
            || p_step
            || ': '
            || p_message);
   END prc_ins_st_jit_offer_event;

   -----------------------------------------------------------------
   -- Procedure Name: prc_ins_st_billing_cancel
   --
   -- Created by: Krishan Kumar Tripathi
   --
   -- Objective: This procedure will insert billing cancel submission  records into st_billing_cancel
   --
   --
   -- Modifications:
   -- Date:        Created By:             Description:
   -- ------------ ----------------------- -------------------------
   -- 01/28/2015   KTripathi               Created.
   -- 08/27/2014   Chandan Taneja           true_mp_id column addition to billing schedule from card_type
   -- 06/20/2016   Anju Jain               true_mp_id column to use merchant Id in case of AMEX . Others continue to use true mp Id from card_type
   -- 08/26/2016   Jonathan Smith          US11188 - Add order request order and line
   -- 02/01/2017   Sachin Singh            US11844 - Added loyalty_seq check to get true_mp_id for loyalty cards.
   -- 05/22/2018   Brian Wolf              US13454 - Adding MP_TRANS_ID and MAPS_MERCHANT_ORDER_NO
   -- 06/28/2018   RAF                     Added 3 additional fields to support RTB reporting
   -- 05/11/2018   Roopesha Brammachary    Modified for US14131 - Added billing_schedule_id
   -- 09/11/2018   Steve Pratt             added billing_schedule_date from NSS to insert stage table for DWH load.
   -----------------------------------------------------------------
   --

   PROCEDURE prc_ins_st_billing_cancel (
      pv_application_id_i   IN ex_error_log.application_id%TYPE,
      pv_process_name_i     IN ex_error_log.process_name%TYPE,
      pv_function_name_i    IN ex_error_log.function_name%TYPE)
   AS
      lv_start_date   DATE;
      lv_end_date     DATE;
   BEGIN
      --
      p_step := 1;
      p_message := 'Capture start date from TR_ETL_BATCH_CTL table ..';
      DBMS_OUTPUT.put_line (p_message || CHR (10));

      lv_start_date :=
         fn_find_run_date (500,
                           'DWHK021',
                           'fn_find_max_batch_ctl_id',
                           'S',
                           1);

      p_step := 2;
      p_message := 'Capture end date from TR_ETL_BATCH_CTL table ..';
      DBMS_OUTPUT.put_line (p_message || CHR (10));

      lv_end_date :=
         fn_find_run_date (102,
                           'DWHK021',
                           'fn_find_max_batch_ctl_id',
                           'E',
                           1);
      --
      p_step := 3;
      p_message := 'truncate ST_BILLING_CANCEL table ..';
      DBMS_OUTPUT.put_line (p_message || CHR (10));

      EXECUTE IMMEDIATE 'truncate table ST_BILLING_CANCEL';

      p_step := 4;
      p_message :=
         'Populating staging table with new billing cancellation submissions info ..';
      DBMS_OUTPUT.put_line (p_message || CHR (10));

      INSERT INTO st_billing_cancel
          (order_id,
           line_item,
           cust_contract_term,
           charge_no,
           charge_ind,
           cust_id,
           trans_type,
           event_no,
           cancel_type_cd,
           bill_cancel_date,
           pmt_amt,
           source_system_id,
           source_cd,
           true_mp_id,
           mp_trans_id,
           maps_merchant_order_no,
           order_request_order_id,
           order_request_line_item,
           token,
           create_date,
           auth_cd,
           external_trans_id,
           associated_charge_no,
           merchant_order_no,
           billing_schedule_id,
           sched_billing_date)                          --SJP   US14743   Added sched_billing _date to be carried over in stage.
      SELECT
         bs.order_id,
         decode(bs.line_item,-1,Null,bs.line_item),
         decode(bs.bill_yr,-1,1,bs.bill_yr),
           bs.charge_no,
           bs.charge_ind,
           bs.cust_id,
           bs.trans_type,
           bs.event_no,
           bs.cancel_type_cd,
           bs.bill_cancel_date,
           bs.pmt_amt,
           1,
           o.source_cd,
           decode(bs.merchant_id,240,bs.merchant_id,ct.true_mp_id),
           bs.mp_trans_id,
           FN_GET_MAPS_MERCHANT_ORDER_NO(bs.mp_trans_id, bs.bill_attempt_no,500,'pkg_etl_news.prc_ins_st_billing_response','FN_GET_MAPS_MERCHANT_ORDER_NO'), --MAPS_MERCHANT_ORDER_NO
         -- US11188
           oe.dm_order_id,
           ole.dm_line_item,
           ans.cc_token,
           sysdate,
           bs.auth_cd,
           bs.external_trans_id,
           bs.associated_charge_no,
           bs.merchant_order_no,
           bs.billing_schedule_id,
           bs.sched_billing_date
       FROM
          rs_nss_billing_schedule bs,
          rs_nss_acct_no_store ans,
          rs_nss_order o,
          RS_NSS_CUSTOMER_ALIAS CA,
          RS_NSS_CARD_TYPE     CT,
          rs_nss_order_extension oe,
          rs_nss_order_line_extension ole
       WHERE  bs.bill_cancel_date > lv_start_date
          AND bs.bill_cancel_date <= lv_end_date
          AND bs.acct_seq = ans.acct_seq(+)
          AND bs.cust_id = ans.cust_id(+)
          AND bs.order_id = o.order_id
          AND bs.cust_id=ca.cust_id(+)
          AND NVL(bs.acct_seq, bs.loyalty_seq) =ca.acct_seq(+)
          AND ca.card_type_cd=ct.card_type_cd(+)
          AND bs.order_id = oe.order_id(+)
          AND bs.order_id = ole.order_id(+)
          AND bs.line_item = ole.line_item(+)
          ;

   EXCEPTION
      WHEN OTHERS
      THEN
         p_error_message := SQLERRM;
         p_error_id :=
            pkg_error.fn_insert_error_log (pv_application_id_i,
                                           pv_process_name_i,
                                           pv_function_name_i,
                                           '1',
                                           SUBSTR (SQLERRM, 1, 4000),
                                           NULL,
                                           NULL,
                                           NULL);
         raise_application_error (
            -20099,
               '['
            || SUBSTR (p_error_message, 5, 5)
            || '], ['
            || TRIM (p_error_id)
            || '] Step '
            || p_step
            || ': '
            || p_message);
   END prc_ins_st_billing_cancel;

-----------------------------------------------------------------
   -- Procedure Name: prc_ins_st_cust_bill_address
   --
   -- Created by: Krishan Kumar Tripathi
   --
   -- Objective: This function will extract the billing address details from NEWS
   --
   --
   -- Modifications:
   -- Date:        Created By:                Description:
   -- ------------ ----------------------- -------------------------
   -- 11/21/2016   Krishan Kumar Tripathi       Created.
   -----------------------------------------------------------------
   --

   PROCEDURE prc_ins_st_cust_bill_address (
      pv_application_id_i   IN ex_error_log.application_id%TYPE,
      pv_process_name_i     IN ex_error_log.process_name%TYPE,
      pv_function_name_i    IN ex_error_log.function_name%TYPE)
   AS
      lv_start_date   DATE;
      lv_end_date     DATE;
   BEGIN
      --
      p_step := 1;
      p_message := 'Capture start date from TR_ETL_BATCH_CTL table ..';
      DBMS_OUTPUT.put_line (p_message || CHR (10));

      lv_start_date :=
         pkg_st_news_etl.fn_find_run_date (500,
                                           'dwhk002',
                                           'fn_find_max_batch_ctl_id',
                                           'S',
                                           1);

      p_step := 2;
      p_message := 'Capture end date from TR_ETL_BATCH_CTL table ..';
      DBMS_OUTPUT.put_line (p_message || CHR (10));

      lv_end_date :=
         pkg_st_news_etl.fn_find_run_date (102,
                                           'dwhk002',
                                           'fn_find_max_batch_ctl_id',
                                           'E',
                                           1);
      --
      p_step := 3;
      p_message := 'truncate  ST_CUST_BILL_ADDRESS table ..';
      DBMS_OUTPUT.put_line (p_message || CHR (10));

      EXECUTE IMMEDIATE 'truncate table ST_CUST_BILL_ADDRESS';

      p_step := 4;
      p_message :=
         'Populating staging table with customer BILLING ADDRESS info ..';
      DBMS_OUTPUT.put_line (p_message || CHR (10));

      INSERT INTO ST_CUST_BILL_ADDRESS (SOURCE_BILL_ADDR_ID,
                                        SOURCE_ADDRESS_ID,
                                        SOURCE_CUST_ID,
                                        CREATED_DATE,
                                        CREATED_BY,
                                        SOURCE_SYSTEM_ID,
                                        order_id,
                                        source_created_date)
         SELECT BILL_ADDRESS_ID,
                ADDRESS_ID,
                cust_id,
                SYSDATE,
                USER,
                1,
                order_id,
                created_date
           FROM RS_BILL_ADDRESS ra
          WHERE     ra.created_date > lv_start_date
                AND ra.created_date <= lv_end_date;
   EXCEPTION
      WHEN OTHERS
      THEN
         p_error_message := SQLERRM;
         p_error_id :=
            pkg_error.fn_insert_error_log (pv_application_id_i,
                                           pv_process_name_i,
                                           pv_function_name_i,
                                           '1',
                                           SUBSTR (SQLERRM, 1, 4000),
                                           NULL,
                                           NULL,
                                           NULL);
         raise_application_error (
            -20099,
               '['
            || SUBSTR (p_error_message, 5, 5)
            || '], ['
            || TRIM (p_error_id)
            || '] Step '
            || p_step
            || ': '
            || p_message);
   END prc_ins_st_cust_bill_address;

   -----------------------------------------------------------------
   -- Procedure Name: prc_ins_st_cust_ship_address
   --
   -- Created by: Krishan Kumar Tripathi
   --
   -- Objective: This function will extract the billing address details from NEWS
   --
   --
   -- Modifications:
   -- Date:        Created By:                Description:
   -- ------------ ----------------------- -------------------------
   -- 11/21/2016   Krishan Kumar Tripathi       Created.
   --02/24/2017    Krishan Kumar Triapthi       US11328 :order_id and line_item picked from tr_ship_address to map tr_order_line_id in DWH insert
   -----------------------------------------------------------------
   --

   PROCEDURE prc_ins_st_cust_ship_address (
      pv_application_id_i   IN ex_error_log.application_id%TYPE,
      pv_process_name_i     IN ex_error_log.process_name%TYPE,
      pv_function_name_i    IN ex_error_log.function_name%TYPE)
   AS
      lv_start_date   DATE;
      lv_end_date     DATE;
   BEGIN
      --
      p_step := 1;
      p_message := 'Capture start date from TR_ETL_BATCH_CTL table ..';
      DBMS_OUTPUT.put_line (p_message || CHR (10));

      lv_start_date :=
         pkg_st_news_etl.fn_find_run_date (500,
                                           'dwhk002',
                                           'fn_find_max_batch_ctl_id',
                                           'S',
                                           1);

      p_step := 2;
      p_message := 'Capture end date from TR_ETL_BATCH_CTL table ..';
      DBMS_OUTPUT.put_line (p_message || CHR (10));

      lv_end_date :=
         pkg_st_news_etl.fn_find_run_date (102,
                                           'dwhk002',
                                           'fn_find_max_batch_ctl_id',
                                           'E',
                                           1);
      --
      p_step := 3;
      p_message := 'truncate  ST_CUST_SHIP_ADDRESS table ..';
      DBMS_OUTPUT.put_line (p_message || CHR (10));

      EXECUTE IMMEDIATE 'truncate table ST_CUST_SHIP_ADDRESS';

      p_step := 4;
      p_message :=
         'Populating staging table with customer SHIPPING ADDRESS info ..';
      DBMS_OUTPUT.put_line (p_message || CHR (10));

      INSERT INTO ST_CUST_SHIP_ADDRESS (SOURCE_SHIP_ADDR_ID,
                                        SOURCE_ADDRESS_ID,
                                        SOURCE_CUST_ID,
                                        CREATED_DATE,
                                        CREATED_BY,
                                        SOURCE_SYSTEM_ID,
                                        order_id,
                                        line_item,
                                        source_created_date)
         SELECT SHIP_ADDRESS_ID,
                ADDRESS_ID,
                CUST_ID,
                SYSDATE,
                USER,
                1,
                order_id,
                line_item,
                created_date
           FROM RS_SHIP_ADDRESS ra
          WHERE     ra.created_date > lv_start_date
                AND ra.created_date <= lv_end_date;
   EXCEPTION
      WHEN OTHERS
      THEN
         p_error_message := SQLERRM;
         p_error_id :=
            pkg_error.fn_insert_error_log (pv_application_id_i,
                                           pv_process_name_i,
                                           pv_function_name_i,
                                           '1',
                                           SUBSTR (SQLERRM, 1, 4000),
                                           NULL,
                                           NULL,
                                           NULL);
         raise_application_error (
            -20099,
               '['
            || SUBSTR (p_error_message, 5, 5)
            || '], ['
            || TRIM (p_error_id)
            || '] Step '
            || p_step
            || ': '
            || p_message);
   END prc_ins_st_cust_ship_address;

   -----------------------------------------------------------------
   -- Procedure Name: prc_ins_st_customer
   --
   -- Created by: Krishan Kumar Tripathi
   --
   -- Objective: This function will extract the address details from NEWS tr_address table
   --
   --
   -- Modifications:
   -- Date:        Created By:                Description:
   -- ------------ ----------------------- -------------------------
   -- 11/21/2016   Krishan Kumar Tripathi       Created.
  --02/24/2017    Krishan Kumar Tripathi       US11328 : (a)Address_change_date population logic added.(b)Hard coding of action code to identify
   --                                                       email change modified with a lookup to a REF table (c) Customer address pull modified to be dependent
   --                                                       on daily billing/shipping transactions rather than create date of tr_address table.
   --                                                       The reasonbeing there are certain transactiosn where the new billing/shipping address points to the same old tr_address
   --                                                       records and hence the address records is not picked as it does not lie in the batch run date. This results in a missing insert in tr_csut_ship_address and tr_cust_bill_address table as these are populated
   --                                                       using the staging st_customer table join to the tr_customer table which fails because of no address records in
   --                                                        the st_customer table for that date range.
   -----------------------------------------------------------------
   --

   PROCEDURE prc_ins_st_customer (
      pv_application_id_i   IN ex_error_log.application_id%TYPE,
      pv_process_name_i     IN ex_error_log.process_name%TYPE,
      pv_function_name_i    IN ex_error_log.function_name%TYPE)
   AS
      lv_start_date   DATE;
      lv_end_date     DATE;
   BEGIN
      --
      p_step := 1;
      p_message := 'Capture start date from TR_ETL_BATCH_CTL table ..';
      DBMS_OUTPUT.put_line (p_message || CHR (10));

      lv_start_date :=
         pkg_st_news_etl.fn_find_run_date (500,
                                           'dwhk002',
                                           'fn_find_max_batch_ctl_id',
                                           'S',
                                           1);

      p_step := 2;
      p_message := 'Capture end date from TR_ETL_BATCH_CTL table ..';
      DBMS_OUTPUT.put_line (p_message || CHR (10));

      lv_end_date :=
         pkg_st_news_etl.fn_find_run_date (102,
                                           'dwhk002',
                                           'fn_find_max_batch_ctl_id',
                                           'E',
                                           1);
      --
      p_step := 3;
      p_message := 'truncate  ST_CUSTOMER table ..';
      DBMS_OUTPUT.put_line (p_message || CHR (10));

      EXECUTE IMMEDIATE 'truncate table st_CUSTOMER';

      p_step := 4;
      p_message := 'Populating staging table with customer ADDRESS info ..';
      DBMS_OUTPUT.put_line (p_message || CHR (10));


      INSERT INTO ST_CUSTOMER (FIRST_NAME,
                               MIDDLE_NAME,
                               LAST_NAME,
                               ADDRESS_LINE_ONE,
                               ADDRESS_LINE_TWO,
                               CITY,
                               STATE,
                               ZIP_CODE,
                               TITLE,
                               SUFFIX,
                               CREATED_DATE,
                               CREATED_BY,
                               SOURCE_ADDRESS_ID,
                               SOURCE_SYSTEM_ID,
                               EMAIL_ADDRESS,
                               GENDER,
                               SOURCE_CUST_ID,
                               EMAIL_CHANGE_DATE,
                               SOURCE_CREATED_DATE)
         SELECT a.first_name,
                a.middle_name,
                a.last_name,
                a.addr_line_one,
                a.addr_line_two,
                a.city,
                a.st,
                a.zip,
                a.title,
                a.suffix,
                SYSDATE,
                USER,
          a.address_id,
          1,
          TRIM (c.email_addr),
          c.gender,
          c.cust_id,
          NULL,
          A.CREATED_DATE
     FROM rs_address a,
          rs_customer c,
          (SELECT source_cust_id,source_address_id FROM st_cust_bill_address
           UNION
           SELECT source_cust_id, source_address_id FROM st_cust_ship_address) b
    WHERE     a.address_id = b.source_address_id
          AND b.source_cust_id = c.cust_id
         UNION ALL
   SELECT NULL,
          NULL,
          NULL,
          NULL,
          NULL,
          NULL,
          NULL,
          NULL,
          NULL,
          NULL,
          SYSDATE,
          USER,
          NULL,
          1,
          TRIM (b.email_addr),
          NULL,
          a.cust_id,
          a.action_date,
          A.CREATED_DATE
     FROM rs_customer_activity a, rs_customer b, LS_CF_CUST_ACTION_TYPE E
    WHERE     a.cust_id = b.cust_id
          AND a.action_cd = e.action_cd
          AND a.action_date > lv_start_date
          AND a.action_date <= lv_end_date;
   EXCEPTION
      WHEN OTHERS
      THEN
         p_error_message := SQLERRM;
         p_error_id :=
            pkg_error.fn_insert_error_log (pv_application_id_i,
                                           pv_process_name_i,
                                           pv_function_name_i,
                                           '1',
                                           SUBSTR (SQLERRM, 1, 4000),
                                           NULL,
                                           NULL,
                                           NULL);
         raise_application_error (
            -20099,
               '['
            || SUBSTR (p_error_message, 5, 5)
            || '], ['
            || TRIM (p_error_id)
            || '] Step '
            || p_step
            || ': '
            || p_message);
   END prc_ins_st_customer;

   -----------------------------------------------------------------
   -- Procedure Name: prc_ins_st_customer_activity
   --
   -- Created by: Sachin Singh
   --
   -- Objective: This procedure will insert nss.customer_activity records into st_customer_activity table using source_create_date.
   --
   --
   -- Modifications:
   -- Date:        Created By:                Description:
   -- ------------ ----------------------- -------------------------
   -- 01/20/2017   Sachins                   Created for US8736.
   -----------------------------------------------------------------

   PROCEDURE prc_ins_st_customer_activity (
      pv_application_id_i   IN ex_error_log.application_id%TYPE,
      pv_process_name_i     IN ex_error_log.process_name%TYPE,
      pv_function_name_i    IN ex_error_log.function_name%TYPE)
   IS
      lv_start_date   DATE;
      lv_end_date     DATE;
   BEGIN
      --
      p_step := 1;
      p_message := 'Capture start date from TR_ETL_BATCH_CTL table ..';
      DBMS_OUTPUT.put_line (p_message || CHR (10));

      lv_start_date :=
         fn_find_run_date (500,
                           'DWHK021',
                           'fn_find_max_batch_ctl_id',
                           'S',
                           1);

      p_step := 2;
      p_message := 'Capture end date from TR_ETL_BATCH_CTL table ..';
      DBMS_OUTPUT.put_line (p_message || CHR (10));

      lv_end_date :=
         fn_find_run_date (102,
                           'DWHK021',
                           'fn_find_max_batch_ctl_id',
                           'E',
                           1);
      --
      p_step := 3;
      p_message := 'truncate st_customer_activity table ..';
      DBMS_OUTPUT.put_line (p_message || CHR (10));

      EXECUTE IMMEDIATE 'truncate table st_customer_activity';

      p_step := 4;
      p_message :=
         '...Populating st_customer_activity staging table';
      DBMS_OUTPUT.put_line (p_message || CHR (10));

      INSERT INTO st_customer_activity (ca_id,
                                        action_cd,
                                        source_cust_id,
                                        action_date,
                                        order_id,
                                        line_item,
                                        user_name,
                                        domain_name,
                                        source_prev_email_address,
                                        cancel_reason_cd,
                                        more_time_date,
                                        code_premium,
                                        source_addr_id,
                                        source_prev_addr_id,
                                        remark,
                                        source_create_date,
                                        source_system_id,
                                        create_date,
                                        created_by)
         SELECT nca.ca_id,
                nca.action_cd,
                nca.cust_id,
                nca.action_date,
                nca.order_id,
                nca.order_line,
                nca.username,
                nca.domain_name,
                TRIM(nca.prev_email) prev_email,
                nca.cancel_reason_cd,
                nca.more_time_date,
                nca.code_premium,
                nca.address_id,
                nca.prev_address_id,
                nca.remark,
                nca.created_date source_create_date,
                1 source_system_id,
                SYSDATE create_date,
                USER
           FROM rs_nss_customer_activity nca
          WHERE nca.action_date > lv_start_date
            AND nca.action_date <= lv_end_date;

   EXCEPTION
      WHEN OTHERS
      THEN
         p_error_message := SQLERRM;
         p_error_id :=
            pkg_error.fn_insert_error_log (pv_application_id_i,
                                           pv_process_name_i,
                                           pv_function_name_i,
                                           '1',
                                           SUBSTR (SQLERRM, 1, 4000),
                                           NULL,
                                           NULL,
                                           NULL);
         raise_application_error (
            -20099,
               '['
            || SUBSTR (p_error_message, 5, 5)
            || '], ['
            || TRIM (p_error_id)
            || '] Step '
            || p_step
            || ': '
            || p_message);
   END prc_ins_st_customer_activity;

       -----------------------------------------------------------------
   -- Procedure Name: prc_ins_st_cap_cl_inbound
   --
   -- Created by: Krishan Kumar Tripathi
   --
   -- Objective: This function will insert the closed loop inbound transactions to staging
   --
   --
   -- Modifications:
   -- Date:        Created By:                Description:
   -- ------------ ----------------------- -------------------------
   -- 10/26/2017   Krishan Kumar Tripathi       Created.
   -- 12/15/2017   Krishan Kumar Tripathi       US13426: truncating table daily to pick the change in process code for existing record.
   --                                           Need to limit the staging table pull to only read data starting 1-Jan-2016 onwards for the files created.
   -- 12/12/2019   Krishan Kumar Tripathi       SBO-18054: Adding 2 new columns
   -----------------------------------------------------------------
   --

PROCEDURE prc_ins_st_cap_cl_inbound (
   pv_application_id_i   IN ex_error_log.application_id%TYPE,
   pv_process_name_i     IN ex_error_log.process_name%TYPE,
   pv_function_name_i    IN ex_error_log.function_name%TYPE)
AS
   lv_start_date   DATE;
   lv_end_date     DATE;
   LV_SYSDATE      DATE := SYSDATE;
BEGIN
   p_step := 1;
   p_message := 'Capture start date from TR_ETL_BATCH_CTL table ..';
   DBMS_OUTPUT.put_line (p_message || CHR (10));

   lv_start_date :=
      pkg_st_news_etl.fn_find_run_date (500,
                                        'dwhk002',
                                        'fn_find_max_batch_ctl_id',
                                        'S',
                                        1);

   p_step := 2;
   p_message := 'Capture end date from TR_ETL_BATCH_CTL table ..';
   DBMS_OUTPUT.put_line (p_message || CHR (10));

   lv_end_date :=
      pkg_st_news_etl.fn_find_run_date (102,
                                        'dwhk002',
                                        'fn_find_max_batch_ctl_id',
                                        'E',
                                        1);
   --
   p_step := 3;
   p_message := 'truncate  ST_CAP_CLOSED_LOOP_INBOUND table ..';
   DBMS_OUTPUT.put_line (p_message || CHR (10));

   EXECUTE IMMEDIATE 'truncate table ST_CAP_CLOSED_LOOP_INBOUND';

   p_step := 4;
   p_message :=
      'Populating staging table with CAP closed loop inbound transactions info ..';
   DBMS_OUTPUT.put_line (p_message || CHR (10));

   INSERT INTO ST_CAP_CLOSED_LOOP_INBOUND (CAP_CL_INB_TRANS_ID,
CREATED_DATE,
SOURCE_CREATED_DATE,
CREATED_BY,
EXTERNAL_IDENTIFIER,
CAP_CL_FILE_ID,
TRANS_TYPE,
MERCHANT_ORDER_NO,
ORDER_ID,
LINE_ITEM,
PMT_AMT,
CAP_REQUEST_DATE,
SOURCE_CUST_ID,
BONUS_CODE,
RESPONSE_CODE,
RESPONSE_MESSAGE,
PROCESS_CODE,
PROCESS_MESSAGE,
ORDER_DATE,
FILE_NAME,
FILE_TYPE,
CAP_CLIENT_NAME,
LAST_NAME,
CAP_CLIENT_FILE_ID,
STATUS,
MP_TRANS_ID,
STATUS_ID)
      SELECT CAP_CL_INB_TRANS_ID,
LV_SYSDATE,
A.CREATED_DATE,
A.CREATED_BY,
EXTERNAL_IDENTIFIER,
A.CAP_CL_FILE_ID,
TRANS_TYPE,
MERCHANT_ORDER_NO,
ORDER_ID,
LINE_ITEM,
PMT_AMT,
REQUEST_DATE,
CUST_ID,
BONUS_CODE,
RESP_CODE,
RESP_MSG,
PROCESS_CD,
PROCESS_MSG,
ORDER_DATE,
FILE_NAME,
FILE_TYPE,
CAP_CLIENT_NAME,
LAST_NAME,
CAP_CLIENT_FILE_ID,
STATUS,
MP_TRANS_ID,
STATUS_ID
        FROM RS_NSS_CL_INBOUND_TRANS a, LS_CF_CAP_FILE_DETAILS b
       WHERE    a.cap_cl_file_id = b.cap_cl_file_id
             AND b.source_created_date >  to_date('12/31/2015 11:59:59 PM','mm/dd/yyyy hh:mi:ss AM') --read all the transactions for the files created starting 1-Jan-2016
             AND b.source_created_date <= lv_end_date

union

SELECT CAP_CL_INB_TRANS_ID,
LV_SYSDATE,
A.CREATED_DATE,
A.CREATED_BY,
EXTERNAL_IDENTIFIER,
A.CAP_CL_FILE_ID,
TRANS_TYPE,
MERCHANT_ORDER_NO,
ORDER_ID,
LINE_ITEM,
PMT_AMT,
REQUEST_DATE,
CUST_ID,
BONUS_CODE,
RESP_CODE,
RESP_MSG,
PROCESS_CD,
PROCESS_MSG,
ORDER_DATE,
FILE_NAME,
FILE_TYPE,
CAP_CLIENT_NAME,
LAST_NAME,
CAP_CLIENT_FILE_ID,
STATUS,
MP_TRANS_ID,
STATUS_ID
        FROM RS_NSS_TR_INBOUND_TRANS a, LS_CF_CAP_FILE_DETAILS b
       WHERE    a.cap_cl_file_id = b.cap_cl_file_id
             AND b.source_created_date >  to_date('12/31/2015 11:59:59 PM','mm/dd/yyyy hh:mi:ss AM') --read all the transactions for the files created starting 1-Jan-2016
             AND b.source_created_date <= lv_end_date;
EXCEPTION
   WHEN OTHERS
   THEN
      p_error_message := SQLERRM;
      p_error_id :=
         pkg_error.fn_insert_error_log (pv_application_id_i,
                                        pv_process_name_i,
                                        pv_function_name_i,
                                        '1',
                                        SUBSTR (SQLERRM, 1, 4000),
                                        NULL,
                                        NULL,
                                        NULL);
      raise_application_error (
         -20099,
            '['
         || SUBSTR (p_error_message, 5, 5)
         || '], ['
         || TRIM (p_error_id)
         || '] Step '
         || p_step
         || ': '
         || p_message);
END prc_ins_st_cap_cl_inbound;


-----------------------------------------------------------------
-- Procedure Name: prc_ins_st_cap_cl_outbound
--
-- Created by: Krishan Kumar Tripathi
--
-- Objective: This function will insert the closed loop outbound transactions to staging
--
--
-- Modifications:
-- Date:        Created By:                Description:
-- ------------ ----------------------- -------------------------
-- 10/26/2017   Krishan Kumar Tripathi       Created.
-- 12/15/2017   Krishan Kumar Tripathi       US13426: truncating table daily to pick the change in process code for existing record.
--                                           Need to limit the staging table pull to only read data starting 1-Jan-2016 onwards for the files created.
-- 12/12/2019   Krishan Kumar Tripathi       SBO -18054: Added 4 new columns
-- 05/13/2020   Sagar                        SBO-20353 Added union condition to fetch old data from archive talbe in nss
-----------------------------------------------------------------
--

PROCEDURE prc_ins_st_cap_cl_outbound (
   pv_application_id_i   IN ex_error_log.application_id%TYPE,
   pv_process_name_i     IN ex_error_log.process_name%TYPE,
   pv_function_name_i    IN ex_error_log.function_name%TYPE)
AS
   lv_start_date   DATE;
   lv_end_date     DATE;
   LV_SYSDATE      DATE := SYSDATE;
BEGIN
   p_step := 1;
   p_message := 'Capture start date from TR_ETL_BATCH_CTL table ..';
   DBMS_OUTPUT.put_line (p_message || CHR (10));

   lv_start_date :=
      pkg_st_news_etl.fn_find_run_date (500,
                                        'dwhk002',
                                        'fn_find_max_batch_ctl_id',
                                        'S',
                                        1);

   p_step := 2;
   p_message := 'Capture end date from TR_ETL_BATCH_CTL table ..';
   DBMS_OUTPUT.put_line (p_message || CHR (10));

   lv_end_date :=
      pkg_st_news_etl.fn_find_run_date (102,
                                        'dwhk002',
                                        'fn_find_max_batch_ctl_id',
                                        'E',
                                        1);
   --
   p_step := 3;
   p_message := 'truncate  ST_CAP_CLOSED_LOOP_OUTBOUND table ..';
   DBMS_OUTPUT.put_line (p_message || CHR (10));

   EXECUTE IMMEDIATE 'truncate table ST_CAP_CLOSED_LOOP_OUTBOUND';

   p_step := 4;
   p_message :=
      'Populating staging table with CAP closed loop inbound transactions info ..';
   DBMS_OUTPUT.put_line (p_message || CHR (10));

   INSERT INTO ST_CAP_CLOSED_LOOP_OUTBOUND (CAP_CL_OUTB_TRANS_ID,
CAP_PROCESSED_DATE,
BONUS_CODE,
EXTERNAL_IDENTIFIER,
CAP_CL_FILE_ID,
MERCHANT_ORDER_NO,
ORDER_ID,
LINE_ITEM,
REQUEST_DATE,
MERCHANT_ID,
CAP_CL_INB_TRANS_ID,
CLIENT_CODE,
ORDER_DATE,
FILE_NAME,
FILE_TYPE,
CAP_CLIENT_FILE_ID,
STATUS,
TRANS_TYPE,
PMT_AMT,
SOURCE_CUST_ID,
LAST_NAME,
SOURCE_CREATED_DATE,
CREATED_DATE,
CREATED_BY,
SOURCE_SYSTEM_ID,
REFERENCE_ID,
PRINTED_PROD_CD,
PRINTED_SOURCE_CD)
      SELECT CAP_CL_OUTB_TRANS_ID,
PROCESSED_DATE,
BONUS_CODE,
EXTERNAL_IDENTIFIER,
A.CAP_CL_FILE_ID,
MERCHANT_ORDER_NO,
ORDER_ID,
LINE_ITEM,
REQUEST_DATE,
MERCHANT_ID,
CAP_CL_INB_TRANS_ID,
CLIENT_ID,
ORDER_DATE,
FILE_NAME,
FILE_TYPE,
CAP_CLIENT_FILE_ID,
b.STATUS,
TRANS_TYPE,
PMT_AMT,
CUST_ID,
LAST_NAME,
A.CREATED_DATE,
LV_SYSDATE,
A.CREATED_BY,
A.SOURCE_SYSTEM_ID,
A.REFERENCE_ID,
A.PRINTED_PROD_CD,
A.PRINTED_SOURCE_CD
        FROM RS_NSS_CL_OUTBOUND_TRANS a, LS_CF_CAP_FILE_DETAILS b
       WHERE     a.cap_cl_file_id = b.cap_cl_file_id
             AND b.source_created_date > to_date('12/31/2015 11:59:59 PM','mm/dd/yyyy hh:mi:ss AM') --read all the transactions for the files created starting 1-Jan-2016
             AND b.source_created_date <= lv_end_date
union
	SELECT CAP_CL_OUTB_TRANS_ID,
PROCESSED_DATE,
BONUS_CODE,
EXTERNAL_IDENTIFIER,
A.CAP_CL_FILE_ID,
MERCHANT_ORDER_NO,
ORDER_ID,
LINE_ITEM,
REQUEST_DATE,
MERCHANT_ID,
CAP_CL_INB_TRANS_ID,
CLIENT_ID,
ORDER_DATE,
FILE_NAME,
FILE_TYPE,
CAP_CLIENT_FILE_ID,
b.STATUS,
TRANS_TYPE,
PMT_AMT,
CUST_ID,
LAST_NAME,
A.CREATED_DATE,
LV_SYSDATE,
A.CREATED_BY,
A.SOURCE_SYSTEM_ID,
A.REFERENCE_ID,
A.PRINTED_PROD_CD,
A.PRINTED_SOURCE_CD
        FROM RS_NSS_TR_OUTBOUND_TRANS a, LS_CF_CAP_FILE_DETAILS b
       WHERE     a.cap_cl_file_id = b.cap_cl_file_id
             AND b.source_created_date > to_date('12/31/2015 11:59:59 PM','mm/dd/yyyy hh:mi:ss AM') --read all the transactions for the files created starting 1-Jan-2016
             AND b.source_created_date <= lv_end_date;

EXCEPTION
   WHEN OTHERS
   THEN
      p_error_message := SQLERRM;
      p_error_id :=
         pkg_error.fn_insert_error_log (pv_application_id_i,
                                        pv_process_name_i,
                                        pv_function_name_i,
                                        '1',
                                        SUBSTR (SQLERRM, 1, 4000),
                                        NULL,
                                        NULL,
                                        NULL);
      raise_application_error (
         -20099,
            '['
         || SUBSTR (p_error_message, 5, 5)
         || '], ['
         || TRIM (p_error_id)
         || '] Step '
         || p_step
         || ': '
         || p_message);
END prc_ins_st_cap_cl_outbound;

   -----------------------------------------------------------------
   -- Procedure Name: prc_ins_st_fulfillment_cleared
   --
   -- Created by: Krishan Kumar Tripathi
   --
   -- Objective: This procedure will insert the fulfillment cleared transaction in staging table
   --
   --
   -- Modifications:
   -- Date:        Created By:                Description:
   -- ------------ ----------------------- -------------------------
   -- 10/26/2017   Krishan Kumar Tripathi       Created.
   -----------------------------------------------------------------
   --

   PROCEDURE prc_ins_st_fulfillment_cleared (
   pv_application_id_i   IN ex_error_log.application_id%TYPE,
   pv_process_name_i     IN ex_error_log.process_name%TYPE,
   pv_function_name_i    IN ex_error_log.function_name%TYPE)
   as
   lv_start_date   DATE;
   lv_end_date     DATE;
   LV_SYSDATE      DATE := SYSDATE;
BEGIN
   p_step := 1;
   p_message := 'Capture start date from TR_ETL_BATCH_CTL table ..';
   DBMS_OUTPUT.put_line (p_message || CHR (10));

   lv_start_date :=
      pkg_st_news_etl.fn_find_run_date (500,
                                        'dwhk002',
                                        'fn_find_max_batch_ctl_id',
                                        'S',
                                        1);

   p_step := 2;
   p_message := 'Capture end date from TR_ETL_BATCH_CTL table ..';
   DBMS_OUTPUT.put_line (p_message || CHR (10));

   lv_end_date :=
      pkg_st_news_etl.fn_find_run_date (102,
                                        'dwhk002',
                                        'fn_find_max_batch_ctl_id',
                                        'E',
                                        1);
   --
   p_step := 3;
   p_message := 'truncate  st_fulfillment_cleared table ..';
   DBMS_OUTPUT.put_line (p_message || CHR (10));

   EXECUTE IMMEDIATE 'truncate table st_fulfillment_cleared';

   p_step := 4;
   p_message :=
      'Populating staging table with fulfillment cleared transaction info ..';
   DBMS_OUTPUT.put_line (p_message || CHR (10));

   insert into st_fulfillment_cleared (order_id,
                                  line_item,
                                  cancel_not_ff,
                                  cleared_ind,
                                  cust_contract_term,
                                  offer_id,
                                  net_remit_amt,
                                  source_cd,
                                  publisher_term,
                                  trans_date,
                                  trans_type,
                                  sent_fulfillement_date,
                                  SCHED_FULFILLMENT_DATE,
                                  expected_term_start_date,
                                  source_system_id,
                                  fulfillment_id,
                                  create_date,
                                  ff_cleared_date)
                                  select fs.order_id,
                 fs.line_item,
                 fs.cancel_not_ff,
                 fs.cleared_ind,
                 fs.bill_yr,
                 fs.offer_id,
                 fs.net_remit_amt,
                 o.source_cd,
                 fs.publisher_term,
                 fs.trans_date,
                 CASE
                    WHEN fs.trans_type = 'T'
                    THEN
                       'O'
                    WHEN fs.trans_type = 'U'
                    THEN
                       'R'
                    ELSE
                       fs.trans_type
                 END,
                 fs.sent_fulfillement_date,
                 FS.SCHED_FULFILLMENT_DATE,
                 fs.expected_term_start_date,
                 1,
                 fs.fulfillment_id,
                 lv_sysdate,
                 fs.ff_cleared_date
                 from
                  rs_nss_fulfillment_schedule fs,
                 rs_nss_order o
           WHERE     fs.ff_cleared_date > lv_start_date
                 AND fs.ff_cleared_date <= lv_end_date
                 AND fs.order_id = o.order_id;


   exception
    WHEN OTHERS
   THEN
      p_error_message := SQLERRM;
      p_error_id :=
         pkg_error.fn_insert_error_log (pv_application_id_i,
                                        pv_process_name_i,
                                        pv_function_name_i,
                                        '1',
                                        SUBSTR (SQLERRM, 1, 4000),
                                        NULL,
                                        NULL,
                                        NULL);
      raise_application_error (
         -20099,
            '['
         || SUBSTR (p_error_message, 5, 5)
         || '], ['
         || TRIM (p_error_id)
         || '] Step '
         || p_step
         || ': '
         || p_message);
   end prc_ins_st_fulfillment_cleared;

   -----------------------------------------------------------------
   -- Procedure Name: prc_ins_st_fulfillment_trans_not_sent
   --
   -- Created by: Krishan Kumar Tripathi
   --
   -- Objective: This procedure will insert the fulfillment records that have sent to fulfillemnt date populated along with cancel_not_ff flag set to 1.
   --
   --
   -- Modifications:
   -- Date:        Created By:                Description:
   -- ------------ ----------------------- -------------------------
   -- 02/12/2018   Krishan Kumar Tripathi       Created.
   -----------------------------------------------------------------
   --

   PROCEDURE prc_ins_st_fulfillment_trans_not_sent (
   pv_application_id_i   IN ex_error_log.application_id%TYPE,
   pv_process_name_i     IN ex_error_log.process_name%TYPE,
   pv_function_name_i    IN ex_error_log.function_name%TYPE)

AS
      lv_start_date   DATE;
      lv_end_date     DATE;
   BEGIN
      --
      p_step := 1;
      p_message := 'Capture start date from TR_ETL_BATCH_CTL table ..';
      DBMS_OUTPUT.put_line (p_message || CHR (10));

      lv_start_date :=
         fn_find_run_date (500,
                           'DWHK002',
                           'fn_find_max_batch_ctl_id',
                           'S',
                           1);

      p_step := 2;
      p_message := 'Capture end date from TR_ETL_BATCH_CTL table ..';
      DBMS_OUTPUT.put_line (p_message || CHR (10));

      lv_end_date :=
         fn_find_run_date (102,
                           'DWHK002',
                           'fn_find_max_batch_ctl_id',
                           'E',
                           1);
      --
      p_step := 3;
      p_message := 'truncate st_fulfillment_trans_not_sent table ..';
      DBMS_OUTPUT.put_line (p_message || CHR (10));

      EXECUTE IMMEDIATE 'truncate table st_fulfillment_trans_not_sent';

      p_step := 4;
      p_message :=
         'Populating staging table with new sent fulfillment info ..';
      DBMS_OUTPUT.put_line (p_message || CHR (10));

      INSERT INTO st_fulfillment_trans_not_sent (order_id,
                                  line_item,
                                  cancel_issues,
                                  cancel_not_ff,
                                  cleared_ind,
                                  cust_contract_term,
                                  offer_id,
                                  net_remit_amt,
                                  source_cd,
                                  publisher_term,
                                  trans_date,
                                  trans_type,
                                  sent_fulfillement_date,
                                  expected_term_start_date,
                                  source_system_id,
                                  original_offer_id,
                                  fulfillment_id,
                                  create_date,
                                  current_term_flexbill_ind)
         (SELECT fs.order_id,
                 fs.line_item,
                 fs.cancel_issues,
                 fs.cancel_not_ff,
                 fs.cleared_ind,
                 fs.bill_yr,
                 fs.offer_id,
                 fs.net_remit_amt,
                 o.source_cd,
                 fs.publisher_term,
                 fs.trans_date,
                 CASE
                    WHEN fs.trans_type = 'T'
                    THEN
                       'O'
                    WHEN fs.trans_type = 'U'
                    THEN
                       'R'
                    WHEN     fs.trans_type IN ('O', 'R')
                         AND fs2.trans_type IN ('T', 'U')
                    THEN
                       'T'
                    ELSE
                       fs.trans_type
                 END,
                 fs.sent_fulfillement_date,
                 fs.expected_term_start_date,
                 1,
                 fs2.offer_id,
                 fs.fulfillment_id,
                 SYSDATE,
                 (SELECT fs3.RNW_FLEX_BILL_INTERVAL_RENEW
                 FROM rs_nss_fulfillment_schedule fs3
                WHERE     fs.order_id = fs3.order_id
                      AND fs.line_item = fs3.line_item
                      AND (fs.bill_yr - 1) = fs3.bill_yr
                      AND fs3.RNW_FLEX_BILL_INTERVAL_RENEW IS NOT NULL)
                 AS RNW_FLEX_BILL_INTERVAL_RENEW
            FROM rs_nss_fulfillment_schedule fs,
                 rs_nss_fulfillment_schedule fs2,
                 rs_nss_order o
           WHERE     fs.sent_fulfillement_date > lv_start_date
                 AND fs.sent_fulfillement_date <= lv_end_date
                 AND fs.trans_type <> 'C'
                 AND NVL (fs.cancel_not_ff, 0)= 1
                 AND fs.order_id = fs2.order_id(+)
                 AND fs.line_item = fs2.line_item(+)
                 AND fs.bill_yr = fs2.bill_yr(+)
                 AND fs2.trans_type(+) IN ('T', 'U')
                 AND fs.order_id = o.order_id);

 EXCEPTION
      WHEN OTHERS
      THEN
         p_error_message := SQLERRM;
         p_error_id :=
            pkg_error.fn_insert_error_log (pv_application_id_i,
                                           pv_process_name_i,
                                           pv_function_name_i,
                                           '1',
                                           SUBSTR (SQLERRM, 1, 4000),
                                           NULL,
                                           NULL,
                                           NULL);
         raise_application_error (
            -20099,
               '['
            || SUBSTR (p_error_message, 5, 5)
            || '], ['
            || TRIM (p_error_id)
            || '] Step '
            || p_step
            || ': '
            || p_message);
   END   prc_ins_st_fulfillment_trans_not_sent;


   -----------------------------------------------------------------
   -- Procedure Name: prc_ins_st_renewal_notification
   --
   -- Created by: RAF
   --
   -- Objective: This procedure will insert into st_renewal_notification
   --
   --
   -- Modifications:
   -- Date:        Created By:                Description:
   -- ------------ ----------------------- -------------------------
   -- 05/05/2018   RAF                     Created
   -- 02/20/2020   David Rodican           Modified for SBO-19214
   -- 04/16/2020   LewisC                  Modified for SBO-20192
   -----------------------------------------------------------------
   --

   PROCEDURE prc_ins_st_renewal_notification (
      pv_application_id_i   IN ex_error_log.application_id%TYPE,
      pv_process_name_i     IN ex_error_log.process_name%TYPE,
      pv_function_name_i    IN ex_error_log.function_name%TYPE)

   AS

   CURSOR cursorRenewal( v_Start_Date IN st_renewal_notification.sent_to_lettershop_date%TYPE,
                         v_End_Date IN st_renewal_notification.sent_to_lettershop_date%TYPE)
    IS
          SELECT FS.ORDER_ID,
                 FS.LINE_ITEM,
                 FS.BILL_YR,
                 FS.OFFER_ID,
                 NVL(FS.NEXT_OFFER_ID,FS.OFFER_ID) as NEXT_OFFER_ID,
                 FS.RENEWAL_DATE,
                 FS.RENEWAL_FIRST_BILL_DATE,
                 FS.SENT_TO_LETTERSHOP_DATE,
                 FS.RNW_FLEX_BILL_INTERVAL_NOTIFY,
                 OFR.SPEC_BILL_INTERVAL_CD,
                 OFR.PMT_METHOD_CD,
                 OFR.NSS_ISSUES,
                 OFR.OFFER_ANNUAL_ISSUES,
                 CPN.CAMPAIGN_ID,
                 CPN.DEF_BILL_INTERVAL_CD,
                 CPN.FLIP_FLAG,
                 ORD.source_cd,
                 ofr.cust_price
           FROM  RS_NSS_FULFILLMENT_SCHEDULE fs,
                 RS_NSS_OFFER ofr,
                 RS_NSS_ORDER ord,
                 RS_NSS_CAMPAIGN cpn
           WHERE NVL(FS.SENT_TO_LETTERSHOP_DATE,sysdate) > v_Start_Date
             AND NVL(FS.SENT_TO_LETTERSHOP_DATE,sysdate) <= v_End_Date
             AND fs.offer_id = ofr.offer_id
             AND fs.order_id = ord.order_id
             AND ord.campaign_id = cpn.campaign_id
             AND fs.bill_yr =(
                   SELECT  MAX(FS2.BILL_YR)
                   FROM RS_NSS_FULFILLMENT_SCHEDULE FS2
                   WHERE NVL(fs2.SENT_TO_LETTERSHOP_DATE,sysdate) > v_Start_Date
                   AND NVL(fs2.SENT_TO_LETTERSHOP_DATE,sysdate) <= v_End_Date
                   AND fs2.order_id = fs.order_id
                   AND fs2.line_item = fs.line_item
                   AND fs2.trans_type in ('O', 'R'))
             AND fs.TRANS_TYPE IN ('O','R');


      lv_start_date         DATE;
      lv_end_date           DATE;
      lv_sysdate            DATE:=sysdate;
      lv_Next_Bill_Yr       RS_NSS_FULFILLMENT_SCHEDULE.Bill_Yr%TYPE;
      lv_Subscription_Type  VARCHAR(01);
      lv_return             NUMBER;
      lv_PaymentAmt         ST_RENEWAL_NOTIFICATION.Payment_Amt%TYPE;
      lv_BillInterval       ST_RENEWAL_NOTIFICATION.Billing_Interval%TYPE;
      lv_NumPmts            NUMBER;
      lv_BillIntervalDesc   ST_RENEWAL_NOTIFICATION.Bill_Interval_Desc%TYPE;

      lv_loc_info VARCHAR2(50);
   BEGIN
      --
      p_step := 1;
      p_message := 'Capture start date from TR_ETL_BATCH_CTL table ..';
      DBMS_OUTPUT.put_line (p_message || CHR (10));

      lv_start_date :=
         fn_find_run_date (500,
                           'DWHK003',
                           'fn_find_max_batch_ctl_id',
                           'S',
                           1);

      p_step := 2;
      p_message := 'Capture end date from TR_ETL_BATCH_CTL table ..';
      DBMS_OUTPUT.put_line (p_message || CHR (10));

      lv_end_date :=
         fn_find_run_date (102,
                           'DWHK003',
                           'fn_find_max_batch_ctl_id',
                           'E',
                           1);
      --
      p_step := 3;
      p_message := 'truncate st_renewal_notification table ..';
      DBMS_OUTPUT.put_line (p_message || CHR (10));

      EXECUTE IMMEDIATE 'truncate table ST_RENEWAL_NOTIFICATION';

      p_step := 4;
      p_message :=
      'Populating staging table with new renewal notification info ..';
      DBMS_OUTPUT.put_line (p_message || CHR (10));


      for cRec in cursorRenewal(lv_start_date,lv_end_date) loop

        --lv_Next_Bill_Yr := cRec.Bill_Yr + 1;

        lv_Subscription_Type := 'R';
        --       CASE lv_Next_Bill_Yr
        --           WHEN 1 THEN 'N'
        --           ELSE 'R'
        --       END;

        lv_loc_info := 'GetIntervalAndNumPmts : ' ||  cRec.Order_Id || '|' || cRec.Line_Item;
        lv_return := RS_NSS_PKG_BILLING_TBL.GetIntervalAndNumPmts(in_DefBillIntervalCd        => cRec.Def_Bill_Interval_Cd,
                                                                  in_SpecBillIntervalCd       => cRec.Spec_Bill_Interval_Cd,
                                                                  in_PmtMethodCd              => cRec.Pmt_Method_Cd,
                                                                  in_NssIssues                => cRec.Nss_Issues,
                                                                  in_OfferAnnualIssues        => cRec.Offer_Annual_Issues,
                                                                  in_FLIPflag                 => cRec.Flip_Flag,
                                                                  out_BillInterval            => lv_BillInterval,
                                                                  out_NumPmts                 => lv_NumPmts,
                                                                  in_CampaignId               => cRec.Campaign_Id,
                                                                  in_OfferId                  => cRec.Next_Offer_Id,
                                                                  in_subscription_type        => lv_Subscription_Type,
                                                                  in_rnwbillingoverride       => cRec.Rnw_Flex_Bill_Interval_Notify);


        lv_loc_info := 'FN_GET_BILLING_INTERVAL_DESC : ' ||  cRec.Order_Id || '|' || cRec.Line_Item;
        lv_BillIntervalDesc := RS_NSS_PKG_BILLING_TBL.FN_GET_BILLING_INTERVAL_DESC (pv_order_id   => cRec.order_id,
                                                                                    pv_line_item  => cRec.line_item,
                                                                                    pv_appflag    => 'NOA');

        lv_loc_info := 'Fn_Get_Payment_Amt : ' ||  cRec.Order_Id || '|' || cRec.Line_Item;
        lv_PaymentAmt :=  Fn_Get_Payment_Amt ( pv_order_id_i          => cRec.order_id,
                                      pv_line_item_i         => cRec.line_item,
                                      pv_bill_yr_i  	     => cRec.Bill_Yr,
                                      pv_offer_id_i          => cRec.next_offer_id,
                                      pv_campaign_id_i       => cRec.campaign_id,
                                      pv_application_id_i    => pv_application_id_i,
                                      pv_process_name_i      => pv_process_name_i,
                                      pv_function_name_i     => pv_function_name_i,
                                      pv_subscription_type   => lv_Subscription_Type
                                     );

        --lv_loc_info := 'Calculating PAY_AMT : ' ||  cRec.Next_Offer_Id || '|' || cRec.source_cd;
        --lv_PaymentAmt := ceil((cRec.cust_price / lv_NumPmts)*100)/100;

        lv_loc_info := 'INSERT : ' ||  cRec.Order_Id || '|' || cRec.Line_Item;
        INSERT INTO ST_RENEWAL_NOTIFICATION
                    ( ORDER_ID,
                      LINE_ITEM,
                      CUST_CONTRACT_TERM,
                      OFFER_ID,
                      NEXT_OFFER_ID,
                      RENEWAL_DATE,
                      RENEWAL_FIRST_BILL_DATE,
                      SENT_TO_LETTERSHOP_DATE,
                      RNW_FLEX_BILL_INTERVAL_NOTIFY,
                      SOURCE_SYSTEM_ID,
                      CREATE_DATE,
                      PAYMENT_AMT,
                      BILLING_INTERVAL,
                      BILL_INTERVAL_DESC)
                 VALUES
                (cRec.ORDER_ID,
                 cRec.LINE_ITEM,
                 cRec.BILL_YR,
                 cRec.OFFER_ID,
                 cRec.NEXT_OFFER_ID,
                 cRec.RENEWAL_DATE,
                 cRec.RENEWAL_FIRST_BILL_DATE,
                 cRec.SENT_TO_LETTERSHOP_DATE,
                 cRec.RNW_FLEX_BILL_INTERVAL_NOTIFY,
                 1,
                 lv_sysdate,
                 lv_PaymentAmt,
                 lv_BillInterval,
                 lv_BillIntervalDesc
                 );

        end loop;

   EXCEPTION
      WHEN OTHERS
      THEN
         p_error_message := SQLERRM;
         p_error_id :=
            pkg_error.fn_insert_error_log (pv_application_id_i,
                                           pv_process_name_i,
                                           pv_function_name_i,
                                           '1',
                                           SUBSTR (SQLERRM, 1, 4000),
                                           NULL,
                                           NULL,
                                           NULL);
         raise_application_error (
            -20099,
               '['
            || SUBSTR (p_error_message, 5, 5)
            || '], ['
            || TRIM (p_error_id)
            || '] Step '
            || p_step
            || ': '
            || p_message || lv_loc_info);
   END prc_ins_st_renewal_notification;
         -----------------------------------------------------------------
   -- Procedure Name: prc_ins_st_fulfillment_cleared_log
   --
   -- Created by: Krishan Kumar Tripathi
   --
   -- Objective: This procedure will insert the fulfillment records that have fulfillment cleared ind flipped from 1 to 0 or are intial 0.
   --
   --
   -- Modifications:
   -- Date:        Created By:                Description:
   -- ------------ ----------------------- -------------------------
   -- 04/16/2018   Krishan Kumar Tripathi       Created.
   --12/05/2018    Krishan Kumar Tripathi       US15496. Corrected the truncate statement.
   -----------------------------------------------------------------
   --

   PROCEDURE prc_ins_st_fulfillment_cleared_log (
   pv_application_id_i   IN ex_error_log.application_id%TYPE,
   pv_process_name_i     IN ex_error_log.process_name%TYPE,
   pv_function_name_i    IN ex_error_log.function_name%TYPE)

   AS

   lv_start_date   DATE;
   lv_end_date     DATE;
   LV_SYSDATE      DATE := SYSDATE;
BEGIN
   p_step := 1;
   p_message := 'Capture start date from TR_ETL_BATCH_CTL table ..';
   DBMS_OUTPUT.put_line (p_message || CHR (10));

   lv_start_date :=
      pkg_st_news_etl.fn_find_run_date (500,
                                        'dwhk002',
                                        'fn_find_max_batch_ctl_id',
                                        'S',
                                        1);

   p_step := 2;
   p_message := 'Capture end date from TR_ETL_BATCH_CTL table ..';
   DBMS_OUTPUT.put_line (p_message || CHR (10));

   lv_end_date :=
      pkg_st_news_etl.fn_find_run_date (102,
                                        'dwhk002',
                                        'fn_find_max_batch_ctl_id',
                                        'E',
                                        1);
   --
   p_step := 3;
   p_message := 'truncate  st_fulfillment_cleared_log table ..';
   DBMS_OUTPUT.put_line (p_message || CHR (10));

   EXECUTE IMMEDIATE 'truncate table st_fulfillment_cleared_log';

   p_step := 4;
   p_message :=
      'Populating staging table with fulfillment cleared transaction info ..';
   DBMS_OUTPUT.put_line (p_message || CHR (10));

   insert into st_fulfillment_cleared_log (ORDER_ID        ,
                                       LINE_ITEM                ,
                                       CUST_CONTRACT_TERM       ,
                                       FF_SCHEDULE_ID           ,
                                       TRANS_TYPE            ,
                                       SOURCE_AUDIT_LOG_ID      ,
                                       SOURCE_OLD_FF_CLEARED_IND,
                                       SOURCE_NEW_FF_CLEARED_IND,
                                       SOURCE_CD                ,
                                       SOURCE_SYSTEM_ID    ,
                                       SOURCE_CREATED_DATE,
                                       SOURCE_CREATED_BY,
                                       CREATE_DATE    ,
                                       CREATED_BY    )
               select fs.order_id,
                 fs.line_item,
                 fs.bill_yr,
                 fls.ff_schedule_id,
                 fs.trans_type,
                 fls.audit_log_id,
                 fls.old_cleared_ind,
                 fls.new_cleared_ind,
                 o.source_cd,
                 1,
                 fls.created_date,
                 fls.created_by,
                 LV_SYSDATE,
                 'dwhk002'
                 from
                  rs_nss_fulfillment_schedule fs,
                  rs_st_fulfillment_cleared_log fls,
                  rs_nss_order o
           WHERE     fls.created_date > lv_start_date
                 AND fls.created_date <= lv_end_date
                 AND fls.ff_schedule_id = fs.ff_schedule_id
                 and fs.order_id=o.order_id;


EXCEPTION
      WHEN OTHERS
      THEN
         p_error_message := SQLERRM;
         p_error_id :=
            pkg_error.fn_insert_error_log (pv_application_id_i,
                                           pv_process_name_i,
                                           pv_function_name_i,
                                           '1',
                                           SUBSTR (SQLERRM, 1, 4000),
                                           NULL,
                                           NULL,
                                           NULL);
         raise_application_error (
            -20099,
               '['
            || SUBSTR (p_error_message, 5, 5)
            || '], ['
            || TRIM (p_error_id)
            || '] Step '
            || p_step
            || ': '
            || p_message);
   END   prc_ins_st_fulfillment_cleared_log;

   -----------------------------------------------------------------
   -- Procedure Name: prc_ins_st_sched_bill_dt_audit_log
   --
   -- Created by: Roopesha Brammachary
   --
   -- Objective: This procedure will insert the audit log records of the news billing_schedule(sched_billing_Date) table
   --
   --
   -- Modifications:
   -- Date:        Created By:                Description:
   -- ------------ ----------------------- -------------------------
   -- 05/11/2018   Roopesha Brammachary       Created for US14131
   -----------------------------------------------------------------
   --

   PROCEDURE prc_ins_st_sched_bill_dt_audit_log (
       pv_application_id_i   IN ex_error_log.application_id%TYPE,
       pv_process_name_i     IN ex_error_log.process_name%TYPE,
       pv_function_name_i    IN ex_error_log.function_name%TYPE)
       AS

       lv_start_date   DATE;
       lv_end_date     DATE;
       lv_sysdate      DATE := SYSDATE;

   BEGIN

       p_step := 1;
       p_message := 'Capture start date from TR_ETL_BATCH_CTL table ..';
       DBMS_OUTPUT.put_line (p_message || CHR (10));

       lv_start_date :=
          pkg_st_news_etl.fn_find_run_date (500,
                                            'DWHK002',
                                            'fn_find_max_batch_ctl_id',
                                            'S',
                                            1);

       p_step := 2;
       p_message := 'Capture end date from TR_ETL_BATCH_CTL table ..';
       DBMS_OUTPUT.put_line (p_message || CHR (10));

       lv_end_date :=
          pkg_st_news_etl.fn_find_run_date (102,
                                            'DWHK002',
                                            'fn_find_max_batch_ctl_id',
                                            'E',
                                            1);

       DBMS_OUTPUT.put_line ('Start Date : '|| lv_start_date||' and End Date : '||lv_end_date);

       --
       p_step := 3;
       p_message := 'truncate ST_SCHED_BILL_DT_AUDIT_LOG table ..';
       DBMS_OUTPUT.put_line (p_message || CHR (10));

       EXECUTE IMMEDIATE 'truncate table ST_SCHED_BILL_DT_AUDIT_LOG';

       p_step := 4;
       p_message :=
          'Populating Audit Log records from RS_NSS_TR_SCHED_BILL_DT_AUDIT_LOG ';
       DBMS_OUTPUT.put_line (p_message || CHR (10));

                             INSERT  INTO ST_SCHED_BILL_DT_AUDIT_LOG
                                           (audit_log_txn_id        ,
                                            billing_schedule_id     ,
                                            old_sched_billing_date  ,
                                            new_sched_billing_date  ,
                                            merchant_order_no       ,
                                            source_create_date      ,
                                            source_modified_date    ,
                                            created_date            ,
                                            created_by
                                           )
                                   SELECT   audit_log_txn_id        ,
                                            billing_schedule_id     ,
                                            old_sched_billing_date  ,
                                            new_sched_billing_date  ,
                                            merchant_order_no       ,
                                            created_date            ,
                                            modified_date           ,
                                            lv_sysdate              ,
                                            USER
                                       FROM RS_NSS_TR_SCHED_BILL_DT_AUDIT_LOG
                                      WHERE created_date > lv_start_date
                                        AND created_date <= lv_end_date;

   EXCEPTION

          WHEN OTHERS
          THEN
             p_error_message := SQLERRM;
             p_error_id :=
                pkg_error.fn_insert_error_log (pv_application_id_i,
                                               pv_process_name_i,
                                               pv_function_name_i,
                                               '1',
                                               SUBSTR (SQLERRM, 1, 4000),
                                               NULL,
                                               NULL,
                                               NULL);
             raise_application_error (
                -20099,
                   '['
                || SUBSTR (p_error_message, 5, 5)
                || '], ['
                || TRIM (p_error_id)
                || '] Step '
                || p_step
                || ': '
                || p_message);

   END   prc_ins_st_sched_bill_dt_audit_log;


   -----------------------------------------------------------------
   -- Procedure Name: prc_ins_st_tr_fulfillment_next_offer
   --
   -- Created by: LewisC
   --
   -- Objective: This procedure will insert renewal records into the st_tr_fulfillment_next_offer table
   --
   --
   -- Modifications:
   -- Date:        Created By:                Description:
   -- ------------ ----------------------- -------------------------
   -- 12/28/2018   LewisC                  Created.
   -----------------------------------------------------------------
   --

   PROCEDURE prc_ins_st_fulfillment_next_offer (
   pv_application_id_i   IN ex_error_log.application_id%TYPE,
   pv_process_name_i     IN ex_error_log.process_name%TYPE,
   pv_function_name_i    IN ex_error_log.function_name%TYPE)

   AS
      lv_start_date   DATE;
      lv_end_date     DATE;
      lv_sysdate      DATE:=sysdate;

   BEGIN
      --
      p_step := 1;
      p_message := 'Capture start date from TR_ETL_BATCH_CTL table ..';
      DBMS_OUTPUT.put_line (p_message || CHR (10));

      lv_start_date :=
         fn_find_run_date (500,
                           'DWHK003',
                           'fn_find_max_batch_ctl_id',
                           'S',
                           1);

      p_step := 2;
      p_message := 'Capture end date from TR_ETL_BATCH_CTL table ..';
      DBMS_OUTPUT.put_line (p_message || CHR (10));

      lv_end_date :=
         fn_find_run_date (102,
                           'DWHK003',
                           'fn_find_max_batch_ctl_id',
                           'E',
                           1);
      --
      p_step := 3;
      p_message := 'truncate st_tr_fulfillment_next_offer table ..';
      DBMS_OUTPUT.put_line (p_message || CHR (10));

      EXECUTE IMMEDIATE 'truncate table st_fulfillment_next_offer';

      p_step := 4;
      p_message :=
         'Populating staging table with next offer id info ..';
      DBMS_OUTPUT.put_line (p_message || CHR (10));

      INSERT INTO st_fulfillment_next_offer
                    ( FF_SCHEDULE_ID,
                      NEXT_OFFER_ID,
                      NEXT_OFFER_CHANGE_DATE,
                      CREATE_DATE)
         (SELECT FF_SCHEDULE_ID,
                 NEXT_OFFER_ID,
                 NEXT_OFFER_ID_DATE,
                 lv_sysdate
           FROM  RS_NSS_FULFILLMENT_SCHEDULE
           WHERE NEXT_OFFER_ID_DATE > LV_START_DATE
             AND NEXT_OFFER_ID_DATE <= LV_END_DATE);

      p_step := 5;
      p_message := 'Inserted data: ' || SQL%ROWCOUNT ;
      DBMS_OUTPUT.put_line (p_message || CHR (10));

  EXCEPTION
      WHEN OTHERS
      THEN
         p_error_message := SQLERRM;
         p_error_id :=
            pkg_error.fn_insert_error_log (pv_application_id_i,
                                           pv_process_name_i,
                                           pv_function_name_i,
                                           '1',
                                           SUBSTR (SQLERRM, 1, 4000),
                                           NULL,
                                           NULL,
                                           NULL);
         raise_application_error (
            -20099,
               '['
            || SUBSTR (p_error_message, 5, 5)
            || '], ['
            || TRIM (p_error_id)
            || '] Step '
            || p_step
            || ': '
            || p_message);
   END prc_ins_st_fulfillment_next_offer;

  ----------------------------------------------------------------------------
  --FUNCTION        :  FN_GET_PAYMENT_AMT
  --Written By      :  LewisC
  --Date            :  04/15/2020
  --Description     :  Get Payment Amount
  --Parameters      :  Order ID and Line Item
  --
  --Date                  Name                    Description
  --------------          ---------------------   ----------------------------
  -- 04/15/2020          LewisC                   Used to calculate NEWS payment amount using a call to NEWS functions
  ----------------------------------------------------------------------------


PROCEDURE prc_get_renew_flex_override
(
    pv_order_id_i                 IN  rs_nss_billing_schedule.order_id%TYPE,
    pv_line_item_i                IN  rs_nss_billing_schedule.line_item%TYPE,
    pv_bill_yr_i                  IN  rs_nss_billing_schedule.bill_yr%TYPE,
    pv_renew_flag                 IN  NUMBER, --parameter to check if this has been called from renewal process or not.
    pv_bill_interval_o            OUT NUMBER)
   IS
      lv_error_id      NUMBER;
      lv_major_card_cd             rs_nss_major_card.major_card_cd%type;
      lv_campaign_id               rs_nss_campaign.campaign_id%type;
      lv_rnw_orride_ct             number;
      lv_true_mp_id                rs_nss_CARD_TYPE.TRUE_MP_ID%type;
      lv_lettershop_date           rs_nss_fulfillment_schedule.sent_to_lettershop_date%TYPE;
      lv_notify_interval	       rs_nss_fulfillment_schedule.RNW_FLEX_BILL_INTERVAL_NOTIFY%TYPE;
      lv_renew_interval	           rs_nss_fulfillment_schedule.RNW_FLEX_BILL_INTERVAL_RENEW%TYPE;
	  lv_check_flag		NUMBER := 0;
   BEGIN
   --<cref> 10.2.2.4-ML-US9960,US9961 - Flexible Renewal Billing Fix - Effect is Immediate
            SELECT
                c.major_card_cd, n.campaign_id, ct.true_mp_id
            INTO
                    lv_major_card_cd,lv_campaign_id, lv_true_mp_id
            FROM
                rs_nss_order n,
                rs_nss_order_line o,
                rs_nss_customer_alias c,
                rs_nss_card_type ct
            WHERE
                n.order_id = pv_order_id_i
            AND c.cust_id = n.cust_id
            AND c.acct_seq = coalesce(n.acct_seq,n.loyalty_seq)
            AND o.order_id = pv_order_id_i
            AND o.line_item = pv_line_item_i
            and c.card_type_cd=ct.card_type_cd;

	--<cref> 11.0.1.0-MA-US14627-20180709
		--NEWS: Use Notification Billing Interval - Modified prc_get_renew_flex_override with the new billing interval logic. </cref>

		IF pv_renew_flag = 1 THEN --called from renewal process

			SELECT RNW_FLEX_BILL_INTERVAL_NOTIFY
				 , SENT_TO_LETTERSHOP_DATE		--11.0.1.1 US14824 -NEWS Bug Fix: Use Notification Billing Interval
			INTO lv_notify_interval
				, lv_lettershop_date			--11.0.1.1 US14824 -NEWS Bug Fix: Use Notification Billing Interval
			FROM rs_nss_fulfillment_schedule fs
			WHERE fs.order_id = pv_order_id_i
				AND fs.line_item = pv_line_item_i
				AND fs.bill_yr = pv_bill_yr_i-1
				AND trans_type IN ('O','R');

			IF lv_lettershop_date IS NOT NULL 		--11.0.1.1 US14824 -NEWS Bug Fix: Use Notification Billing Interval	--IF lv_notify_interval IS NOT NULL THEN
			THEN
				pv_bill_interval_o := lv_notify_interval;  --When notification is already sent respect the same billing interval during renewal process.
				lv_check_flag	:= 1;
			ELSE
				pv_bill_interval_o := NULL;  --11.0.1.1 US14824 -NEWS Bug Fix: Use Notification Billing Interval
			END IF;

		ELSE    --called from notification process

            SELECT RNW_FLEX_BILL_INTERVAL_RENEW
            INTO lv_renew_interval
            FROM rs_nss_fulfillment_schedule fs
            WHERE fs.order_id = pv_order_id_i
                AND fs.line_item = pv_line_item_i
                AND fs.bill_yr = pv_bill_yr_i-1
                AND trans_type IN ('O','R');

            IF lv_renew_interval IS NOT NULL THEN
                pv_bill_interval_o := lv_renew_interval;
                lv_check_flag    := 1;
            ELSE
                pv_bill_interval_o := NULL;
            END IF;
        END IF;
	--<cref> 11.0.1.0-MA-US14627-20180709
		--NEWS: Use Notification Billing Interval - Modified prc_get_renew_flex_override with the new billing interval logic. </cref>

            SELECT count(1)
            INTO lv_rnw_orride_ct
            FROM rs_nss_cf_flex_bill_override
            WHERE campaign_id = lv_campaign_id
            AND bill_yr = pv_bill_yr_i
            AND major_card_cd = lv_major_card_cd
            AND true_mp_id = lv_true_mp_id
            AND start_date < sysdate
            AND (expire_date is null or expire_date > sysdate);

            IF lv_rnw_orride_ct = 1 AND lv_check_flag = 0 THEN -- get override	-<cref> 11.0.1.0-MA-US14627-20180709 --Modified to check this only if the above block does not populate pv_bill_interval_o
					SELECT bill_interval_cd
                    INTO pv_bill_interval_o
                    FROM rs_nss_cf_flex_bill_override
                    WHERE campaign_id = lv_campaign_id
                        AND bill_yr = pv_bill_yr_i
                        AND major_card_cd = lv_major_card_cd
                        AND true_mp_id = lv_true_mp_id
                        AND start_date < sysdate
                        AND (expire_date is null or expire_date > sysdate);
            END IF;

   EXCEPTION
      WHEN OTHERS
      THEN
         lv_error_id :=
            Pkg_Error.fn_insert_error_log (
               5,
               'PKG_BILLING_TBL',
               'prc_get_renew_flex_override: ',
               '1',
               SQLERRM,
                  'Error occured in getting flex bill override for '
               || ' order_id '
               || pv_order_id_i
               || ' line_item '
               || pv_line_item_i,
               SQLCODE,
               NULL);
         RAISE;
   END prc_get_renew_flex_override;

  ----------------------------------------------------------------------------
  --FUNCTION        :  FN_GET_PAYMENT_AMT
  --Written By      :  Shaheer Ahamad
  --Date            :  11/03/2009
  --Description     :  Get Payment Amount
  --Parameters      :
  --
  --Calling Program :  (name).(extention)
  --Modifications   :
  --Date                  Name                    Description
  --------------          ---------------------   ----------------------------
  --11/03/2009            Shaheer Ahamad           Created
  --01/13/2010            Joseph George            Addition of logging steps
  --01/25/2010            Joseph George            Logic changes for calculating
  --						   payment Amt with flip flag
  -- 06/06/2012          Shikher Saxena             Added  param pv_subscription_type(Default NULL):
  --                                                               This param is reqd when we call  GetIntervalAndNumPmts
  --                                                               to calc. num_of_pmts (for a FlexBill campaign)
  -- 09/28/2015          Duane Diniz               Added  param pv_line_item_i to look up number of payments for flex billing override on renewals:
  -- 12/14/2018          Duane Diniz               US15504 NEWS: Use FF Flex Override if exists upon notification process instead of cf_flex_bill_override:
  -- 01/23/2020		 	 Roopesha				   SMP-17384 - Enhancement: NEWS Lettershop Extract Process (ROPE)
  ----------------------------------------------------------------------------

   FUNCTION FN_GET_PAYMENT_AMT (
      pv_order_id_i 	    IN rs_nss_order.order_id%TYPE,
      pv_line_item_i        IN rs_nss_order_line.line_item%TYPE,--US9614
      pv_bill_yr_i  	    IN rs_nss_fulfillment_schedule.bill_yr%TYPE,
      pv_offer_id_i         IN rs_nss_offer.offer_id%TYPE,
      pv_campaign_id_i      IN rs_nss_campaign.campaign_id%TYPE,
      pv_application_id_i   IN ex_error_log.application_id%TYPE,
      pv_process_name_i     IN ex_error_log.process_name%TYPE,
      pv_function_name_i    IN ex_error_log.function_name%TYPE,
      --<cref>-9.1.7.0-US1181-SS-20120606
      pv_subscription_type   IN   varchar2   DEFAULT NULL
      --</cref>-9.1.7.0-US1181-SS-20120606
   )
      RETURN NUMBER
   IS
      lv_def_bill_interval_cd       rs_nss_campaign.def_bill_interval_cd%TYPE;
      lv_spec_bill_interval_cd      rs_nss_offer.spec_bill_interval_cd%TYPE;
      lv_pmt_method_cd              rs_nss_offer.pmt_method_cd%TYPE;
      lv_nss_issues                 rs_nss_offer.nss_issues%TYPE;
      lv_offer_annual_issue         rs_nss_offer.offer_annual_issues%TYPE;
      lv_flip_flag                  rs_nss_campaign.flip_flag%TYPE;
      lv_bill_interval              NUMBER;
      lv_cust_price                 NUMBER;
      lv_payment_amount             NUMBER;
      lv_num_of_payments            NUMBER (8, 2) := 0.0;
      lv_ren_first_bill_date        DATE;
      lv_getintervalandnumpmts_ret  NUMBER;
      lv_step                       NUMBER(2);
      lv_message                    VARCHAR2(1000);
      lv_error_message              VARCHAR2(4000);
      lv_error_id                   EX_ERROR_LOG.ERROR_LOG_ID%TYPE;

      lv_campaign_billint_ct				NUMBER;
      lv_renewal_billing_interval      rs_nss_fulfillment_schedule.rnw_flex_bill_interval_notify%TYPE;

	  -- <cref> 11.3.24.1 -SMP-17384
	  lv_notification_type 			 rs_nss_fulfillment_schedule.notification_type%type;
	  -- </cref> 11.3.24.1 -SMP-17384

   BEGIN

      lv_step := 1;
      lv_message := 'select flip_flag for OrderId,BillYr:'||pv_order_id_i||','||pv_bill_yr_i;
      lv_flip_flag :=rs_nss_PKG_BILLING_TBL.get_FLIP_flag_by_bill_year(pv_order_id_i, pv_bill_yr_i);

      lv_step := 2;
      lv_message := 'select bill_interval for campaign_id:'||pv_campaign_id_i;
       --</cref>9.2.2.6-US3218-SS-20130304
      /* Per US1743: FlexBill renewals will refer camp.renewal_billing_interval and will have bill interval and num of pmts as calculated just like Legacy campaign.
         If FlexBill campaign renewal order then refer renewal_billing_interval else def bill interval cd
      */
       --</cref>9.2.2.6-US3218-SS-20130304</cref>
      SELECT   --c.DEF_BILL_INTERVAL_CD
              case when (c.billing_interval_type_id <> 0 and c.num_pmts_type_id <> 0 and pv_subscription_type ='R')
                 then renewal_billing_interval
                 else def_bill_interval_cd
              end
        INTO   lv_def_bill_interval_cd
        FROM   rs_nss_campaign c
       WHERE   c.campaign_id = pv_campaign_id_i;

      lv_step := 3;
      lv_message := 'select bill_interval,payment mtd for offer_id:'||pv_offer_id_i;

      SELECT   o.spec_bill_interval_cd,
               o.pmt_method_cd,
               o.nss_issues,
               o.offer_annual_issues,
               o.cust_price
        INTO   lv_spec_bill_interval_cd,
               lv_pmt_method_cd,
               lv_nss_issues,
               lv_offer_annual_issue,
               lv_cust_price
        FROM   rs_nss_offer o
       WHERE   o.offer_id = pv_offer_id_i;

    --<cref> 11.2.3.0-DD-US15504 - Get override value from fulfillment_schedule

		-- <cref> 11.3.24.1 -SMP-17384

        IF pv_bill_yr_i > 1 THEN
     	lv_step := 4;
            lv_message := 'check for flex bill interval notify (ROPE - Changes) for order:'||pv_order_id_i;

           SELECT rnw_flex_bill_interval_notify, notification_type
             INTO lv_renewal_billing_interval, lv_notification_type
             FROM rs_nss_fulfillment_schedule
            WHERE order_id = pv_order_id_i
              AND line_item = pv_line_item_i
              AND bill_yr = pv_bill_yr_i
              AND trans_type in ('O','R');

        END IF;

		IF lv_renewal_billing_interval IS NOT NULL AND lv_notification_type IS NOT NULL THEN
			gv_rnw_bill_override := lv_renewal_billing_interval;
		ELSE

			lv_step := 4.1;
     	lv_message := 'check for flex bill renewal override for order:'||pv_order_id_i;
	   SELECT rnw_flex_bill_interval_notify
	   INTO lv_renewal_billing_interval
	   FROM rs_nss_fulfillment_schedule
	   WHERE order_id = pv_order_id_i
	   AND line_item = pv_line_item_i
	   AND bill_yr = pv_bill_yr_i
	   AND trans_type in ('O','R');
    --</cref> 11.2.3.0-DD-US15504 - Get override value from fulfillment_schedule


		-- set gv_rnw_bill_override
		IF lv_renewal_billing_interval is not null THEN
			gv_rnw_bill_override := lv_renewal_billing_interval;
		ELSE
    		--<cref> 10.2.0.0-DD-US9614 - Do flex bill override lookup for flexible renewal billing
			lv_step := 5;
			lv_message := 'check for flex bill renewal override for order:'||pv_order_id_i;
			SELECT count(campaign_id)
			INTO lv_campaign_billint_ct
			FROM rs_nss_cf_flex_bill_override
			WHERE campaign_id = pv_campaign_id_i;
			--<cref> 10.2.2.4-ML-US9960 - Flexible Renewal Billing Fix - Effect is Immediate
    		gv_rnw_bill_override := NULL;
    		IF lv_campaign_billint_ct > 0 THEN
				prc_get_renew_flex_override(pv_order_id_i,pv_line_item_i,pv_bill_yr_i+1,0,gv_rnw_bill_override);
    		END IF;
    		--</cref> 10.2.0.0-DD-US9614 - Do flex bill override lookup for flexible renewal billing
        --</cref> 10.2.2.4-ML-US9960 - Flexible Renewal Billing Fix - Effect is Immediate
		END IF;

		END IF;	   -- </cref> 11.3.24.1 -SMP-17384

     --<cref>-9.1.7.0-US1181-SS-20120606
     -- Now we would pass pv_campaign_id_i,  pv_offer_id_i, pv_subscription_type, doing so would fetch us
     -- respective value of number_of_payments (for a FlexBill campaign).
     --</cref>-9.1.7.0-US1181-SS-20120606
      lv_getintervalandnumpmts_ret := rs_nss_PKG_BILLING_TBL.GETINTERVALANDNUMPMTS(lv_def_bill_interval_cd,
                                                 lv_spec_bill_interval_cd,
                                                 lv_pmt_method_cd,
                                                 lv_nss_issues,
                                                 lv_offer_annual_issue,
                                                 lv_flip_flag,
                                                 lv_bill_interval,
                                                 lv_num_of_payments,
                                                 --<cref>-9.1.7.0-US1181-SS-20120606
                                                 pv_campaign_id_i,
                                                 pv_offer_id_i,
                                                 pv_subscription_type,
                                                 --</cref>-9.1.7.0-US1181-SS-20120606
                                                 gv_rnw_bill_override --<cref> 10.2.0.0-DD-US9614
                                                 );

      IF (lv_num_of_payments > 0)
      THEN
    		--</cref> 10.2.0.0-DD-US9614
         lv_step := 6;
         lv_message := 'calculating payment_amount for offer_id:'||pv_offer_id_i;
         --<cref> US6264-9.13.1.0-Ensuring installment amount is always on the higher side with 2 decimal places
         --lv_payment_amount := lv_cust_price / lv_num_of_payments;
         lv_payment_amount := ceil((lv_cust_price / lv_num_of_payments)*100)/100;
         --</cref>
      END IF;

   RETURN lv_payment_amount;
   EXCEPTION
     WHEN OTHERS
     THEN
        lv_error_message := SQLERRM;
        lv_error_id :=
           Pkg_Error.fn_insert_error_log (
              pv_application_id_i,
              pv_process_name_i,
              pv_function_name_i,
              '1',
              lv_error_message,
              'Step-' || lv_step || ': ' || lv_message,
              SQLCODE,
              NULL
           );
     RAISE;
   END;



END pkg_st_news_etl;
/
