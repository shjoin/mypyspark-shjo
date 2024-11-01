CREATE OR REPLACE PACKAGE BODY pkg_oms_etl
AS

   --
   -- This package contains procedures/functions that insert Spotify - oms data into the DW.
   --

/***************************************************************************************************
   -- Function Name: fn_find_max_batch_ctl_id
   --
   -- Created by: Roopesh
   --
   -- Objective: This function will return max etl_batch_ctl_id
   --
   --
   -- Modifications:
   -- Date:        Created By:                Description:
   -- ------------ ----------------------- -------------------------
   -- 02/20/2018   Roopesh                      Created.
   -----------------------------------------------------------------
***************************************************************************************************/
  
   FUNCTION fn_find_max_batch_ctl_id (
      pv_application_id_i   IN ex_error_log.application_id%TYPE,
      pv_process_name_i     IN ex_error_log.process_name%TYPE,
      pv_function_name_i    IN ex_error_log.function_name%TYPE,
      pv_status_id_i        IN NUMBER,
      pv_source_system_id   IN NUMBER)
      RETURN NUMBER
      
   IS
   
      lv_batch_ctl_id   NUMBER;
      
   BEGIN
   
      p_step := 1;
      p_message :=
         'Capture MAX (etl_batch_ctl_id) from TR_ETL_BATCH_CTL table ..';
      DBMS_OUTPUT.put_line (p_message || CHR (10));

      SELECT MAX (etl_batch_ctl_id)
        INTO lv_batch_ctl_id
        FROM tr_etl_batch_ctl
       WHERE status_id        = pv_status_id_i
         AND source_system_id = pv_source_system_id;

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

/***************************************************************************************************
   -- Procedure Name: prc_upd_etl_batch_ctl
   --
   -- Created by: Roopesh
   --
   -- Objective: This procedure will update status flag on tr_etl_batch_ctl
   --
   --
   -- Modifications:
   -- Date:        Created By:                Description:
   -- ------------ ----------------------- -------------------------
   -- 02/20/2018   Roopesh                      Created.
   -----------------------------------------------------------------
***************************************************************************************************/

   PROCEDURE prc_upd_etl_batch_ctl (
      pv_application_id_i   IN ex_error_log.application_id%TYPE,
      pv_process_name_i     IN ex_error_log.process_name%TYPE,
      pv_function_name_i    IN ex_error_log.function_name%TYPE,
      pv_upd_id_i           IN NUMBER,
      pv_etl_batch_ctl_id   IN NUMBER)
      
   AS
   
   BEGIN
      --
      p_step := 1;
      p_message := 'Updating TR_ETL_BATCH_CTL table ..';
      DBMS_OUTPUT.put_line (p_message || CHR (10));

      UPDATE tr_etl_batch_ctl
         SET status_id        = pv_upd_id_i,
             modified_date    = gv_sysdate,
             modified_by      = pv_process_name_i
       WHERE etl_batch_ctl_id = pv_etl_batch_ctl_id;
       
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
            
   END prc_upd_etl_batch_ctl;

/***************************************************************************************************
   -- Procedure Name: prc_ins_etl_batch_ctl
   --
   -- Created by: Roopesh
   --
   -- Objective: This procedure will insert date range into tr_etl_batch_ctl
   --
   --
   -- Modifications:
   -- Date:        Created By:                Description:
   -- ------------ ----------------------- -------------------------
   -- 02/20/2018   Roopesh                      Created.
   -----------------------------------------------------------------
***************************************************************************************************/

   PROCEDURE prc_ins_etl_batch_ctl (
      pv_application_id_i   IN ex_error_log.application_id%TYPE,
      pv_process_name_i     IN ex_error_log.process_name%TYPE,
      pv_function_name_i    IN ex_error_log.function_name%TYPE)
      
   AS
   
      lv_start_date     DATE;
      lv_batch_ctl_id   NUMBER;
      lv_data_exists    EXCEPTION;
      
   BEGIN
      
      p_step := 1;
      p_message :=
         'Capture MAX (etl_batch_ctl_id) from TR_ETL_BATCH_CTL table ..';
      DBMS_OUTPUT.put_line (p_message || CHR (10));

      lv_batch_ctl_id :=
         fn_find_max_batch_ctl_id (gv_application_id,
                                   gv_process_name,
                                   'fn_find_max_batch_ctl_id',
                                   100,
                                   gv_source_system_id);

      p_step := 2;
      p_message := 'Inserting date range into TR_ETL_BATCH_CTL table ..';
      DBMS_OUTPUT.put_line (p_message || CHR (10));

      SELECT process_end_date
        INTO lv_start_date
        FROM tr_etl_batch_ctl
       WHERE etl_batch_ctl_id = lv_batch_ctl_id;

      IF TRUNC (lv_start_date) = TRUNC (gv_sysdate - 1)
      THEN
       /*
       Do not change the status of the schedule if completed already.
       This change was made to prevent the data duplication issue if the job
       re-run for a particular day.
       prc_upd_etl_batch_ctl (gv_application_id,
                                gv_process_name,
                                'prc_upd_etl_batch_ctl',
                                0,
                                lv_batch_ctl_id);
       */
       p_user_def_error_message := 'Data already exists.';
       raise lv_data_exists;
       
      ELSE
      
         INSERT INTO tr_etl_batch_ctl (etl_batch_ctl_id,
                                       process_run_date,
                                       process_start_date,
                                       process_end_date,
                                       status_id,
                                       job_name,
                                       source_system_id,
                                       created_date,
                                       created_by)
                             VALUES (  seq_etl_batch_ctl_id.NEXTVAL,
                                       gv_sysdate,
                                       lv_start_date,
                                       TRUNC (gv_sysdate - 1)
                                       + INTERVAL '23' HOUR
                                       + INTERVAL '59' MINUTE
                                       + INTERVAL '59' SECOND,
                                       0,
                                       pv_process_name_i,
                                       gv_source_system_id,
                                       TRUNC (gv_sysdate),
                                       pv_process_name_i);
                                       
      END IF;
      
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
                                           p_user_def_error_message,
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
            
   END prc_ins_etl_batch_ctl;

/***************************************************************************************************
   -- Procedure Name: prc_ins_order_request
   --
   -- Created by: Roopesh
   --
   -- Objective: This procedure will insert data to tr_order_request table
   --
   --
   -- Modifications:
   -- Date:        Created By:                Description:
   -- ------------ ----------------------- -------------------------
   -- 02/20/2018   Roopesh                      Created.
   -----------------------------------------------------------------
***************************************************************************************************/

   PROCEDURE prc_ins_order_request (
      pv_application_id_i   IN ex_error_log.application_id%TYPE,
      pv_process_name_i     IN ex_error_log.process_name%TYPE,
      pv_function_name_i    IN ex_error_log.function_name%TYPE)
   AS
   BEGIN
      --
      --
      p_step := 1;
      p_message := 'Insert data into TR_ORDER_REQUEST table ..';
      DBMS_OUTPUT.put_line (p_message || CHR (10));

                    MERGE INTO TR_ORDER_REQUEST tro
                         USING (SELECT order_header_id                  ,
                                       billing_customer_id              ,
                                       application_id                   ,
                                       affiliate_id                     ,
                                       store_id                         ,
                                       partner_order_date               ,
                                       affiliate_key                    ,
                                       web_promo_id                     ,
                                       partner_order_id                 ,
                                       source_system_id                 ,
                                       source_create_date               
                                  FROM LS_ST_OMS_ORDER_REQUEST) aa
                            ON (tro.order_id = aa.order_header_id)
                    WHEN NOT MATCHED
                    THEN
                       INSERT     (tr_order_request_id                  ,
                                   order_id                             ,
                                   customer_billing_address_id          ,
                                   application_id                       ,
                                   affiliate_id                         ,
                                   store_id                             ,
                                   order_date                           ,
                                   affiliate_key                        ,
                                   web_promo_id                         ,
                                   partner_order_id                     ,
                                   created_date                         ,
                                   created_by                           ,
                                   modified_date                        ,
                                   modified_by                          ,
                                   source_system_id                     ,
                                   source_create_date                   )
                           VALUES (seq_order_req_trans_id.NEXTVAL       ,
                                   aa.order_header_id                   ,
                                   aa.billing_customer_id               ,
                                   aa.application_id                    ,
                                   aa.affiliate_id                      ,
                                   aa.store_id                          ,
                                   aa.partner_order_date                ,
                                   aa.affiliate_key                     ,
                                   aa.web_promo_id                      ,
                                   aa.partner_order_id                  ,
                                   SYSDATE                              ,
                                   USER                                 ,
                                   SYSDATE                              ,
                                   USER                                 ,
                                   aa.source_system_id                  ,
                                   aa.source_create_date                );
                                   
                                  
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
            
   END prc_ins_order_request;

/***************************************************************************************************
   -- Procedure Name: prc_ins_cust_order_request
   --
   -- Created by: Roopesh
   --
   -- Objective: This procedure will insert data to tr_customer_order_request table
   --
   --
   -- Modifications:
   -- Date:        Created By:                Description:
   -- ------------ ----------------------- -------------------------
   -- 02/20/2018   Roopesh                      Created.
   -----------------------------------------------------------------
***************************************************************************************************/

   PROCEDURE prc_ins_cust_order_request (
      pv_application_id_i   IN ex_error_log.application_id%TYPE,
      pv_process_name_i     IN ex_error_log.process_name%TYPE,
      pv_function_name_i    IN ex_error_log.function_name%TYPE)
   AS
   BEGIN
   
      p_step := 1;
      p_message := 'Insert data into TR_CUSTOMER_ORDER_REQUEST table ..';
      DBMS_OUTPUT.put_line (p_message || CHR (10));

      INSERT INTO TR_CUSTOMER_ORDER_REQUEST (tr_cust_order_request_id   ,
                                             order_id                   ,
                                             order_date                 ,
                                             first_name                 ,
                                             middle_name                ,
                                             last_name                  ,
                                             street_address1            ,
                                             street_address2            ,
                                             city                       ,
                                             state                      ,
                                             zip                        ,
                                             phone                      ,
                                             phone_extension            ,
                                             email_address              ,
                                             date_entered               ,
                                             customer_address_id        ,
                                             cc_token                   ,
                                             created_date               ,
                                             created_by                 ,
                                             modified_date              ,
                                             modified_by                ,
                                             source_system_id)
                                 SELECT      seq_cust_order_req_trans_id.NEXTVAL,
                                             order_header_id            ,
                                             order_date                 ,
                                             first_name                 ,
                                             middle_name                ,
                                             last_name                  ,
                                             street_address1            ,
                                             street_address2            ,
                                             city                       ,
                                             state                      ,
                                             zip                        ,
                                             phone                      ,
                                             phone_ext                  ,
                                             email_address              ,
                                             date_entered               ,
                                             customer_address_id        ,
                                             cc_token                   ,
                                             SYSDATE                    ,
                                             USER                       ,
                                             SYSDATE                    ,
                                             USER                       ,
                                             source_system_id
                                        FROM LS_ST_OMS_CUST_ORDER_REQUEST ;
                                      
                                     
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
            
   END prc_ins_cust_order_request;

/***************************************************************************************************
   -- Procedure Name: prc_ins_order_item_request
   --
   -- Created by: Roopesh
   --
   -- Objective: This procedure will insert data to tr_order_item_request table
   --
   --
   -- Modifications:
   -- Date:        Created By:                Description:
   -- ------------ ----------------------- -------------------------
   -- 02/20/2018   Roopesh                      Created.
   -----------------------------------------------------------------
***************************************************************************************************/
   
   PROCEDURE prc_ins_ord_item_request (
      pv_application_id_i   IN ex_error_log.application_id%TYPE,
      pv_process_name_i     IN ex_error_log.process_name%TYPE,
      pv_function_name_i    IN ex_error_log.function_name%TYPE)
   AS
   BEGIN
   
      p_step := 1;
      p_message := 'Insert data into TR_ORDER_ITEM_REQUEST table ..';
      DBMS_OUTPUT.put_line (p_message || CHR (10));
      
      
    INSERT  INTO TR_ORDER_ITEM_REQUEST  (tr_order_item_request_id       ,
                                         order_id                       ,
                                         line_item                      ,
                                         order_date                     ,
                                         currency_id                    ,
                                         product_id                     ,
                                         new_sub_source_code            ,
                                         new_sub_printed_product_code   ,
                                         quantity_ordered               ,
                                         order_item_price               ,
                                         customer_shipping_address_id   ,
                                         valid_flag                     ,
                                         created_date                   ,
                                         created_by                     ,
                                         modified_date                  ,
                                         modified_by                    ,                                         
                                         source_system_id               ,
                                         source_cd                      ,
                                         offer_id                       ,
                                         item_key)                                
                                SELECT   seq_order_item_req_trans_id.NEXTVAL,
                                         order_header_id                ,
                                         line_item                      ,
                                         partner_order_date             ,
                                         2 currency_id                  ,  ---Need to get this value
                                         product_id                     ,
                                         printed_source_code            ,
                                         printed_product_code           ,
                                         quantity                       ,
                                         price                          ,
                                         shipping_customer_id           ,
                                         NULL valid_flag                ,
                                         SYSDATE                        ,
                                         USER                           ,
                                         SYSDATE                        ,
                                         USER                           ,                                         
                                         source_system_id               ,
                                         NULL source_code               ,
                                         offer_id                       ,
                                         item_key
                                    FROM LS_ST_OMS_ORDER_REQUEST        ;

      
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
            
   END prc_ins_ord_item_request;

/***************************************************************************************************
   -- Procedure Name: prc_ins_reject_item_hist
   --
   -- Created by: Roopesh
   --
   -- Objective: This procedure will insert data to tr_reject_item_hist table
   --
   --
   -- Modifications:
   -- Date:        Created By:                Description:
   -- ------------ ----------------------- -------------------------
   -- 02/20/2018   Roopesh                      Created.
   -----------------------------------------------------------------
***************************************************************************************************/

   PROCEDURE prc_ins_reject_item_hist (
      pv_application_id_i   IN ex_error_log.application_id%TYPE,
      pv_process_name_i     IN ex_error_log.process_name%TYPE,
      pv_function_name_i    IN ex_error_log.function_name%TYPE)
   AS
   BEGIN
      --
      p_step := 1;
      p_message := 'Insert data into TR_REJECTED_ITEM_HIST table ..';
      DBMS_OUTPUT.put_line (p_message || CHR (10));

            INSERT INTO TR_REJECTED_ITEM (rejected_item_id            ,
                                          order_id                    ,
                                          line_item                   ,
                                          reject_code                 ,
                                          reject_reason               ,
                                          reject_date                 ,
                                          printed_source_cd           ,
                                          order_status_id             ,
                                          client_name                 ,
                                          offer_id                    ,
                                          prod_id                     ,
                                          prod_desc                   ,
                                          created_date                ,
                                          source_system_id)                                          
                                  SELECT seq_rej_item_trans_id.NEXTVAL,
                                          ri.order_id                 ,
                                          ri.line_item                ,
                                          ri.reject_code              ,
                                          ri.reject_reason            ,
                                          ri.reject_date              ,
                                          ri.printed_source_code      ,
                                          ri.order_status_id          ,
                                          oor.client_name             ,
                                          oor.offer_id                ,
                                          oor.product_id              ,
                                          oor.product_description     ,  
                                          ri.created_date             ,
                                          ri.source_system_id
                                     FROM LS_ST_OMS_REJECTED_ITEM  RI ,
                                          LS_ST_OMS_ORDER_REQUEST  OOR
                                    WHERE ri.order_id  =  oor.order_header_id 
                                      AND ri.line_item =  oor.line_item;
                                     
                                     
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
            
   END prc_ins_reject_item_hist;

/***************************************************************************************************
   -- Procedure name: prc_ins_order_property
   --
   -- Created by: Roopesh
   --
   -- Objective: This procedure will insert OMS Order level properties into the TR_ORDER_PROPERTY table
   --
   --
   -- Modifications:
   -- Date:        Created By:                Description:
   -- ------------ ----------------------- -------------------------
   -- 02/20/2018   Roopesh                      Created.
   -----------------------------------------------------------------
***************************************************************************************************/

   PROCEDURE prc_ins_order_property (
      pv_application_id_i   IN ex_error_log.application_id%TYPE,
      pv_process_name_i     IN ex_error_log.process_name%TYPE,
      pv_function_name_i    IN ex_error_log.function_name%TYPE)
   AS
   BEGIN
      --
      p_step := 1;
      --
      p_message := 'Insert data into TR_ORDER_PROPERTY table ..';
      DBMS_OUTPUT.put_line (p_message || CHR (10));

      INSERT INTO TR_ORDER_PROPERTY (client_user_id         ,
                                     order_id               ,
                                     affiliate_id           ,
                                     affliate_cd            ,
                                     associate_id           ,
                                     file_name              ,
                                     member_id              ,
                                     oe_batch_id            ,
                                     oe_control_no          ,
                                     oe_order_no            ,
                                     oe_user_id             ,
                                     register_number        ,
                                     sales_associate        ,
                                     segment                ,
                                     sessionid              ,
                                     store_id               ,
                                     swat_client_id         ,
                                     swat_segment_id        ,
                                     transaction_id         ,
                                     upsell_items           ,
                                     webpromoid             ,
                                     tr_order_request_id    ,
                                     created_by             ,
                                     created_date           ,
                                     source_system_id       ,
                                     card_type              ,
                                     chain                  ,
                                     gender                 ,
                                     partner_order_id       ,
                                     source                 ,
                                     medium                 ,
                                     device_type            ,
                                     ind1                   ,
                                     ind2                   ,
                                     oc1                    ,
                                     oc2                    ,
                                     oc3                    ,
                                     csize                  ,
                                     piq_response           ,
                                     context                ,
                                     age_id                 ,
                                     creative_version_id    ,
                                     cookie_id              ,
                                     test_id                ,
                                     flow_id                ,
                                     respondent_id          ,
                                     education_id           ,
                                     origin_id              ,
                                     income_id              ,
                                     lead_gen)
                              SELECT lst.client_user_id     ,
                                     lst.order_id           ,                                     
                                     lst.affiliate_id       ,
                                     lst.affliate_cd        ,
                                     lst.associate_id       ,
                                     lst.file_name          ,
                                     lst.member_id          ,
                                     lst.oe_batch_id        ,
                                     lst.oe_control_no      ,
                                     lst.oe_order_no        ,
                                     lst.oe_user_id         ,
                                     lst.register_number    ,
                                     lst.sales_associate    ,
                                     lst.segment            ,
                                     lst.sessionid          ,
                                     lst.store_id           ,
                                     lst.swat_client_id     ,
                                     lst.swat_segment_id    ,
                                     lst.transaction_id     ,
                                     lst.upsell_items       ,
                                     lst.webpromoid         ,
                                     tro.tr_order_request_id,
                                     USER                   ,
                                     SYSDATE                ,
                                     lst.source_system_id   ,
                                     lst.card_type          ,
                                     lst.chain              ,
                                     lst.gender             ,
                                     lst.partner_order_id   ,
                                     lst.source             ,
                                     lst.medium             ,
                                     lst.device_type        ,
                                     lst.ind1               ,
                                     lst.ind2               ,
                                     lst.oc1                ,
                                     lst.oc2                ,
                                     lst.oc3                ,
                                     lst.csize              ,
                                     lst.piq_response       ,
                                     lst.context            ,
                                     lst.age_id             ,
                                     lst.creative_version_id,
                                     lst.cookie_id          ,
                                     lst.test_id            ,
                                     lst.flow_id            ,
                                     lst.respondent_id      ,
                                     lst.education_id       ,
                                     lst.origin_id          ,
                                     lst.income_id          ,
                                     lst.lead_gen
                                FROM LS_ST_OMS_ORDER_PROPERTY LST, 
                                     TR_ORDER_REQUEST         TRO
                               WHERE lst.order_id = tro.order_id;
                              
                              
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
            
   END prc_ins_order_property;

/***************************************************************************************************
   -- Procedure name: prc_ins_order_item_property
   --
   -- Created by: Roopesh
   --
   -- Objective: This procedure will insert OMS Order level properties into the TR_ORDER_ITEM_PROPERTY table
   --
   --
   -- Modifications:
   -- Date:        Created By:                Description:
   -- ------------ ----------------------- -------------------------
   -- 02/20/2018   Roopesh                      Created.
   -----------------------------------------------------------------
***************************************************************************************************/

   PROCEDURE prc_ins_order_item_property (
      pv_application_id_i   IN ex_error_log.application_id%TYPE,
      pv_process_name_i     IN ex_error_log.process_name%TYPE,
      pv_function_name_i    IN ex_error_log.function_name%TYPE)
   AS
   BEGIN
      p_step := 1;
      --
      p_message := 'Insert data into TR_ORDER_ITEM_PROPERTY table ..';
      DBMS_OUTPUT.put_line (p_message || CHR (10));

      INSERT INTO TR_ORDER_ITEM_PROPERTY (client_user_id            ,
                                          order_id                  ,
                                          line_item                 ,
                                          amazon_shipment_id        ,
                                          amazon_asin               ,
                                          amazon_sub_ref_id         ,
                                          amazon_orig_sub_ref_id    ,
                                          tr_order_item_request_id  ,
                                          created_date              ,
                                          created_by                ,
                                          source_system_id          ,
                                          price_paid                ,
                                          lead_gen_product)
                          SELECT          lst.client_user_id        ,
                                          lst.order_id              ,
                                          lst.line_item             ,
                                          lst.amazon_shipment_id    ,
                                          lst.amazon_asin           ,
                                          lst.amazon_sub_ref_id     ,
                                          lst.amazon_orig_sub_ref_id,
                                          tro.tr_order_item_request_id  ,
                                          SYSDATE                   ,
                                          USER                      ,
                                          lst.source_system_id      ,
                                          lst.price_paid            ,
                                          lst.lead_gen_product
                                     FROM LS_ST_OMS_ORDER_ITEM_PROPERTY LST, 
                                          TR_ORDER_ITEM_REQUEST         TRO
                                    WHERE lst.order_id  = tro.order_id 
                                      AND lst.line_item = tro.line_item;
                          
                          
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
            
   END prc_ins_order_item_property;
   
/***************************************************************************************************
   -- Procedure Name: prc_run_complete
   --
   -- Created by: Roopesh
   --
   -- Objective: This procedure will update the status 
   --
   --
   -- Modifications:
   -- Date:        Created By:                Description:
   -- ------------ ----------------------- -------------------------
   -- 02/20/2018   Roopesh                      Created.
   -----------------------------------------------------------------
***************************************************************************************************/

   PROCEDURE prc_run_complete (
      pv_application_id_i   IN ex_error_log.application_id%TYPE,
      pv_process_name_i     IN ex_error_log.process_name%TYPE,
      pv_function_name_i    IN ex_error_log.function_name%TYPE
                              )
      
   AS
   
      lv_batch_ctl_id   NUMBER;
      
   BEGIN
      --
      p_step := 1;
      p_message :=
         'Capture MAX (etl_batch_ctl_id) from TR_ETL_BATCH_CTL table ..';

      DBMS_OUTPUT.put_line (p_message || CHR (10));

         lv_batch_ctl_id :=
         fn_find_max_batch_ctl_id (gv_application_id,
                                   gv_process_name,
                                   'fn_find_max_batch_ctl_id',
                                   0,
                                   gv_source_system_id);

      p_step := 2;
      p_message := 'Updating TR_ETL_BATCH_CTL table ..';

      DBMS_OUTPUT.put_line (p_message || CHR (10));

      prc_upd_etl_batch_ctl (gv_application_id,
                             gv_process_name,
                             'prc_upd_etl_batch_ctl',
                             100,
                             lv_batch_ctl_id);
                             
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
                                         
   END prc_run_complete;   

END pkg_oms_etl;
/