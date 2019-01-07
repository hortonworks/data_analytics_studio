/*
 *
 * HORTONWORKS DATAPLANE SERVICE AND ITS CONSTITUENT SERVICES
 *
 * (c) 2016-2018 Hortonworks, Inc. All rights reserved.
 *
 * This code is provided to you pursuant to your written agreement with Hortonworks, which may be the terms of the
 * Affero General Public License version 3 (AGPLv3), or pursuant to a written agreement with a third party authorized
 * to distribute this code.  If you do not have a written agreement with Hortonworks or with an authorized and
 * properly licensed third party, you do not have any rights to this code.
 *
 * If this code is provided to you under the terms of the AGPLv3:
 * (A) HORTONWORKS PROVIDES THIS CODE TO YOU WITHOUT WARRANTIES OF ANY KIND;
 * (B) HORTONWORKS DISCLAIMS ANY AND ALL EXPRESS AND IMPLIED WARRANTIES WITH RESPECT TO THIS CODE, INCLUDING BUT NOT
 *   LIMITED TO IMPLIED WARRANTIES OF TITLE, NON-INFRINGEMENT, MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE;
 * (C) HORTONWORKS IS NOT LIABLE TO YOU, AND WILL NOT DEFEND, INDEMNIFY, OR HOLD YOU HARMLESS FOR ANY CLAIMS ARISING
 *   FROM OR RELATED TO THE CODE; AND
 * (D) WITH RESPECT TO YOUR EXERCISE OF ANY RIGHTS GRANTED TO YOU FOR THE CODE, HORTONWORKS IS NOT LIABLE FOR ANY
 *   DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, PUNITIVE OR CONSEQUENTIAL DAMAGES INCLUDING, BUT NOT LIMITED TO,
 *   DAMAGES RELATED TO LOST REVENUE, LOST PROFITS, LOSS OF INCOME, LOSS OF BUSINESS ADVANTAGE OR UNAVAILABILITY,
 *   OR LOSS OR CORRUPTION OF DATA.
 *
 */

import { moduleForComponent, test } from 'ember-qunit';
import hbs from 'htmlbars-inline-precompile';
import Ember from 'ember';

moduleForComponent('query-editor', 'Integration | Component | query editor', {
  integration: true
});

let selectedTablesModels = [{dbName: "default", id : "1", isSelected : true}, {dbName: "simple", id : "1", isSelected : true},
  {dbName: "test", id : "1", isSelected : true}, {dbName: "example", id : "1", isSelected : true}];
let alldatabases = [];
alldatabases.push(Ember.Object.create({
  name: 'default'
}),Ember.Object.create({
  name: 'simple'
}),Ember.Object.create({
  name: 'sample'
}),Ember.Object.create({
  name: 'test'
}));
const includeHeaders = true;
let dbTables = [{"dbname":"default","tables":[{"column":{"id":"store_sales","headerTitle":"store_sales"},"name":"store_sales","facets":[{"value":"ss_net_paid_inc_tax"},{"value":"ss_hdemo_sk"},{"value":"ss_ext_tax"},{"value":"ss_net_paid"},{"value":"ss_ext_wholesale_cost"},{"value":"ss_customer_sk"},{"value":"ss_sales_price"},{"value":"ss_quantity"},{"value":"ss_list_price"},{"value":"ss_coupon_amt"},{"value":"ss_sold_date_sk"},{"value":"ss_item_sk"},{"value":"ss_ext_list_price"},{"value":"ss_wholesale_cost"},{"value":"ss_ticket_number"},{"value":"ss_addr_sk"},{"value":"ss_net_profit"},{"value":"ss_sold_time_sk"},{"value":"ss_cdemo_sk"},{"value":"ss_promo_sk"},{"value":"ss_ext_discount_amt"},{"value":"ss_ext_sales_price"}],"columns":["ss_net_paid_inc_tax","ss_hdemo_sk","ss_ext_tax","ss_net_paid","ss_ext_wholesale_cost","ss_customer_sk","ss_sales_price","ss_quantity","ss_list_price","ss_coupon_amt","ss_sold_date_sk","ss_item_sk","ss_ext_list_price","ss_wholesale_cost","ss_ticket_number","ss_addr_sk","ss_net_profit","ss_sold_time_sk","ss_cdemo_sk","ss_promo_sk","ss_ext_discount_amt","ss_ext_sales_price"]},{"column":{"id":"customer_demographics","headerTitle":"customer_demographics"},"name":"customer_demographics","facets":[{"value":"cd_gender"},{"value":"cd_marital_status"},{"value":"cd_dep_college_count"},{"value":"cd_purchase_estimate"},{"value":"cd_demo_sk"},{"value":"cd_credit_rating"},{"value":"cd_dep_count"},{"value":"cd_dep_employed_count"}],"columns":["cd_gender","cd_marital_status","cd_dep_college_count","cd_purchase_estimate","cd_demo_sk","cd_credit_rating","cd_dep_count","cd_dep_employed_count"]},{"column":{"id":"customer","headerTitle":"customer"},"name":"customer","facets":[{"value":"c_first_shipto_date_sk"},{"value":"c_email_address"},{"value":"c_birth_day"},{"value":"c_last_review_date"},{"value":"c_birth_month"},{"value":"c_birth_country"},{"value":"c_customer_id"},{"value":"c_last_name"},{"value":"c_current_addr_sk"},{"value":"c_current_hdemo_sk"},{"value":"c_first_name"},{"value":"c_current_cdemo_sk"},{"value":"c_login"},{"value":"c_customer_sk"},{"value":"c_first_sales_date_sk"},{"value":"c_salutation"},{"value":"c_preferred_cust_flag"}],"columns":["c_first_shipto_date_sk","c_email_address","c_birth_day","c_last_review_date","c_birth_month","c_birth_country","c_customer_id","c_last_name","c_current_addr_sk","c_current_hdemo_sk","c_first_name","c_current_cdemo_sk","c_login","c_customer_sk","c_first_sales_date_sk","c_salutation","c_preferred_cust_flag"]},{"column":{"id":"web_page","headerTitle":"web_page"},"name":"web_page","facets":[{"value":"wp_rec_end_date"},{"value":"wp_access_date_sk"},{"value":"wp_max_ad_count"},{"value":"wp_link_count"},{"value":"wp_creation_date_sk"},{"value":"wp_type"},{"value":"wp_image_count"},{"value":"wp_rec_start_date"},{"value":"wp_url"},{"value":"wp_autogen_flag"},{"value":"wp_customer_sk"},{"value":"wp_web_page_sk"},{"value":"wp_web_page_id"},{"value":"wp_char_count"}],"columns":["wp_rec_end_date","wp_access_date_sk","wp_max_ad_count","wp_link_count","wp_creation_date_sk","wp_type","wp_image_count","wp_rec_start_date","wp_url","wp_autogen_flag","wp_customer_sk","wp_web_page_sk","wp_web_page_id","wp_char_count"]},{"column":{"id":"item","headerTitle":"item"},"name":"item","facets":[{"value":"i_rec_end_date"},{"value":"i_color"},{"value":"i_product_name"},{"value":"i_units"},{"value":"i_brand"},{"value":"i_category_id"},{"value":"i_category"},{"value":"i_item_id"},{"value":"i_class_id"},{"value":"i_class"},{"value":"i_manager_id"},{"value":"i_item_sk"},{"value":"i_rec_start_date"},{"value":"i_manufact"},{"value":"i_size"},{"value":"i_current_price"},{"value":"i_brand_id"},{"value":"i_formulation"},{"value":"i_wholesale_cost"},{"value":"i_manufact_id"},{"value":"i_container"},{"value":"i_item_desc"}],"columns":["i_rec_end_date","i_color","i_product_name","i_units","i_brand","i_category_id","i_category","i_item_id","i_class_id","i_class","i_manager_id","i_item_sk","i_rec_start_date","i_manufact","i_size","i_current_price","i_brand_id","i_formulation","i_wholesale_cost","i_manufact_id","i_container","i_item_desc"]},{"column":{"id":"time_dim","headerTitle":"time_dim"},"name":"time_dim","facets":[{"value":"t_time_sk"},{"value":"t_minute"},{"value":"t_second"},{"value":"t_am_pm"},{"value":"t_time"},{"value":"t_meal_time"},{"value":"t_sub_shift"},{"value":"t_time_id"},{"value":"t_shift"},{"value":"t_hour"}],"columns":["t_time_sk","t_minute","t_second","t_am_pm","t_time","t_meal_time","t_sub_shift","t_time_id","t_shift","t_hour"]},{"column":{"id":"store_returns","headerTitle":"store_returns"},"name":"store_returns","facets":[{"value":"sr_return_time_sk"},{"value":"sr_customer_sk"},{"value":"sr_return_quantity"},{"value":"sr_store_sk"},{"value":"sr_addr_sk"},{"value":"sr_return_amt_inc_tax"},{"value":"sr_cdemo_sk"},{"value":"sr_return_amt"},{"value":"sr_store_credit"},{"value":"sr_fee"},{"value":"sr_return_tax"},{"value":"sr_refunded_cash"},{"value":"sr_item_sk"},{"value":"sr_reason_sk"},{"value":"sr_net_loss"},{"value":"sr_ticket_number"},{"value":"sr_returned_date_sk"},{"value":"sr_hdemo_sk"},{"value":"sr_return_ship_cost"},{"value":"sr_reversed_charge"}],"columns":["sr_return_time_sk","sr_customer_sk","sr_return_quantity","sr_store_sk","sr_addr_sk","sr_return_amt_inc_tax","sr_cdemo_sk","sr_return_amt","sr_store_credit","sr_fee","sr_return_tax","sr_refunded_cash","sr_item_sk","sr_reason_sk","sr_net_loss","sr_ticket_number","sr_returned_date_sk","sr_hdemo_sk","sr_return_ship_cost","sr_reversed_charge"]},{"column":{"id":"income_band","headerTitle":"income_band"},"name":"income_band","facets":[{"value":"ib_upper_bound"},{"value":"ib_income_band_sk"},{"value":"ib_lower_bound"}],"columns":["ib_upper_bound","ib_income_band_sk","ib_lower_bound"]},{"column":{"id":"call_center","headerTitle":"call_center"},"name":"call_center","facets":[{"value":"cc_gmt_offset"},{"value":"cc_street_type"},{"value":"cc_suite_number"},{"value":"cc_rec_start_date"},{"value":"cc_employees"},{"value":"cc_manager"},{"value":"cc_county"},{"value":"cc_mkt_id"},{"value":"cc_state"},{"value":"cc_company_name"},{"value":"cc_call_center_id"},{"value":"cc_sq_ft"},{"value":"cc_country"},{"value":"cc_tax_percentage"},{"value":"cc_call_center_sk"},{"value":"cc_name"},{"value":"cc_street_number"},{"value":"cc_mkt_desc"},{"value":"cc_open_date_sk"},{"value":"cc_hours"},{"value":"cc_division"},{"value":"cc_closed_date_sk"},{"value":"cc_rec_end_date"},{"value":"cc_street_name"},{"value":"cc_zip"},{"value":"cc_mkt_class"},{"value":"cc_company"},{"value":"cc_market_manager"},{"value":"cc_division_name"},{"value":"cc_city"},{"value":"cc_class"}],"columns":["cc_gmt_offset","cc_street_type","cc_suite_number","cc_rec_start_date","cc_employees","cc_manager","cc_county","cc_mkt_id","cc_state","cc_company_name","cc_call_center_id","cc_sq_ft","cc_country","cc_tax_percentage","cc_call_center_sk","cc_name","cc_street_number","cc_mkt_desc","cc_open_date_sk","cc_hours","cc_division","cc_closed_date_sk","cc_rec_end_date","cc_street_name","cc_zip","cc_mkt_class","cc_company","cc_market_manager","cc_division_name","cc_city","cc_class"]},{"column":{"id":"web_site","headerTitle":"web_site"},"name":"web_site","facets":[{"value":"web_close_date_sk"},{"value":"web_name"},{"value":"web_rec_end_date"},{"value":"web_open_date_sk"},{"value":"web_street_type"},{"value":"web_gmt_offset"},{"value":"web_mkt_desc"},{"value":"web_site_id"},{"value":"web_county"},{"value":"web_tax_percentage"},{"value":"web_rec_start_date"},{"value":"web_street_number"},{"value":"web_state"},{"value":"web_site_sk"},{"value":"web_city"},{"value":"web_manager"},{"value":"web_mkt_id"},{"value":"web_company_id"},{"value":"web_country"},{"value":"web_company_name"},{"value":"web_zip"},{"value":"web_class"},{"value":"web_market_manager"},{"value":"web_mkt_class"},{"value":"web_suite_number"},{"value":"web_street_name"}],"columns":["web_close_date_sk","web_name","web_rec_end_date","web_open_date_sk","web_street_type","web_gmt_offset","web_mkt_desc","web_site_id","web_county","web_tax_percentage","web_rec_start_date","web_street_number","web_state","web_site_sk","web_city","web_manager","web_mkt_id","web_company_id","web_country","web_company_name","web_zip","web_class","web_market_manager","web_mkt_class","web_suite_number","web_street_name"]},{"column":{"id":"warehouse","headerTitle":"warehouse"},"name":"warehouse","facets":[{"value":"w_warehouse_id"},{"value":"w_gmt_offset"},{"value":"w_warehouse_sk"},{"value":"w_country"},{"value":"w_street_number"},{"value":"w_suite_number"},{"value":"w_warehouse_sq_ft"},{"value":"w_street_type"},{"value":"w_county"},{"value":"w_city"},{"value":"w_state"},{"value":"w_street_name"},{"value":"w_warehouse_name"},{"value":"w_zip"}],"columns":["w_warehouse_id","w_gmt_offset","w_warehouse_sk","w_country","w_street_number","w_suite_number","w_warehouse_sq_ft","w_street_type","w_county","w_city","w_state","w_street_name","w_warehouse_name","w_zip"]},{"column":{"id":"catalog_sales","headerTitle":"catalog_sales"},"name":"catalog_sales","facets":[{"value":"cs_bill_customer_sk"},{"value":"cs_sales_price"},{"value":"cs_wholesale_cost"},{"value":"cs_ship_hdemo_sk"},{"value":"cs_bill_addr_sk"},{"value":"cs_ship_cdemo_sk"},{"value":"cs_ship_customer_sk"},{"value":"cs_ship_mode_sk"},{"value":"cs_ext_sales_price"},{"value":"cs_order_number"},{"value":"cs_ext_wholesale_cost"},{"value":"cs_net_paid_inc_ship_tax"},{"value":"cs_coupon_amt"},{"value":"cs_bill_hdemo_sk"},{"value":"cs_net_paid_inc_ship"},{"value":"cs_bill_cdemo_sk"},{"value":"cs_item_sk"},{"value":"cs_net_profit"},{"value":"cs_catalog_page_sk"},{"value":"cs_ext_tax"},{"value":"cs_ship_addr_sk"},{"value":"cs_ext_discount_amt"},{"value":"cs_sold_date_sk"},{"value":"cs_net_paid"},{"value":"cs_list_price"},{"value":"cs_ext_ship_cost"},{"value":"cs_promo_sk"},{"value":"cs_sold_time_sk"},{"value":"cs_net_paid_inc_tax"},{"value":"cs_quantity"},{"value":"cs_ext_list_price"},{"value":"cs_ship_date_sk"},{"value":"cs_warehouse_sk"}],"columns":["cs_bill_customer_sk","cs_sales_price","cs_wholesale_cost","cs_ship_hdemo_sk","cs_bill_addr_sk","cs_ship_cdemo_sk","cs_ship_customer_sk","cs_ship_mode_sk","cs_ext_sales_price","cs_order_number","cs_ext_wholesale_cost","cs_net_paid_inc_ship_tax","cs_coupon_amt","cs_bill_hdemo_sk","cs_net_paid_inc_ship","cs_bill_cdemo_sk","cs_item_sk","cs_net_profit","cs_catalog_page_sk","cs_ext_tax","cs_ship_addr_sk","cs_ext_discount_amt","cs_sold_date_sk","cs_net_paid","cs_list_price","cs_ext_ship_cost","cs_promo_sk","cs_sold_time_sk","cs_net_paid_inc_tax","cs_quantity","cs_ext_list_price","cs_ship_date_sk","cs_warehouse_sk"]},{"column":{"id":"store","headerTitle":"store"},"name":"store","facets":[{"value":"s_store_sk"},{"value":"s_street_type"},{"value":"s_gmt_offset"},{"value":"s_store_id"},{"value":"s_market_manager"},{"value":"s_manager"},{"value":"s_hours"},{"value":"s_country"},{"value":"s_market_desc"},{"value":"s_division_id"},{"value":"s_street_name"},{"value":"s_store_name"},{"value":"s_suite_number"},{"value":"s_rec_start_date"},{"value":"s_market_id"},{"value":"s_company_name"},{"value":"s_street_number"},{"value":"s_tax_precentage"},{"value":"s_number_employees"},{"value":"s_floor_space"},{"value":"s_rec_end_date"},{"value":"s_state"},{"value":"s_zip"},{"value":"s_division_name"},{"value":"s_city"},{"value":"s_closed_date_sk"},{"value":"s_county"},{"value":"s_company_id"},{"value":"s_geography_class"}],"columns":["s_store_sk","s_street_type","s_gmt_offset","s_store_id","s_market_manager","s_manager","s_hours","s_country","s_market_desc","s_division_id","s_street_name","s_store_name","s_suite_number","s_rec_start_date","s_market_id","s_company_name","s_street_number","s_tax_precentage","s_number_employees","s_floor_space","s_rec_end_date","s_state","s_zip","s_division_name","s_city","s_closed_date_sk","s_county","s_company_id","s_geography_class"]},{"column":{"id":"reason","headerTitle":"reason"},"name":"reason","facets":[{"value":"r_reason_id"},{"value":"r_reason_sk"},{"value":"r_reason_desc"}],"columns":["r_reason_id","r_reason_sk","r_reason_desc"]},{"column":{"id":"inventory","headerTitle":"inventory"},"name":"inventory","facets":[{"value":"inv_item_sk"},{"value":"inv_warehouse_sk"},{"value":"inv_date_sk"},{"value":"inv_quantity_on_hand"}],"columns":["inv_item_sk","inv_warehouse_sk","inv_date_sk","inv_quantity_on_hand"]},{"column":{"id":"catalog_returns","headerTitle":"catalog_returns"},"name":"catalog_returns","facets":[{"value":"cr_refunded_addr_sk"},{"value":"cr_returning_customer_sk"},{"value":"cr_returned_time_sk"},{"value":"cr_ship_mode_sk"},{"value":"cr_return_tax"},{"value":"cr_reversed_charge"},{"value":"cr_order_number"},{"value":"cr_refunded_hdemo_sk"},{"value":"cr_call_center_sk"},{"value":"cr_refunded_cdemo_sk"},{"value":"cr_returning_cdemo_sk"},{"value":"cr_return_ship_cost"},{"value":"cr_refunded_customer_sk"},{"value":"cr_item_sk"},{"value":"cr_returning_hdemo_sk"},{"value":"cr_returned_date_sk"},{"value":"cr_catalog_page_sk"},{"value":"cr_return_amt_inc_tax"},{"value":"cr_fee"},{"value":"cr_net_loss"},{"value":"cr_store_credit"},{"value":"cr_reason_sk"},{"value":"cr_return_amount"},{"value":"cr_returning_addr_sk"},{"value":"cr_return_quantity"},{"value":"cr_warehouse_sk"},{"value":"cr_refunded_cash"}],"columns":["cr_refunded_addr_sk","cr_returning_customer_sk","cr_returned_time_sk","cr_ship_mode_sk","cr_return_tax","cr_reversed_charge","cr_order_number","cr_refunded_hdemo_sk","cr_call_center_sk","cr_refunded_cdemo_sk","cr_returning_cdemo_sk","cr_return_ship_cost","cr_refunded_customer_sk","cr_item_sk","cr_returning_hdemo_sk","cr_returned_date_sk","cr_catalog_page_sk","cr_return_amt_inc_tax","cr_fee","cr_net_loss","cr_store_credit","cr_reason_sk","cr_return_amount","cr_returning_addr_sk","cr_return_quantity","cr_warehouse_sk","cr_refunded_cash"]},{"column":{"id":"date_dim","headerTitle":"date_dim"},"name":"date_dim","facets":[{"value":"d_date_sk"},{"value":"d_fy_year"},{"value":"d_year"},{"value":"d_qoy"},{"value":"d_same_day_lq"},{"value":"d_fy_quarter_seq"},{"value":"d_current_year"},{"value":"d_quarter_name"},{"value":"d_date"},{"value":"d_month_seq"},{"value":"d_weekend"},{"value":"d_day_name"},{"value":"d_same_day_ly"},{"value":"d_fy_week_seq"},{"value":"d_current_week"},{"value":"d_date_id"},{"value":"d_following_holiday"},{"value":"d_holiday"},{"value":"d_current_quarter"},{"value":"d_dow"},{"value":"d_moy"},{"value":"d_first_dom"},{"value":"d_current_month"},{"value":"d_week_seq"},{"value":"d_current_day"},{"value":"d_dom"},{"value":"d_last_dom"},{"value":"d_quarter_seq"}],"columns":["d_date_sk","d_fy_year","d_year","d_qoy","d_same_day_lq","d_fy_quarter_seq","d_current_year","d_quarter_name","d_date","d_month_seq","d_weekend","d_day_name","d_same_day_ly","d_fy_week_seq","d_current_week","d_date_id","d_following_holiday","d_holiday","d_current_quarter","d_dow","d_moy","d_first_dom","d_current_month","d_week_seq","d_current_day","d_dom","d_last_dom","d_quarter_seq"]},{"column":{"id":"web_returns","headerTitle":"web_returns"},"name":"web_returns","facets":[{"value":"wr_refunded_cdemo_sk"},{"value":"wr_refunded_customer_sk"},{"value":"wr_returned_date_sk"},{"value":"wr_return_tax"},{"value":"wr_web_page_sk"},{"value":"wr_return_quantity"},{"value":"wr_net_loss"},{"value":"wr_returning_cdemo_sk"},{"value":"wr_reversed_charge"},{"value":"wr_returned_time_sk"},{"value":"wr_account_credit"},{"value":"wr_item_sk"},{"value":"wr_return_ship_cost"},{"value":"wr_reason_sk"},{"value":"wr_return_amt_inc_tax"},{"value":"wr_return_amt"},{"value":"wr_refunded_cash"},{"value":"wr_returning_hdemo_sk"},{"value":"wr_returning_addr_sk"},{"value":"wr_refunded_addr_sk"},{"value":"wr_returning_customer_sk"},{"value":"wr_fee"},{"value":"wr_refunded_hdemo_sk"},{"value":"wr_order_number"}],"columns":["wr_refunded_cdemo_sk","wr_refunded_customer_sk","wr_returned_date_sk","wr_return_tax","wr_web_page_sk","wr_return_quantity","wr_net_loss","wr_returning_cdemo_sk","wr_reversed_charge","wr_returned_time_sk","wr_account_credit","wr_item_sk","wr_return_ship_cost","wr_reason_sk","wr_return_amt_inc_tax","wr_return_amt","wr_refunded_cash","wr_returning_hdemo_sk","wr_returning_addr_sk","wr_refunded_addr_sk","wr_returning_customer_sk","wr_fee","wr_refunded_hdemo_sk","wr_order_number"]},{"column":{"id":"customer_address","headerTitle":"customer_address"},"name":"customer_address","facets":[{"value":"ca_address_id"},{"value":"ca_country"},{"value":"ca_address_sk"},{"value":"ca_gmt_offset"},{"value":"ca_state"},{"value":"ca_location_type"},{"value":"ca_street_name"},{"value":"ca_street_type"},{"value":"ca_suite_number"},{"value":"ca_city"},{"value":"ca_zip"},{"value":"ca_street_number"},{"value":"ca_county"}],"columns":["ca_address_id","ca_country","ca_address_sk","ca_gmt_offset","ca_state","ca_location_type","ca_street_name","ca_street_type","ca_suite_number","ca_city","ca_zip","ca_street_number","ca_county"]},{"column":{"id":"web_sales","headerTitle":"web_sales"},"name":"web_sales","facets":[{"value":"ws_bill_addr_sk"},{"value":"ws_list_price"},{"value":"ws_web_page_sk"},{"value":"ws_sales_price"},{"value":"ws_ext_list_price"},{"value":"ws_ext_discount_amt"},{"value":"ws_net_paid_inc_ship_tax"},{"value":"ws_item_sk"},{"value":"ws_ship_hdemo_sk"},{"value":"ws_ext_wholesale_cost"},{"value":"ws_ext_tax"},{"value":"ws_warehouse_sk"},{"value":"ws_ship_cdemo_sk"},{"value":"ws_sold_date_sk"},{"value":"ws_bill_hdemo_sk"},{"value":"ws_ship_customer_sk"},{"value":"ws_wholesale_cost"},{"value":"ws_coupon_amt"},{"value":"ws_bill_customer_sk"},{"value":"ws_ext_ship_cost"},{"value":"ws_net_paid"},{"value":"ws_net_profit"},{"value":"ws_bill_cdemo_sk"},{"value":"ws_ship_date_sk"},{"value":"ws_quantity"},{"value":"ws_ext_sales_price"},{"value":"ws_promo_sk"},{"value":"ws_net_paid_inc_ship"},{"value":"ws_net_paid_inc_tax"},{"value":"ws_ship_addr_sk"},{"value":"ws_sold_time_sk"},{"value":"ws_ship_mode_sk"},{"value":"ws_order_number"}],"columns":["ws_bill_addr_sk","ws_list_price","ws_web_page_sk","ws_sales_price","ws_ext_list_price","ws_ext_discount_amt","ws_net_paid_inc_ship_tax","ws_item_sk","ws_ship_hdemo_sk","ws_ext_wholesale_cost","ws_ext_tax","ws_warehouse_sk","ws_ship_cdemo_sk","ws_sold_date_sk","ws_bill_hdemo_sk","ws_ship_customer_sk","ws_wholesale_cost","ws_coupon_amt","ws_bill_customer_sk","ws_ext_ship_cost","ws_net_paid","ws_net_profit","ws_bill_cdemo_sk","ws_ship_date_sk","ws_quantity","ws_ext_sales_price","ws_promo_sk","ws_net_paid_inc_ship","ws_net_paid_inc_tax","ws_ship_addr_sk","ws_sold_time_sk","ws_ship_mode_sk","ws_order_number"]},{"column":{"id":"promotion","headerTitle":"promotion"},"name":"promotion","facets":[{"value":"p_promo_sk"},{"value":"p_promo_name"},{"value":"p_channel_dmail"},{"value":"p_channel_radio"},{"value":"p_item_sk"},{"value":"p_response_target"},{"value":"p_promo_id"},{"value":"p_cost"},{"value":"p_channel_event"},{"value":"p_purpose"},{"value":"p_channel_details"},{"value":"p_channel_tv"},{"value":"p_discount_active"},{"value":"p_end_date_sk"},{"value":"p_channel_press"},{"value":"p_channel_email"},{"value":"p_channel_demo"},{"value":"p_channel_catalog"},{"value":"p_start_date_sk"}],"columns":["p_promo_sk","p_promo_name","p_channel_dmail","p_channel_radio","p_item_sk","p_response_target","p_promo_id","p_cost","p_channel_event","p_purpose","p_channel_details","p_channel_tv","p_discount_active","p_end_date_sk","p_channel_press","p_channel_email","p_channel_demo","p_channel_catalog","p_start_date_sk"]},{"column":{"id":"household_demographics","headerTitle":"household_demographics"},"name":"household_demographics","facets":[{"value":"hd_vehicle_count"},{"value":"hd_dep_count"},{"value":"hd_demo_sk"},{"value":"hd_income_band_sk"},{"value":"hd_buy_potential"}],"columns":["hd_vehicle_count","hd_dep_count","hd_demo_sk","hd_income_band_sk","hd_buy_potential"]},{"column":{"id":"ship_mode","headerTitle":"ship_mode"},"name":"ship_mode","facets":[{"value":"sm_ship_mode_id"},{"value":"sm_contract"},{"value":"sm_carrier"},{"value":"sm_code"},{"value":"sm_type"},{"value":"sm_ship_mode_sk"}],"columns":["sm_ship_mode_id","sm_contract","sm_carrier","sm_code","sm_type","sm_ship_mode_sk"]},{"column":{"id":"catalog_page","headerTitle":"catalog_page"},"name":"catalog_page","facets":[{"value":"cp_start_date_sk"},{"value":"cp_end_date_sk"},{"value":"cp_catalog_number"},{"value":"cp_catalog_page_id"},{"value":"cp_catalog_page_sk"},{"value":"cp_department"},{"value":"cp_type"},{"value":"cp_catalog_page_number"},{"value":"cp_description"}],"columns":["cp_start_date_sk","cp_end_date_sk","cp_catalog_number","cp_catalog_page_id","cp_catalog_page_sk","cp_department","cp_type","cp_catalog_page_number","cp_description"]}]}]

function findTableCount(includeHeaders) {
	let tableCount = dbTables[0].tables.length;
	if(includeHeaders) {
		return tableCount+1;
	} else {
		return tableCount;
	}
}

function findColumnsInTable(databaseName, tableName, dbTables) {
	let columns;
	for(let i in dbTables) {
		let tables = dbTables[i];
		if(tables.dbname !== databaseName) {
			continue;
		}
		let tablesArray = tables.tables;
		for(let j in tablesArray) {
			if(tablesArray[j].name === tableName) {
				columns = tablesArray[j].columns;
				break;
			}
		}
	}
	return columns;
}

function findDatabaseCount(selectedTablesModels) {
	let databaseCount = selectedTablesModels.length;
	if(includeHeaders) {
		return databaseCount+1;
	} else {
		return databaseCount;
	}
}

function initTestCaseEnv(self, queryText) {
  Ember.set(self, "allUDFList", undefined);
  Ember.set(self, "selectedDb", "default");
  Ember.set(self, "selectedTablesModels", selectedTablesModels);
  Ember.set(self, "selectedMultiDb", ["default"]);
  Ember.set(self, "alldatabases", alldatabases);
  Ember.set(self, "highlightedText", "");
  Ember.set(self, "query", queryText);
}

test('List databases', function(assert) {
  let queryText = "use ";
  let cm = {
    getValue: function () {
      return queryText;
    },
    getCursor: function () {
      return {ch:4, line:0};
    },
    getLine: function () {
      return queryText;
    }
  };
  initTestCaseEnv(this, queryText);
  let lastWord = "store_sales";
  this.render(hbs`{{query-editor}}`);

  assert.equal(Ember.getOwner(this).lookup('component:query-editor').formCustomSuggestions(dbTables, lastWord, cm, alldatabases).length, 5 );
});

test('should show table count', function(assert) {

  let queryText = "select * from ;";
  let cm = {
    getValue: function () {
      return queryText;
    },
    getCursor: function () {
      return {ch:14, line:0};
    },
    getLine: function () {
      return queryText;
    }
  };
  initTestCaseEnv(this, queryText);

  let lastWord = "store_sales";
  this.render(hbs`{{query-editor}}`);

  assert.equal(Ember.getOwner(this).lookup('component:query-editor').formCustomSuggestions(dbTables, lastWord, cm, alldatabases).length, findTableCount(includeHeaders));
});

test('should table column count', function(assert) {

  let lastWord = "store_sales";

  let queryText = `select  from ${lastWord};`;
  let cm = {
    getValue: function () {
      return queryText;
    },
    getCursor: function () {
      return {ch:7, line:0};
    },
    getLine: function () {
      return queryText;
    }
  };
  initTestCaseEnv(this, queryText);

  this.render(hbs`{{query-editor}}`);
  assert.equal(Ember.getOwner(this).lookup('component:query-editor').formCustomSuggestions(dbTables, lastWord, cm, alldatabases).length, 68);
});

test('Load keyword', function(assert) {

  let queryText = "load ";
  let cm = {
    getValue: function () {
      return queryText;
    },
    getCursor: function () {
      return {ch:5, line:0};
    },
    getLine: function () {
      return queryText;
    }
  };

  let lastWord = "";
  this.render(hbs`{{query-editor}}`);
  assert.equal(Ember.getOwner(this).lookup('component:query-editor').formCustomSuggestions(dbTables, lastWord, cm, alldatabases).length, 3);
});

test('List databases', function(assert) {

  let queryText = "load ";
  let cm = {
    getValue: function () {
      return queryText;
    },
    getCursor: function () {
      return {ch:5, line:0};
    },
    getLine: function () {
      return queryText;
    }
  };
  initTestCaseEnv(this, queryText);

  let lastWord = "";
  this.render(hbs`{{query-editor}}`);
  assert.equal(Ember.getOwner(this).lookup('component:query-editor').formCustomSuggestions(dbTables, lastWord, cm, alldatabases).length, 3);
});

test('Analyze table', function(assert) {

  let queryText = "Analyze table ";
  let cm = {
    getValue: function () {
      return queryText;
    },
    getCursor: function () {
      return {ch:14, line:0};
    },
    getLine: function () {
      return queryText;
    }
  };
  initTestCaseEnv(this, queryText);

  let lastWord = "";
  this.render(hbs`{{query-editor}}`);
  assert.equal(Ember.getOwner(this).lookup('component:query-editor').formCustomSuggestions(dbTables, lastWord, cm, alldatabases).length, findTableCount(includeHeaders));
});

test('MSCK REPAIR TABLE', function(assert) {

  let queryText = "MSCK REPAIR TABLE ";
  let cm = {
    getValue: function () {
      return queryText;
    },
    getCursor: function () {
      return {ch:18, line:0};
    },
    getLine: function () {
      return queryText;
    }
  };
  initTestCaseEnv(this, queryText);

  let lastWord = "";
  this.render(hbs`{{query-editor}}`);
  assert.equal(Ember.getOwner(this).lookup('component:query-editor').formCustomSuggestions(dbTables, lastWord, cm, alldatabases).length, findTableCount(includeHeaders));
});

test("LOAD DATA INPATH 'hdfs://user/admin'", function(assert) {

  let queryText = "LOAD DATA INPATH 'hdfs://user/admin' ";
  let cm = {
    getValue: function () {
      return queryText;
    },
    getCursor: function () {
      return {ch:38, line:0};
    },
    getLine: function () {
      return queryText;
    }
  };
  initTestCaseEnv(this, queryText);

  let lastWord = "";
  this.render(hbs`{{query-editor}}`);
  assert.equal(Ember.getOwner(this).lookup('component:query-editor').formCustomSuggestions(dbTables, lastWord, cm, alldatabases).length, 3);
});

test("LOAD DATA INPATH 'hdfs://user/admin' INTO TABLE", function(assert) {

  let queryText = "LOAD DATA INPATH 'hdfs://user/admin' INTO TABLE ";
  let cm = {
    getValue: function () {
      return queryText;
    },
    getCursor: function () {
      return {ch:48, line:0};
    },
    getLine: function () {
      return queryText;
    }
  };
  initTestCaseEnv(this, queryText);

  let lastWord = "";
  this.render(hbs`{{query-editor}}`);
  assert.equal(Ember.getOwner(this).lookup('component:query-editor').formCustomSuggestions(dbTables, lastWord, cm, alldatabases).length,  findTableCount(includeHeaders));
});

test("DROP TABLE IF EXISTS", function(assert) {

  let queryText = "DROP TABLE IF EXISTS ";
  let cm = {
    getValue: function () {
      return queryText;
    },
    getCursor: function () {
      return {ch:21, line:0};
    },
    getLine: function () {
      return queryText;
    }
  };
  initTestCaseEnv(this, queryText);

  let lastWord = "";
  this.render(hbs`{{query-editor}}`);
  assert.equal(Ember.getOwner(this).lookup('component:query-editor').formCustomSuggestions(dbTables, lastWord, cm, alldatabases).length,  findTableCount(includeHeaders));
});

test("see keywords", function(assert) {

  let queryText = " ";
  let cm = {
    getValue: function () {
      return queryText;
    },
    getCursor: function () {
      return {ch:1, line:0};
    },
    getLine: function () {
      return queryText;
    }
  };
  initTestCaseEnv(this, queryText);

  let lastWord = "";
  this.render(hbs`{{query-editor}}`);
  assert.equal(Ember.getOwner(this).lookup('component:query-editor').formCustomSuggestions(dbTables, lastWord, cm, alldatabases).length, 26);
});

test("EXPLAIN ", function(assert) {

  let queryText = "EXPLAIN ";
  let cm = {
    getValue: function () {
      return queryText;
    },
    getCursor: function () {
      return {ch:8, line:0};
    },
    getLine: function () {
      return queryText;
    }
  };
  initTestCaseEnv(this, queryText);

  let lastWord = "";
  this.render(hbs`{{query-editor}}`);
  assert.equal(Ember.getOwner(this).lookup('component:query-editor').formCustomSuggestions(dbTables, lastWord, cm, alldatabases).length, 27);
});

test("ALTER TABLE   ", function(assert) {
  let lastWord = "store_sales";

  let queryText = "ALTER TABLE ${lastWord} ";
  let cm = {
    getValue: function () {
      return queryText;
    },
    getCursor: function () {
      return {ch:25, line:0};
    },
    getLine: function () {
      return queryText;
    }
  };
  initTestCaseEnv(this, queryText);

  this.render(hbs`{{query-editor}}`);
  assert.equal(Ember.getOwner(this).lookup('component:query-editor').formCustomSuggestions(dbTables, lastWord, cm, alldatabases).length, 30);
});

test("Select * from tablename ", function(assert) {
  let lastWord = "store_sales";

  let queryText = "Select * from ${lastWord} ";
  let cm = {
    getValue: function () {
      return queryText;
    },
    getCursor: function () {
      return {ch:26, line:0};
    },
    getLine: function () {
      return queryText;
    }
  };
  initTestCaseEnv(this, queryText);

  this.render(hbs`{{query-editor}}`);
  assert.equal(Ember.getOwner(this).lookup('component:query-editor').formCustomSuggestions(dbTables, lastWord, cm, alldatabases).length, 24);
});

test("Multiple tables in select ", function(assert) {
  let lastWord = "store_sales";

  let queryText = "Select * from ${lastWord},  ${lastWord}, ";
  let cm = {
    getValue: function () {
      return queryText;
    },
    getCursor: function () {
      return {ch:40, line:0};
    },
    getLine: function () {
      return queryText;
    }
  };
  initTestCaseEnv(this, queryText);

  this.render(hbs`{{query-editor}}`);
  assert.equal(Ember.getOwner(this).lookup('component:query-editor').formCustomSuggestions(dbTables, lastWord, cm, alldatabases).length, findTableCount(includeHeaders));
  assert.equal(Ember.getOwner(this).lookup('component:query-editor').formCustomSuggestions(dbTables, lastWord, cm, alldatabases).length, findTableCount(includeHeaders));
});

test("Multiple tables in select without spaces", function(assert) {
  let lastWord = "store_sales";

  let queryText = "Select * from ${lastWord},  ${lastWord}, ";

  let cm = {
    getValue: function () {
      return queryText;
    },
    getCursor: function () {
      return {ch:40, line:0};
    },
    getLine: function () {
      return queryText;
    }
  };
  initTestCaseEnv(this, queryText);

  this.render(hbs`{{query-editor}}`);
  assert.equal(Ember.getOwner(this).lookup('component:query-editor').formCustomSuggestions(dbTables, lastWord, cm, alldatabases).length, findTableCount(includeHeaders));

  queryText = "Select * from ${lastWord},  ${lastWord}, ";

  cm = {
    getValue: function () {
      return queryText;
    },
    getCursor: function () {
      return {ch:41, line:0};
    },
    getLine: function () {
      return queryText;
    }
  };
  assert.equal(Ember.getOwner(this).lookup('component:query-editor').formCustomSuggestions(dbTables, lastWord, cm, alldatabases).length, findTableCount(includeHeaders));


});

test("Multiple line simple query tablenames", function(assert) {
  let lastWord = "store_sales";

  let queryText = `SELECT

* 

FROM `;

  let cm = {
    getValue: function () {
      return queryText;
    },
    getCursor: function () {
      return {ch:17, line:0};
    },
    getLine: function () {
      return queryText;
    }
  };
  initTestCaseEnv(this, queryText);

  this.render(hbs`{{query-editor}}`);
  assert.equal(Ember.getOwner(this).lookup('component:query-editor').formCustomSuggestions(dbTables, lastWord, cm, alldatabases).length, findTableCount(includeHeaders));
});

test("Table alias and column alias checks", function(assert) {
  let lastWord = "store_sales";

  let queryText = `select ss. from ${lastWord} ss`;

  let cm = {
    getValue: function () {
      return queryText;
    },
    getCursor: function () {
      return {ch:10, line:0};
    },
    getLine: function () {
      return queryText;
    }
  };
  initTestCaseEnv(this, queryText);

  this.render(hbs`{{query-editor}}`);
  assert.equal(Ember.getOwner(this).lookup('component:query-editor').formCustomSuggestions(dbTables, lastWord, cm, alldatabases).length, findColumnsInTable(Ember.get(this, 'selectedDb') ,lastWord, dbTables).length);
});



// test("Multiple line simple query column names", function(assert) {
//   let lastWord = "store_sales";

//   let queryText = `SELECT



// FROM ${lastWord}`;
//   console.log(queryText);
//   console.log(queryText.length);
//   let cm = {
//     getValue: function () {
//       return queryText;
//     },
//     getCursor: function () {
//       return {ch:7, line:1};
//     },
//     getLine: function () {
//       return queryText;
//     }
//   };
//   Ember.set(this, "highlightedText", "");
//   Ember.set(this, "query", queryText);


//   this.render(hbs`{{query-editor}}`);
//   //console.log(Ember.getOwner(this).lookup('component:query-editor').formCustomSuggestions(dbTables, lastWord, cm, alldatabases));
//   assert.equal(Ember.getOwner(this).lookup('component:query-editor').formCustomSuggestions(dbTables, lastWord, cm, alldatabases).length, findTableCount(includeHeaders));
// });
