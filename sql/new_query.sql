-----------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------
-- this query is saved in table hbgdwc.svd_interface_booking_v6, we will use that table as main query
-----------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------

select *
--into hbgdwc.david_kpis_normal_20170510
from
(
with hotel as
(select hv.grec_Seq_rec, hv.rres_seq_reserva, hv.ghor_seq_hotel, h.seq_hotel, h.izge_cod_destino, di1.ides_cod_destino, di1.nom_destino, di1.sidi_cod_idioma,
    row_number () over (partition by hv.grec_Seq_rec, hv.rres_seq_reserva order by grec_Seq_rec, rres_seq_reserva, ghor_seq_hotel) as rownum
  from (select grec_Seq_rec, rres_seq_reserva, ghor_seq_hotel from hbgdwc.dwc_bok_t_hotel_sale) hv
    inner join (select seq_hotel, izge_cod_destino from hbgdwc.dwc_mtd_t_hotel) h on (h.seq_hotel = hv.ghor_seq_hotel)
    inner join (select ides_cod_destino, nom_destino, sidi_cod_idioma from hbgdwc.dwc_itn_t_internet_destination_id) di1 on (di1.ides_cod_destino = h.izge_cod_destino AND di1.sidi_cod_idioma  = 'ENG')
),

oth_con as
(select o.seq_rec_other, o.seq_reserva, o.nom_contrato, o.ind_tipo_otro, o.fec_desde_other, c.seq_rec, c.nom_contrato, c.cod_destino, c.ind_tipo_otro, c.fec_desde, c.fec_hasta, di2.ides_cod_Destino, di2.sidi_cod_idioma, di2.nom_destino,
    row_number () over (partition by o.seq_rec_other, o.seq_reserva order by o.seq_rec_other, o.seq_reserva, o.nom_contrato) as rownum
  from (select seq_rec as seq_rec_other, seq_reserva, nom_contrato, ind_tipo_otro, fec_desde as fec_desde_other from hbgdwc.dwc_bok_t_other) o
    inner join (select seq_rec, nom_contrato, cod_destino, ind_tipo_otro, fec_desde, fec_hasta from hbgdwc.dwc_con_t_contract_other) c on (c.seq_rec = o.seq_rec_other AND c.nom_contrato = o.nom_contrato AND c.ind_tipo_otro = o.ind_tipo_otro AND o.fec_desde_other BETWEEN c.fec_desde AND c.fec_hasta)
    inner join (select ides_cod_Destino, sidi_cod_idioma, nom_destino from hbgdwc.dwc_itn_t_internet_destination_id) di2 on (di2.ides_cod_Destino = c.cod_destino AND di2.sidi_cod_idioma  = 'ENG')
),

reventa as
(select seq_rec, seq_reserva, ind_status from hbgdwc.dwc_cry_t_cancellation_recovery_res_resale
),

information as
(select i.seq_rec, i.seq_reserva, i.aplicacion,
    row_number () over (partition by i.seq_rec, i.seq_reserva) as rownum
  from hbgdwc.dwc_bok_t_booking_information i
  where i.tipo_op='A'
),

num_services as
(SELECT ri.grec_seq_rec, ri.rres_seq_reserva, COUNT(1) act_services
  FROM hbgdwc.dwc_bok_t_hotel_sale ri
  WHERE ri.fec_cancelacion is null
  group by ri.grec_seq_rec, ri.rres_seq_reserva
)

select cabecera.interface_id,--
       re.semp_cod_emp operative_company,--
       re.sofi_cod_ofi operative_office,--
       re.des_receptivo operative_office_desc,--
       cabecera.grec_seq_rec operative_incoming,--
       cabecera.seq_reserva booking_id,--
       cabecera.fint_cod_interface interface,--
       rf.semp_cod_emp invoicing_company,--
       rf.sofi_cod_ofi invoicing_office,--
       rf.seq_rec invoicing_incoming,--
       TRUNC (cabecera.creation_date) creation_date,--

       cabecera.creation_date creation_ts,--
       ri.min_fec_creacion first_booking_ts,--

       NVL(TO_CHAR(TRUNC (cabecera.fec_modifica),'yyyy-mm-dd'),'') modification_date,--
       NVL(TO_CHAR(cabecera.fec_modifica,'yyyy-mm-dd hh24:mi:ss'),'') modification_ts,--
       NVL(TO_CHAR(TRUNC (cabecera.fec_cancelacion),'yyyy-mm-dd'),'') cancellation_date,--
       NVL(TO_CHAR(cabecera.fec_cancelacion,'yyyy-mm-dd hh24:mi:ss'),'') cancellation_ts,--
       decode(cabecera.fec_cancelacion, null, 'N', 'S') cancelled_booking,--

       TRUNC(GREATEST(cabecera.creation_date, cabecera.fec_modifica, cabecera.fec_cancelacion)) status_date,--
       trunc(cabecera.fec_desde) booking_service_from,--

       trunc(cabecera.fec_hasta) booking_service_to,--
       t.seq_ttoo client_code,--
       t.nom_corto_ttoo customer_name,--XX
       NVL (cabecera.cod_pais_cliente, t.gpai_cod_pais_mercado) source_market,--XX
       p.cod_iso source_market_iso,--

       REPLACE(cabecera.nom_general,';','') holder,--XX

       cabecera.nro_ad num_adults,--
       cabecera.nro_ni num_childrens,--XX

       cabecera.gdep_cod_depart department_code,--XX
       cabecera.rtre_cod_tipo_res booking_type,--XX
       cabecera.ind_facturable_res invoicing_booking,--XX
       cabecera.ind_facturable_adm invoicing_admin,--XX
       NVL(cabecera.pct_comision,0) Client_commision_esp,
       NVL(cabecera.pct_rappel,0) client_override_esp,
       cabecera.ind_confirma confirmed_booking,

       decode (i.partner_ttoo, null, 'N', 'S') Partner_booking,
       NVL(cabecera.cod_divisa_p,'') Partner_booking_currency,
       NVL(cabecera.seq_ttoo_p,0) Partner_code,
       NVL(cabecera.cod_suc_p,0) Partner_brand,
       NVl(cabecera.seq_agencia_p,0) Partner_agency_code,
       NVL(cabecera.seq_sucursal_p,0) Partner_agency_brand,
       NVL(cabecera.seq_rec_expediente,0) Booking_file_incoming,
       NVL(cabecera.seq_res_expediente,0) booking_file_number,
       decode (cabecera.ind_tippag, null, 'Merchant', 'Pago en hotel') Accomodation_model,

       NVL ( hotel.izge_cod_destino || '-' || hotel.nom_destino, oth_con.cod_destino || '-' ||oth_con.nom_destino, 'NO_DESTINATION_CODE'  ) Destination_code,
       cabecera.gdiv_cod_divisa booking_currency,

       round(nvl(ttv.ttv, 0), nvl(mon.decimales, 2)) TTV_booking_currency,--ROUND
       round(nvl(ttv.ttv, 0) * nvl(exchange_rate(1.00, cabecera.gdiv_cod_divisa, 'EUR', trunc(cabecera.fec_creacion)::date), 0), 2) TTV_EUR_currency,--ROUND
--       nvl(ttv.ttv, 0)*nvl(tip.rate, 0) TTV_EUR_currency,


       round(nvl(tax_ttv.tax_ttv, 0), nvl(mon.decimales, 2)) tax_ttv,--ROUND
       round(nvl(tax_ttv.tax_ttv, 0) * nvl(exchange_rate(1.00, cabecera.gdiv_cod_divisa, 'EUR', trunc(cabecera.fec_creacion)::date), 0), 2) tax_ttv_eur,--ROUND
--       nvl(tax_ttv.tax_ttv, 0)*nvl(tip.rate, 0) tax_ttv_eur,

       round(nvl(tax_ttv.tax_ttv_toms, 0), nvl(mon.decimales, 2)) tax_ttv_toms,--ROUND
       round(nvl(tax_ttv.tax_ttv_toms, 0) * nvl(exchange_rate(1.00, cabecera.gdiv_cod_divisa, 'EUR', trunc(cabecera.fec_creacion)::date), 0), 2) Tax_TTV_EUR_TOMS,--ROUND
--       nvl(tax_ttv.tax_ttv_toms, 0)*nvl(tip.rate, 0) Tax_TTV_EUR_TOMS,

       null MISSING_CANCO_Tax_Sales_Transfer_pricing,
       null MISSING_CANCO_Tax_Sales_Transfer_pricing_EUR,
       null MISSING_CANCO_Transfer_pricing,
       null missing_canco_tax_transfer_pricing_eur,--XX
       null MISSING_CANCO_Tax_Cost_Transfer_pricing,
       null MISSING_CANCO_Tax_Cost_Transfer_pricing_EUR,

       round(nvl(cli_comm.cli_comm, 0), nvl(mon.decimales, 2)) Client_Commision,--ROUND
       round(nvl(cli_comm.cli_comm, 0) * nvl(exchange_rate(1.00, cabecera.gdiv_cod_divisa, 'EUR', trunc(cabecera.fec_creacion)::date), 0), 2) Client_EUR_Commision,--ROUND
--       nvl(cli_comm.cli_comm, 0)*nvl(tip.rate, 0) Client_EUR_Commision,

       round(nvl(tax_cli_comm.Tax_Client_Com, 0), nvl(mon.decimales, 2)) Tax_Client_commision,--ROUND
       round(nvl(tax_cli_comm.Tax_Client_Com, 0) * nvl(exchange_rate(1.00, cabecera.gdiv_cod_divisa, 'EUR', trunc(cabecera.fec_creacion)::date), 0), 2) Tax_Client_EUR_commision,--ROUND
--       nvl(tax_cli_comm.Tax_Client_Com, 0)*nvl(tip.rate, 0) Tax_Client_EUR_commision,

       round(nvl(cli_rappel.cli_rappel, 0), nvl(mon.decimales, 2)) client_rappel,--ROUND
       round(nvl(cli_rappel.cli_rappel, 0) * nvl(exchange_rate(1.00, cabecera.gdiv_cod_divisa, 'EUR', trunc(cabecera.fec_creacion)::date), 0), 2) Client_EUR_rappel,--ROUND
--       nvl(cli_rappel.cli_rappel, 0)*nvl(tip.rate, 0) Client_EUR_rappel,

       round(nvl(tax_cli_rappel.tax_cli_rappel, 0), nvl(mon.decimales, 2)) tax_client_rappel,--ROUND
       round(nvl(tax_cli_rappel.tax_cli_rappel, 0) * nvl(exchange_rate(1.00, cabecera.gdiv_cod_divisa, 'EUR', trunc(cabecera.fec_creacion)::date), 0), 2) tax_Client_EUR_rappel,--ROUND
--       nvl(tax_cli_rappel.tax_cli_rappel, 0)*nvl(tip.rate, 0) tax_Client_EUR_rappel,

--       0 as missing_cost_booking_currency, --XX OK: a transformar costes a divisa de BOOKING
       nvl(booking_cost.Booking_cost, 0) cost_booking_currency,--XX
       nvl(booking_cost.Booking_cost_EUR, 0) cost_eur_currency,--XX

--       0 as MISSING_tax_cost, --OK: a transformar costes a divisa de BOOKING
       nvl(tax_cost.tax_cost, 0) tax_cost,
       nvl(tax_cost.Tax_Cost_EUR, 0) tax_cost_EUR,
--       0 as MISSING_tax_cost_TOMS, --OK: a transformar costes a divisa de BOOKING
       nvl(tax_cost.tax_cost_TOMS, 0) tax_cost_TOMS,
       nvl(tax_cost.Tax_Cost_TOMS_EUR, 0) tax_cost_EUR_TOMS,

       inf.aplicacion Application,
       decode(
              decode(rev.ind_status,
                      'RR', 'R',
                      'CN', 'RL',
                      'RL', decode(cabecera.fec_cancelacion, null, 'RS', 'C'),
                      rev.ind_status),
              'RS', 'Reventa',
              'C', 'Cancelada',
              'RL', 'Liberada',
              'R', 'Reventa') canrec_status,

       round(nvl(GSA_comm.GSA_Comm, 0), nvl(mon.decimales, 2)) GSA_Commision,--ROUND
       round(nvl(GSA_comm.GSA_Comm, 0) * nvl(exchange_rate(1.00, cabecera.gdiv_cod_divisa, 'EUR', trunc(cabecera.fec_creacion)::date), 0), 2) GSA_EUR_Commision,--ROUND
--       nvl(GSA_comm.GSA_Comm, 0)*nvl(tip.rate, 0) GSA_EUR_Commision,

       round(nvl(GSA_comm.Tax_GSA_Comm, 0), nvl(mon.decimales, 2)) Tax_GSA_Commision,--ROUND
       round(nvl(GSA_comm.Tax_GSA_Comm, 0) * nvl(exchange_rate(1.00, cabecera.gdiv_cod_divisa, 'EUR', trunc(cabecera.fec_creacion)::date), 0), 2) Tax_GSA_EUR_Commision,--ROUND
--       nvl(GSA_comm.Tax_GSA_Comm, 0)*nvl(tip.rate, 0) Tax_GSA_EUR_Commision,

--       0 MISSING_Agency_commision_hotel_payment, --OK: a transformar costes a divisa de BOOKING
--       0 MISSING_Tax_Agency_commision_hotel_pay, --OK: a transformar costes a divisa de BOOKING
       nvl(age_com_hot_pay.Agency_commision_hotel_payment, 0) Agency_commision_hotel_payment,--XX
       nvl(age_com_hot_pay.Agency_commision_hotel_payment_EUR, 0) agency_comm_hotel_pay_eur,--XX
       nvl(age_com_hot_pay.Tax_Agency_commision_hotel_pay, 0) Tax_Agency_commision_hotel_pay,--XX
       nvl(age_com_hot_pay.Tax_Agency_commision_hotel_pay_EUR, 0) tax_agency_comm_hotel_pay_eur,--XX

--       0 MISSING_Fix_override_hotel_payment, --OK: a transformar costes a divisa de BOOKING
--       0 MISSING_Tax_Fix_override_hotel_pay, --OK: a transformar costes a divisa de BOOKING
       nvl(fix_over_hot_pay.Fix_override_hotel_payment, 0) Fix_override_hotel_payment,--XX
       nvl(fix_over_hot_pay.Fix_override_hotel_payment_EUR, 0) fix_override_hotel_pay_eur,--XX
       nvl(fix_over_hot_pay.Tax_Fix_override_hotel_pay, 0) Tax_Fix_override_hotel_pay,--XX
       nvl(fix_over_hot_pay.Tax_Fix_override_hotel_pay_EUR, 0) tax_fix_overr_hotel_pay_eur,--XX

--       0 MISSING_Var_override_hotel_payment, --OK: a transformar costes a divisa de BOOKING
--       0 MISSING_Tax_Var_override_hotel_pay, --OK: a transformar costes a divisa de BOOKING
       nvl(var_over_hot_pay.Var_override_hotel_payment, 0) Var_override_hotel_payment,--XX
       nvl(var_over_hot_pay.Var_override_hotel_payment_EUR, 0) var_override_hotel_pay_eur,--XX
       nvl(var_over_hot_pay.Tax_Var_override_hotel_pay, 0) Tax_Var_override_hotel_pay,
       nvl(var_over_hot_pay.Tax_Var_override_hotel_pay_EUR, 0) Tax_Var_override_hotel_pay_EUR,

--       0 MISSING_Hotel_commision_hotel_payment, --OK: a transformar costes a divisa de BOOKING
--       0 MISSING_Tax_Hotel_commision_Hotel_pay, --OK: a transformar costes a divisa de BOOKING
       nvl(hot_comm_hot_pay.Hotel_commision_hotel_payment, 0) Hotel_commision_hotel_payment,--XX
       nvl(hot_comm_hot_pay.Hotel_commision_hotel_payment_EUR, 0) hotel_commision_hotel_pay_eur,--XX
       nvl(hot_comm_hot_pay.Tax_Hotel_commision_Hotel_pay, 0) Tax_Hotel_commision_Hotel_pay,--XX
       nvl(hot_comm_hot_pay.Tax_Hotel_commision_Hotel_pay_EUR, 0) tax_hotel_comm_hotel_pay_eur,--XX

--       0 MISSING_Marketing_contribution, --OK: a transformar costes a divisa de BOOKING
--       0 MISSING_Tax_marketing_contribution, --OK: a transformar costes a divisa de BOOKING
       nvl(marketing_contrib.Marketing_contribution, 0) Marketing_contribution,
       nvl(marketing_contrib.Marketing_contribution_EUR, 0) Marketing_contribution_EUR,
       nvl(marketing_contrib.Tax_marketing_contribution, 0) Tax_marketing_contribution,
       nvl(marketing_contrib.Tax_marketing_contribution_EUR, 0) Tax_marketing_contribution_EUR,

--       0 MISSING_bank_expenses, --OK: a transformar costes a divisa de BOOKING
--       0 MISSING_Tax_Bank_expenses, --OK: a transformar costes a divisa de BOOKING
       nvl(bank_exp.bank_expenses, 0) bank_expenses,
       nvl(bank_exp.bank_expenses_EUR, 0) bank_expenses_EUR,
       nvl(bank_exp.Tax_Bank_expenses, 0) Tax_Bank_expenses,
       nvl(bank_exp.Tax_Bank_expenses_EUR, 0) Tax_Bank_expenses_EUR,

--       0 MISSING_Platform_Fee, --OK: a transformar costes a divisa de BOOKING
--       0 MISSING_Tax_Platform_fee, --OK: a transformar costes a divisa de BOOKING
       nvl(platform_fee.Platform_Fee, 0) Platform_Fee,
       nvl(platform_fee.Platform_Fee_EUR, 0) Platform_Fee_EUR,
       nvl(platform_fee.Tax_Platform_fee, 0) Tax_Platform_fee,
       nvl(platform_fee.Tax_Platform_fee_EUR, 0) Tax_Platform_fee_EUR,

--       0 MISSING_credit_card_fee, --OK: a transformar costes a divisa de BOOKING
--       0 missing_tax_credit_card_fee, --XX OK: a transformar costes a divisa de BOOKING
       nvl(credit_card_fee.credit_card_fee, 0) credit_card_fee,
       nvl(credit_card_fee.credit_card_fee_EUR, 0) credit_card_fee_EUR,
       nvl(credit_card_fee.Tax_credit_card_fee, 0) Tax_credit_card_fee,
       nvl(credit_card_fee.Tax_credit_card_fee_EUR, 0) Tax_credit_card_fee_EUR,

--       0 MISSING_Withholding, --OK: a transformar costes a divisa de BOOKING
--       0 MISSING_Tax_withholding, --OK: a transformar costes a divisa de BOOKING
       nvl(withholding.Withholding, 0) Withholding,
       nvl(withholding.Withholding_EUR, 0) Withholding_EUR,
       nvl(withholding.Tax_withholding, 0) Tax_withholding,
       nvl(withholding.Tax_withholding_EUR, 0) Tax_withholding_EUR,

--       0 MISSING_Local_Levy, --OK: a transformar costes a divisa de BOOKING
--       0 MISSING_Tax_Local_Levy, --OK: a transformar costes a divisa de BOOKING
       nvl(local_levy.Local_Levy, 0) Local_Levy,
       nvl(local_levy.Local_Levy_EUR, 0) Local_Levy_EUR,
       nvl(local_levy.Tax_Local_Levy, 0) Tax_Local_Levy,
       nvl(local_levy.Tax_Local_Levy_EUR, 0) Tax_Local_Levy_EUR,

--       0 MISSING_Partner_Third_commision, --OK: a transformar costes a divisa de BOOKING
--       0 MISSING_Tax_partner_third_commision, --OK: a transformar costes a divisa de BOOKING
       nvl(partner_third_comm.Partner_Third_commision, 0) Partner_Third_commision,--XX
       nvl(partner_third_comm.Partner_Third_commision_EUR, 0) partner_third_comm_eur,--XX
       nvl(partner_third_comm.Tax_partner_third_commision, 0) Tax_partner_third_commision,--XX
       nvl(partner_third_comm.Tax_partner_third_commision_EUR, 0) tax_partner_third_comm_eur,--XX

       nvl(num_services.act_services, 0) NUMBER_ACTIVE_ACC_SERV,

       cabecera.ind_tipo_credito cod_credit_type,--
       cabecera.cod_pais_cliente cod_tag_nationality,--
       cabecera.ref_ttoo name_ref_age,--
       cabecera.nom_agente cod_agent--


----

from
(select grec_Seq_rec, seq_reserva, gdiv_cod_divisa booking_currency, fec_creacion, fec_creacion creation_date, grec_seq_rec || '-' || seq_reserva interface_id, gtto_seq_ttoo, cod_pais_cliente, nro_ad, nro_ni,
  gdiv_cod_divisa, fec_modifica, fec_cancelacion, fec_desde, fint_cod_interface, seq_rec_hbeds, fec_hasta, nom_general, ind_tipo_credito, gdep_cod_depart, rtre_cod_tipo_res, ind_facturable_res,
  ind_facturable_adm, pct_comision, pct_rappel, ind_confirma, cod_divisa_p, seq_ttoo_p, cod_suc_p, seq_agencia_p, seq_sucursal_p, seq_rec_expediente, seq_res_expediente, ind_tippag,
  ref_ttoo, nom_agente
  from hbgdwc.dwc_bok_t_booking
  where trunc(fec_creacion) >= to_date('01/10/2016','dd/mm/yyyy')
    and seq_reserva>0
    ) cabecera
inner join hbgdwc.dwc_mtd_t_ttoo t on  cabecera.gtto_seq_ttoo = t.seq_ttoo
inner join hbgdwc.dwc_gen_t_general_country p on (nvl(cabecera.cod_pais_cliente, t.gpai_cod_pais_mercado) = p.cod_pais)

      left join hbgdwc.dwc_mtd_t_receptive re on cabecera.grec_Seq_rec  = re.seq_rec  -- re_t_ge_receptivo CAMBIAR A INNER
      left join hbgdwc.dwc_mtd_t_receptive rf on cabecera.seq_rec_hbeds = rf.seq_rec  -- re_t_ge_receptivo CAMBIAR A INNER
      left join hbgdwc.dwc_itf_t_fc_interface i on cabecera.fint_cod_interface = i.cod_interface      -- re_t_fc_interface CAMBIAR A INNER

left join reventa rev on (rev.seq_rec=cabecera.grec_seq_rec and rev.seq_reserva=cabecera.seq_reserva)
left join information inf on (inf.seq_rec=cabecera.grec_seq_rec and inf.seq_reserva=cabecera.seq_reserva and inf.rownum=1)

left join (SELECT ri.seq_rec, ri.seq_reserva, MIN(ri.fec_creacion) min_fec_creacion
                 FROM hbgdwc.dwc_bok_t_booking_information ri
                WHERE ri.tipo_op = 'A'
                group by ri.seq_rec, ri.seq_reserva) ri on ri.seq_rec = cabecera.grec_seq_rec AND ri.seq_reserva = cabecera.seq_reserva

left join hotel on (hotel.grec_Seq_rec = cabecera.grec_seq_rec AND hotel.rres_seq_reserva = cabecera.seq_reserva and hotel.rownum=1)
left join oth_con on (oth_con.seq_rec_other = cabecera.grec_seq_rec AND oth_con.seq_reserva = cabecera.seq_reserva and oth_con.rownum=1)

left join num_services on (num_services.grec_Seq_rec = cabecera.grec_seq_rec AND num_services.rres_seq_reserva = cabecera.seq_reserva)

left join hbgdwc.svd_david_ttv ttv on ttv.grec_seq_rec||'-'||ttv.rres_seq_reserva=cabecera.interface_id --
left join hbgdwc.svd_david_tax_ttv tax_ttv on tax_ttv.grec_seq_rec||'-'||tax_ttv.rres_seq_reserva=cabecera.interface_id --

left join hbgdwc.svd_david_client_com cli_comm on cli_comm.grec_seq_rec||'-'||cli_comm.rres_seq_reserva=cabecera.interface_id --
left join hbgdwc.svd_david_tax_client_com tax_cli_comm on tax_cli_comm.grec_seq_rec||'-'||tax_cli_comm.rres_seq_reserva=cabecera.interface_id --

left join hbgdwc.svd_david_client_rap cli_rappel on cli_rappel.grec_seq_rec||'-'||cli_rappel.rres_seq_reserva=cabecera.interface_id --
left join hbgdwc.svd_david_tax_client_rap tax_cli_rappel on tax_cli_rappel.grec_seq_rec||'-'||tax_cli_rappel.rres_seq_reserva=cabecera.interface_id --

left join hbgdwc.svd_david_cost_v2 booking_cost on booking_cost.grec_seq_rec||'-'||booking_cost.rres_seq_reserva=cabecera.interface_id --
left join hbgdwc.svd_david_tax_cost_v2 tax_cost on tax_cost.grec_seq_rec||'-'||tax_cost.rres_seq_reserva=cabecera.interface_id --

left join hbgdwc.svd_david_GSA_comm GSA_comm on GSA_comm.seq_rec||'-'||GSA_comm.seq_reserva=cabecera.interface_id --

left join hbgdwc.svd_david_age_comm_hot_pay_v2 age_com_hot_pay on age_com_hot_pay.grec_seq_rec||'-'||age_com_hot_pay.rres_seq_reserva=cabecera.interface_id --
left join hbgdwc.svd_david_fix_over_hot_pay_v2 fix_over_hot_pay on fix_over_hot_pay.grec_seq_rec||'-'||fix_over_hot_pay.rres_seq_reserva=cabecera.interface_id --
left join hbgdwc.svd_david_var_over_hot_pay_v2 var_over_hot_pay on var_over_hot_pay.grec_seq_rec||'-'||var_over_hot_pay.rres_seq_reserva=cabecera.interface_id --
left join hbgdwc.svd_david_hot_comm_hot_pay_v2 hot_comm_hot_pay on hot_comm_hot_pay.grec_seq_rec||'-'||hot_comm_hot_pay.rres_seq_reserva=cabecera.interface_id --
left join hbgdwc.svd_david_marketing_contrib_v2 marketing_contrib on marketing_contrib.grec_seq_rec||'-'||marketing_contrib.rres_seq_reserva=cabecera.interface_id --
left join hbgdwc.svd_david_bank_exp_v2 bank_exp on bank_exp.grec_seq_rec||'-'||bank_exp.rres_seq_reserva=cabecera.interface_id --
left join hbgdwc.svd_david_platform_fee_v2 platform_fee on platform_fee.grec_seq_rec||'-'||platform_fee.rres_seq_reserva=cabecera.interface_id --
left join hbgdwc.svd_david_credit_card_fee_v2 credit_card_fee on credit_card_fee.grec_seq_rec||'-'||credit_card_fee.rres_seq_reserva=cabecera.interface_id --
left join hbgdwc.svd_david_withholding_v2 withholding on withholding.grec_seq_rec||'-'||withholding.rres_seq_reserva=cabecera.interface_id --
left join hbgdwc.svd_david_local_levy_v2 local_levy on local_levy.grec_seq_rec||'-'||local_levy.rres_seq_reserva=cabecera.interface_id --
left join hbgdwc.svd_david_partner_third_comm_v2 partner_third_comm on partner_third_comm.grec_seq_rec||'-'||partner_third_comm.rres_seq_reserva=cabecera.interface_id --

--left join hbgdwc.david_tasas_cambio_flc tip on trunc(cabecera.creation_date)=tip.date and cabecera.booking_currency=tip.currency
left join (select cod_divisa, decimales from hbgdwc.dwc_gen_t_general_currency) mon on cabecera.booking_currency=mon.cod_divisa

);
