


Booking Interface.sql

/* Formatted on 28/03/2017 16:24:37 (QP5 v5.163.1008.3004) */
-- Booking interface

SELECT r.grec_seq_rec || '-' || r.seq_reserva interface_id,
       -- re.semp_cod_emp operative_company,
       -- re.sofi_cod_ofi operative_office,
       -- re.des_receptivo operative_office_desc,
       -- r.grec_seq_rec operative_incoming,
       -- r.seq_reserva booking_id,
       -- r.fint_cod_interface interface,
       -- rf.semp_cod_emp invoicing_company,
       -- rf.sofi_cod_ofi invoicing_office,
       -- rf.seq_rec invoicing_incoming,
       -- TRUNC (r.fec_creacion) Creation_date,
       -- r.fec_creacion Creation_ts,
       (SELECT MIN (RI.FEC_CREACION)
          FROM hbgdwc.dwc_bok_t_booking_information ri
         WHERE     ri.seq_rec = r.grec_seq_rec
               AND ri.seq_reserva = r.seq_reserva
               AND ri.tipo_op = 'A')
          First_booking_ts,
       -- TRUNC (r.fec_modifica) modification_date,
       -- r.fec_modifica modification_ts,
       -- TRUNC (r.fec_cancelacion) cancellation_date,
       -- r.fec_cancelacion cancellation_ts,

       -- DECODE (r.fec_cancelacion, NULL, 'N', 'S') cancelled_booking,

       GREATEST (
          TRUNC (r.fec_creacion),
          NVL (TRUNC (r.fec_modifica), TRUNC (r.fec_creacion)),
          NVL (TRUNC (r.fec_cancelacion),
               NVL (TRUNC (r.fec_modifica), TRUNC (r.fec_creacion))))
          status_date,

       -- r.fec_desde booking_service_from,
       -- r.fec_hasta booking_service_to,
       -- t.seq_ttoo client_code,
       -- t.nom_corto_ttoo costumer_name,
       -- NVL (R.COD_PAIS_CLIENTE, t.gpai_cod_pais_mercado) Source_market,
       -- p.cod_iso source_market_iso,
       -- REPLACE (r.nom_general, ';', '') Holder,
       -- r.nro_ad num_adults,
       -- R.NRO_NI num_childrens,
       -- r.gdep_cod_depart Department_code,
       -- R.RTRE_COD_TIPO_RES Booking_type,
       -- R.IND_FACTURABLE_RES Invoicing_booking,
       -- R.IND_FACTURABLE_ADM Invoicing_admin,
       -- R.PCT_COMISION Client_commision_esp,
       -- r.pct_rappel client_override_esp,
       -- r.ind_confirma confirmed_booking,
       -- DECODE (I.PARTNER_TTOO, NULL, 'N', 'S') Partner_booking,
       -- cod_divisa_p Partner_booking_currency,
       -- seq_ttoo_p Partner_code,
       -- cod_suc_p Partner_brand,
       -- seq_agencia_p Partner_agency_code,
       -- seq_sucursal_p Partner_agency_brand,
       -- seq_rec_expediente Booking_file_incoming,
       -- seq_res_expediente booking_file_number,
       -- DECODE (ind_tippag, NULL, 'Merchant', 'Pago en hotel')
       --    Accomodation_model,

       NVL (
          (SELECT h.izge_cod_destino || '-' || di.nom_destino
             FROM hbgdwc.dwc_mtd_t_hotel h,
                  hbgdwc.dwc_bok_t_hotel_sale hv,
                  hbgdwc.dwc_itn_t_internet_destination_id di
            WHERE     hv.grec_Seq_rec = r.grec_seq_rec
                  AND hv.rres_seq_reserva = r.seq_reserva
                  AND hv.ghor_seq_hotel = h.seq_hotel
                  AND h.izge_cod_destino = di.ides_cod_destino
                  AND 'ENG' = di.sidi_cod_idioma
                  AND ROWNUM = 1),
          (SELECT c.cod_destino || '-' || di.nom_destino
             FROM hbgdwc.dwc_bok_t_other o,
                  hbgdwc.dwc_con_t_contract_other c,
                  hbgdwc.dwc_itn_t_internet_destination_id di
            WHERE     o.seq_rec = r.grec_seq_rec
                  AND o.seq_reserva = r.seq_reserva
                  AND O.SEQ_REC = c.seq_rec
                  AND o.nom_contrato = c.nom_contrato
                  AND o.ind_tipo_otro = c.ind_tipo_otro
                  AND o.fec_desde BETWEEN c.fec_desde AND c.fec_hasta
                  AND c.cod_destino = di.ides_cod_Destino
                  AND 'ENG' = di.sidi_cod_idioma
                  AND ROWNUM = 1))
          Destination_code,
       -- r.gdiv_cod_divisa Booking_currency,
       NVL (
          (SELECT NVL (SUM (Re_Pk_Admon.Re_Fu_Cambio_Divisa (
                               rf.semp_cod_emp,
                               Re_Pk_Admon.Cambio_Res, --R
                               DECODE (I.IND_FEC_CAM_DIV,
                                       'E', r.fec_desde,
                                       r.fec_creacion),
                               R.GDIV_COD_DIVISA,
                               r.gdiv_cod_divisa,
                               Re_Pk_Reser1.Re_Fu_Calcular_Noch_Imp (
                                  ven.fec_desde,
                                  ven.fec_hasta,
                                  ven.nro_unidades,
                                  ven.nro_pax,
                                  ven.ind_tipo_unid,
                                  ven.ind_p_s,
                                  ven.imp_unitario))),
                       0)
             FROM hbgdwc.dwc_bok_t_sale VEN
            WHERE r.grec_seq_rec = ven.grec_seq_rec
                  AND r.seq_reserva = ven.rres_seq_reserva
                  AND ven.ind_tipo_registro NOT IN
                         ('O', 'V', 'D', 'W', 'Y', 'IT')
                  AND ( (r.ind_tippag IS NULL AND (ven.Ind_Facturable = 'S'))
                       OR (r.ind_tippag IS NOT NULL
                           AND ven.ind_tipo_registro <> 'CH'))
                  AND ven.ind_contra_apunte = 'N'),
          0)
          TTV_booking_currency,
       NVL (
          (SELECT NVL (SUM (Re_Pk_Admon.Re_Fu_Cambio_Divisa (
                               rf.semp_cod_emp,
                               Re_Pk_Admon.Cambio_Res,
                               DECODE (I.IND_FEC_CAM_DIV,
                                       'E', r.fec_desde,
                                       r.fec_creacion),
                               R.GDIV_COD_DIVISA,
                               r.gdiv_cod_divisa,
                               DECODE (
                                  ind_tipo_registro,
                                  'BT', 0,
                                  DECODE (
                                     ven.ind_tipo_regimen
                                     || NVL (ven.ind_trfprc, 'N'),
                                     'GN', DECODE (
                                              ven.imp_impuesto,
                                              NULL, Re_Pk_Reser1.Re_Fu_Calcular_Noch_Imp (
                                                       ven.fec_desde,
                                                       ven.fec_hasta,
                                                       ven.nro_unidades,
                                                       ven.nro_pax,
                                                       ven.ind_tipo_unid,
                                                       ven.ind_p_s,
                                                       (ven.imp_unitario
                                                        - (ven.imp_unitario
                                                           / (1
                                                              + (pct_impuesto
                                                                 / 100))))),
                                              ven.imp_impuesto))))),
                       0)
             FROM hbgdwc.dwc_bok_t_sale VEN, hbgdwc.dwc_oth_v_re_v_impuesto_sap IMP
            WHERE     r.grec_seq_rec = ven.grec_seq_rec
                  AND r.seq_reserva = ven.rres_seq_reserva
                  AND ven.ind_tipo_registro NOT IN ('O', 'D', 'IT')
                  AND ( (r.ind_tippag IS NULL AND (ven.Ind_Facturable = 'S'))
                       OR (r.ind_tippag IS NOT NULL
                           AND ven.ind_tipo_registro <> 'CH'))
                  AND ven.ind_contra_apunte = 'N'
                  AND ven.Rvp_Ind_Tipo_Imp = imp.Ind_Tipo_Imp(+)
                  AND ven.Rvp_Cod_Impuesto = imp.Cod_Impuesto(+)
                  AND ven.Rvp_Cod_Esquema = imp.Cod_Esquema(+)
                  AND ven.Rvp_Cod_Clasif = imp.Cod_Clasif(+)
                  AND ven.Rvp_Cod_Empresa = imp.Cod_Emp_Atlas(+)),
          0)
          Tax_TTV,
       NVL (
          (SELECT NVL (SUM (Re_Pk_Admon.Re_Fu_Cambio_Divisa (
                               rf.semp_cod_emp,
                               Re_Pk_Admon.Cambio_Res,
                               DECODE (I.IND_FEC_CAM_DIV,
                                       'E', r.fec_desde,
                                       r.fec_creacion),
                               R.GDIV_COD_DIVISA,
                               r.gdiv_cod_divisa,
                               DECODE (
                                  ind_tipo_registro,
                                  'BT', 0,
                                  DECODE (
                                     ven.ind_tipo_regimen
                                     || NVL (ven.ind_trfprc, 'N'),
                                     'EN', DECODE (
                                              ven.imp_impuesto,
                                              NULL, Re_Pk_Reser1.Re_Fu_Calcular_Noch_Imp (
                                                       ven.fec_desde,
                                                       ven.fec_hasta,
                                                       ven.nro_unidades,
                                                       ven.nro_pax,
                                                       ven.ind_tipo_unid,
                                                       ven.ind_p_s,
                                                       (ven.imp_unitario
                                                        - (ven.imp_unitario
                                                           / (1
                                                              + (pct_impuesto
                                                                 / 100))))),
                                              ven.imp_impuesto))))),
                       0)
             FROM hbgdwc.dwc_bok_t_sale VEN, hbgdwc.dwc_oth_v_re_v_impuesto_sap IMP
            WHERE     r.grec_seq_rec = ven.grec_seq_rec
                  AND r.seq_reserva = ven.rres_seq_reserva
                  AND ven.ind_tipo_registro NOT IN ('O', 'D', 'IT')
                  AND ( (r.ind_tippag IS NULL AND (ven.Ind_Facturable = 'S'))
                       OR (r.ind_tippag IS NOT NULL
                           AND ven.ind_tipo_registro <> 'CH'))
                  AND ven.ind_contra_apunte = 'N'
                  AND ven.Rvp_Ind_Tipo_Imp = imp.Ind_Tipo_Imp(+)
                  AND ven.Rvp_Cod_Impuesto = imp.Cod_Impuesto(+)
                  AND ven.Rvp_Cod_Esquema = imp.Cod_Esquema(+)
                  AND ven.Rvp_Cod_Clasif = imp.Cod_Clasif(+)
                  AND ven.Rvp_Cod_Empresa = imp.Cod_Emp_Atlas(+)),
          0)
          Tax_TTV_TOMS,
       (SELECT Re_Pk_Admon.RE_FU_REDONDEO (NVL (SUM (impuesto_canal), 0),
                                           r.gdiv_cod_divisa)
          FROM (SELECT SUM (
                          NVL (
                             DECODE (
                                ind_tipo_regimen_fac,
                                'E', imp_margen_canal
                                     * (1
                                        - (1 / (1 + (vta.pct_impuesto / 100)))),
                                imp_venta
                                * (1 - (1 / (1 + (vta.pct_impuesto / 100))))
                                - (imp_venta - imp_margen_canal)
                                  * (1 - (1 / (1 + (cpa.pct_impuesto / 100))))),
                             0))
                          impuesto_canal
                  FROM hbgdwc.dwc_bok_t_canco_hotel h,
                       hbgdwc.dwc_oth_v_re_v_impuesto_sap vta,
                       hbgdwc.dwc_oth_v_re_v_impuesto_sap cpa
                 WHERE     seq_rec = r.grec_seq_rec
                       AND seq_reserva = r.seq_Reserva
                       AND ind_tipo_imp_vta_fac = vta.ind_tipo_imp
                       AND cod_impuesto_vta_fac = vta.cod_impuesto
                       AND cod_clasif_vta_fac = vta.cod_clasif
                       AND cod_esquema_vta_fac = vta.cod_esquema
                       AND cod_empresa_vta_fac = vta.cod_emp_atlas
                       AND ind_tipo_imp_vta_fac = cpa.ind_tipo_imp
                       AND cod_impuesto_vta_fac = cpa.cod_impuesto
                       AND cod_clasif_vta_fac = cpa.cod_clasif
                       AND cod_esquema_vta_fac = cpa.cod_esquema
                       AND cod_empresa_vta_fac = cpa.cod_emp_atlas
                UNION
                SELECT SUM (
                          NVL (
                             DECODE (
                                ind_tipo_regimen_fac,
                                'E', imp_margen_canal
                                     * (1
                                        - (1 / (1 + (vta.pct_impuesto / 100)))),
                                imp_venta
                                * (1 - (1 / (1 + (vta.pct_impuesto / 100))))
                                - (imp_venta - imp_margen_canal)
                                  * (1 - (1 / (1 + (cpa.pct_impuesto / 100))))),
                             0))
                          impuesto_canal
                  FROM hbgdwc.dwc_bok_t_canco_hotel_circuit h,
                       hbgdwc.dwc_oth_v_re_v_impuesto_sap vta,
                       hbgdwc.dwc_oth_v_re_v_impuesto_sap cpa
                 WHERE     seq_rec = r.grec_seq_rec
                       AND seq_reserva = r.seq_Reserva
                       AND ind_tipo_imp_vta_fac = vta.ind_tipo_imp
                       AND cod_impuesto_vta_fac = vta.cod_impuesto
                       AND cod_clasif_vta_fac = vta.cod_clasif
                       AND cod_esquema_vta_fac = vta.cod_esquema
                       AND cod_empresa_vta_fac = vta.cod_emp_atlas
                       AND ind_tipo_imp_vta_fac = cpa.ind_tipo_imp
                       AND cod_impuesto_vta_fac = cpa.cod_impuesto
                       AND cod_clasif_vta_fac = cpa.cod_clasif
                       AND cod_esquema_vta_fac = cpa.cod_esquema
                       AND cod_empresa_vta_fac = cpa.cod_emp_atlas
                UNION
                SELECT SUM (
                          NVL (
                             DECODE (
                                ind_tipo_regimen_fac,
                                'E', imp_margen_canal
                                     * (1
                                        - (1 / (1 + (vta.pct_impuesto / 100)))),
                                imp_venta
                                * (1 - (1 / (1 + (vta.pct_impuesto / 100))))
                                - (imp_venta - imp_margen_canal)
                                  * (1 - (1 / (1 + (cpa.pct_impuesto / 100))))),
                             0))
                          impuesto_canal
                  FROM hbgdwc.dwc_bok_t_canco_other h,
                       hbgdwc.dwc_oth_v_re_v_impuesto_sap vta,
                       hbgdwc.dwc_oth_v_re_v_impuesto_sap cpa
                 WHERE     seq_rec = r.grec_seq_rec
                       AND seq_reserva = r.seq_reserva
                       AND ind_tipo_imp_vta_fac = vta.ind_tipo_imp
                       AND cod_impuesto_vta_fac = vta.cod_impuesto
                       AND cod_clasif_vta_fac = vta.cod_clasif
                       AND cod_esquema_vta_fac = vta.cod_esquema
                       AND cod_empresa_vta_fac = vta.cod_emp_atlas
                       AND ind_tipo_imp_vta_fac = cpa.ind_tipo_imp
                       AND cod_impuesto_vta_fac = cpa.cod_impuesto
                       AND cod_clasif_vta_fac = cpa.cod_clasif
                       AND cod_esquema_vta_fac = cpa.cod_esquema
                       AND cod_empresa_vta_fac = cpa.cod_emp_atlas
                UNION
                SELECT SUM (
                          NVL (
                             DECODE (
                                ind_tipo_regimen_fac,
                                'E', imp_margen_canal
                                     * (1
                                        - (1 / (1 + (vta.pct_impuesto / 100)))),
                                imp_venta
                                * (1 - (1 / (1 + (vta.pct_impuesto / 100))))
                                - (imp_venta - imp_margen_canal)
                                  * (1 - (1 / (1 + (cpa.pct_impuesto / 100))))),
                             0))
                          impuesto_canal
                  FROM hbgdwc.dwc_bok_t_canco_transfer h,
                       hbgdwc.dwc_oth_v_re_v_impuesto_sap vta,
                       hbgdwc.dwc_oth_v_re_v_impuesto_sap cpa
                 WHERE     seq_rec = r.grec_seq_rec
                       AND seq_reserva = r.seq_reserva
                       AND ind_tipo_imp_vta_fac = vta.ind_tipo_imp
                       AND cod_impuesto_vta_fac = vta.cod_impuesto
                       AND cod_clasif_vta_fac = vta.cod_clasif
                       AND cod_esquema_vta_fac = vta.cod_esquema
                       AND cod_empresa_vta_fac = vta.cod_emp_atlas
                       AND ind_tipo_imp_vta_fac = cpa.ind_tipo_imp
                       AND cod_impuesto_vta_fac = cpa.cod_impuesto
                       AND cod_clasif_vta_fac = cpa.cod_clasif
                       AND cod_esquema_vta_fac = cpa.cod_esquema
                       AND cod_empresa_vta_fac = cpa.cod_emp_atlas
                UNION
                SELECT SUM (
                          NVL (
                             DECODE (
                                ind_tipo_regimen_fac,
                                'E', imp_margen_canal
                                     * (1
                                        - (1 / (1 + (vta.pct_impuesto / 100)))),
                                imp_venta
                                * (1 - (1 / (1 + (vta.pct_impuesto / 100))))
                                - (imp_venta - imp_margen_canal)
                                  * (1 - (1 / (1 + (cpa.pct_impuesto / 100))))),
                             0))
                          impuesto_canal
                  FROM hbgdwc.dwc_bok_t_canco_endowments h,
                       hbgdwc.dwc_oth_v_re_v_impuesto_sap vta,
                       hbgdwc.dwc_oth_v_re_v_impuesto_sap cpa
                 WHERE     seq_rec = r.grec_seq_rec
                       AND seq_reserva = r.seq_reserva
                       AND ind_tipo_imp_vta_fac = vta.ind_tipo_imp
                       AND cod_impuesto_vta_fac = vta.cod_impuesto
                       AND cod_clasif_vta_fac = vta.cod_clasif
                       AND cod_esquema_vta_fac = vta.cod_esquema
                       AND cod_empresa_vta_fac = vta.cod_emp_atlas
                       AND ind_tipo_imp_vta_fac = cpa.ind_tipo_imp
                       AND cod_impuesto_vta_fac = cpa.cod_impuesto
                       AND cod_clasif_vta_fac = cpa.cod_clasif
                       AND cod_esquema_vta_fac = cpa.cod_esquema
                       AND cod_empresa_vta_fac = cpa.cod_emp_atlas
                UNION
                SELECT SUM (
                          NVL (
                             DECODE (
                                ind_tipo_regimen_fac,
                                'E', imp_margen_canal
                                     * (1
                                        - (1 / (1 + (vta.pct_impuesto / 100)))),
                                imp_venta
                                * (1 - (1 / (1 + (vta.pct_impuesto / 100))))
                                - (imp_venta - imp_margen_canal)
                                  * (1 - (1 / (1 + (cpa.pct_impuesto / 100))))),
                             0))
                          impuesto_canal
                  FROM hbgdwc.dwc_bok_t_canco_extra cce,
                       hbgdwc.dwc_oth_v_re_v_impuesto_sap vta,
                       hbgdwc.dwc_oth_v_re_v_impuesto_sap cpa
                 WHERE     seq_rec = r.grec_seq_rec
                       AND seq_reserva = r.seq_reserva
                       AND ind_tipo_imp_vta_fac = vta.ind_tipo_imp
                       AND cod_impuesto_vta_fac = vta.cod_impuesto
                       AND cod_clasif_vta_fac = vta.cod_clasif
                       AND cod_esquema_vta_fac = vta.cod_esquema
                       AND cod_empresa_vta_fac = vta.cod_emp_atlas
                       AND ind_tipo_imp_vta_fac = cpa.ind_tipo_imp
                       AND cod_impuesto_vta_fac = cpa.cod_impuesto
                       AND cod_clasif_vta_fac = cpa.cod_clasif
                       AND cod_esquema_vta_fac = cpa.cod_esquema
                       AND cod_empresa_vta_fac = cpa.cod_emp_atlas
                       AND (CCE.seq_rec, CCE.seq_reserva, CCE.ord_extra) NOT IN
                              (SELECT EXT.grec_seq_rec,
                                      EXT.rres_seq_reserva,
                                      EXT.ord_extra
                                 FROM hbgdwc.dwc_bok_t_extra EXT,
                                      hbgdwc.dwc_cli_dir_t_cd_discount_bond BON,
                                      hbgdwc.dwc_cli_dir_t_cd_campaign CAM
                                WHERE EXT.num_bono = BON.num_bono
                                      AND EXT.cod_interface =
                                             BON.cod_interface
                                      AND BON.cod_campana = CAM.cod_campana
                                      AND BON.cod_interface =
                                             CAM.cod_interface
                                      AND CAM.ind_rentabilidad = 'N')))
          Tax_Sales_Transfer_pricing,
       (SELECT Re_Pk_Admon.RE_FU_REDONDEO (
                  NVL (
                     SUM (
                        impuesto_canco
                        + (SELECT NVL (SUM (Re_Pk_Admon.Re_Fu_Cambio_Divisa (
                                               rf.semp_cod_emp,
                                               Re_Pk_Admon.Cambio_Res,
                                               r.fec_creacion,
                                               cst.sdiv_cod_divisa,
                                               r.gdiv_Cod_Divisa,
                                               re_pk_reser1.re_fu_calcular_noch_imp (
                                                  cst.fec_desde,
                                                  cst.fec_hasta,
                                                  cst.nro_unidades,
                                                  cst.nro_pax,
                                                  cst.ind_tipo_unid,
                                                  cst.ind_p_s,
                                                  cst.imp_unitario))),
                                       0)
                             FROM hbgdwc.dwc_bok_t_cost cst
                            WHERE     cst.grec_seq_rec = r.grec_seq_rec
                                  AND cst.rres_seq_reserva = r.seq_reserva
                                  AND cst.ind_tipo_registro = 'DR'
                                  AND cst.ind_facturable = 'S'
                                  AND NOT EXISTS
                                             (SELECT 1
                                                FROM hbgdwc.dwc_bok_t_extra EXT,
                                                     hbgdwc.dwc_cli_dir_t_cd_discount_bond BON,
                                                     hbgdwc.dwc_cli_dir_t_cd_campaign CAM
                                               WHERE CST.grec_seq_rec = EXT.grec_seq_rec
                                                     AND CST.rres_seq_reserva = EXT.rres_seq_reserva
                                                     AND CST.rext_ord_extra = EXT.ord_extra
                                                     AND EXT.num_bono = BON.num_bono
                                                     AND EXT.cod_interface = BON.cod_interface
                                                     AND BON.cod_campana = CAM.cod_campana
                                                     AND BON.cod_interface = CAM.cod_interface
                                                     AND CAM.ind_rentabilidad = 'N'))),
                     0),
                  r.gdiv_cod_divisa)
          FROM (SELECT SUM (
                          NVL (
                             DECODE (
                                ind_tipo_regimen_fac,
                                'E', imp_margen_canal
                                     * (1
                                        - (1 / (1 + (vta.pct_impuesto / 100)))),
                                imp_venta
                                * (1 - (1 / (1 + (vta.pct_impuesto / 100))))
                                - (imp_venta - imp_margen_canal)
                                  * (1 - (1 / (1 + (cpa.pct_impuesto / 100))))),
                             0))
                       + SUM (
                            NVL (
                               Re_Pk_Admon.Re_Fu_Cambio_Divisa (
                                  rf.semp_cod_emp,
                                  Re_Pk_Admon.Cambio_Res,
                                  r.fec_creacion,
                                  r.gdiv_Cod_Divisa,
                                  'EUR',
                                  DECODE (
                                     ind_tipo_regimen_con,
                                     'E', imp_margen_canco
                                          * (1
                                             - (1
                                                / (1
                                                   + (vta.pct_impuesto
                                                      / 100)))),
                                     (imp_coste + imp_margen_canco)
                                     * (1
                                        - (1
                                           / (1
                                              + (vta.pct_impuesto / 100))))
                                     - (imp_coste)
                                       * (1
                                          - (1
                                             / (1
                                                + (cpa.pct_impuesto / 100)))))),
                               0))
                          impuesto_canco
                  FROM hbgdwc.dwc_bok_t_canco_hotel h,
                       hbgdwc.dwc_oth_v_re_v_impuesto_sap vta,
                       hbgdwc.dwc_oth_v_re_v_impuesto_sap cpa
                 WHERE     seq_rec = r.grec_seq_rec
                       AND seq_reserva = r.seq_reserva
                       AND ind_tipo_imp_vta_fac = vta.ind_tipo_imp
                       AND cod_impuesto_vta_fac = vta.cod_impuesto
                       AND cod_clasif_vta_fac = vta.cod_clasif
                       AND cod_esquema_vta_fac = vta.cod_esquema
                       AND cod_empresa_vta_fac = vta.cod_emp_atlas
                       AND ind_tipo_imp_vta_fac = cpa.ind_tipo_imp
                       AND cod_impuesto_vta_fac = cpa.cod_impuesto
                       AND cod_clasif_vta_fac = cpa.cod_clasif
                       AND cod_esquema_vta_fac = cpa.cod_esquema
                       AND cod_empresa_vta_fac = cpa.cod_emp_atlas
                UNION
                SELECT SUM (
                          NVL (
                             DECODE (
                                ind_tipo_regimen_fac,
                                'E', imp_margen_canal
                                     * (1
                                        - (1 / (1 + (vta.pct_impuesto / 100)))),
                                imp_venta
                                * (1 - (1 / (1 + (vta.pct_impuesto / 100))))
                                - (imp_venta - imp_margen_canal)
                                  * (1 - (1 / (1 + (cpa.pct_impuesto / 100))))),
                             0))
                       + SUM (
                            NVL (
                               Re_Pk_Admon.Re_Fu_Cambio_Divisa (
                                  rf.semp_cod_emp,
                                  Re_Pk_Admon.Cambio_Res,
                                  r.fec_creacion,
                                  r.gdiv_Cod_Divisa,
                                  'EUR',
                                  DECODE (
                                     ind_tipo_regimen_con,
                                     'E', imp_margen_canco
                                          * (1
                                             - (1
                                                / (1
                                                   + (vta.pct_impuesto
                                                      / 100)))),
                                     (imp_coste + imp_margen_canco)
                                     * (1
                                        - (1
                                           / (1
                                              + (vta.pct_impuesto / 100))))
                                     - (imp_coste)
                                       * (1
                                          - (1
                                             / (1
                                                + (cpa.pct_impuesto / 100)))))),
                               0))
                          impuesto_canco
                  FROM hbgdwc.dwc_bok_t_canco_hotel_circuit h,
                       hbgdwc.dwc_oth_v_re_v_impuesto_sap vta,
                       hbgdwc.dwc_oth_v_re_v_impuesto_sap cpa
                 WHERE     seq_rec = r.grec_seq_rec
                       AND seq_reserva = r.seq_reserva
                       AND ind_tipo_imp_vta_fac = vta.ind_tipo_imp
                       AND cod_impuesto_vta_fac = vta.cod_impuesto
                       AND cod_clasif_vta_fac = vta.cod_clasif
                       AND cod_esquema_vta_fac = vta.cod_esquema
                       AND cod_empresa_vta_fac = vta.cod_emp_atlas
                       AND ind_tipo_imp_vta_fac = cpa.ind_tipo_imp
                       AND cod_impuesto_vta_fac = cpa.cod_impuesto
                       AND cod_clasif_vta_fac = cpa.cod_clasif
                       AND cod_esquema_vta_fac = cpa.cod_esquema
                       AND cod_empresa_vta_fac = cpa.cod_emp_atlas
                UNION
                SELECT SUM (
                          NVL (
                             DECODE (
                                ind_tipo_regimen_fac,
                                'E', imp_margen_canal
                                     * (1
                                        - (1 / (1 + (vta.pct_impuesto / 100)))),
                                imp_venta
                                * (1 - (1 / (1 + (vta.pct_impuesto / 100))))
                                - (imp_venta - imp_margen_canal)
                                  * (1 - (1 / (1 + (cpa.pct_impuesto / 100))))),
                             0))
                       + SUM (
                            NVL (
                               Re_Pk_Admon.Re_Fu_Cambio_Divisa (
                                  rf.semp_cod_emp,
                                  Re_Pk_Admon.Cambio_Res,
                                  r.fec_creacion,
                                  r.gdiv_Cod_Divisa,
                                  'EUR',
                                  DECODE (
                                     ind_tipo_regimen_con,
                                     'E', imp_margen_canco
                                          * (1
                                             - (1
                                                / (1
                                                   + (vta.pct_impuesto
                                                      / 100)))),
                                     (imp_coste + imp_margen_canco)
                                     * (1
                                        - (1
                                           / (1
                                              + (vta.pct_impuesto / 100))))
                                     - (imp_coste)
                                       * (1
                                          - (1
                                             / (1
                                                + (cpa.pct_impuesto / 100)))))),
                               0))
                          impuesto_canco
                  FROM hbgdwc.dwc_bok_t_canco_other h,
                       hbgdwc.dwc_oth_v_re_v_impuesto_sap vta,
                       hbgdwc.dwc_oth_v_re_v_impuesto_sap cpa
                 WHERE     seq_rec = r.grec_seq_rec
                       AND seq_reserva = r.seq_reserva
                       AND ind_tipo_imp_vta_fac = vta.ind_tipo_imp
                       AND cod_impuesto_vta_fac = vta.cod_impuesto
                       AND cod_clasif_vta_fac = vta.cod_clasif
                       AND cod_esquema_vta_fac = vta.cod_esquema
                       AND cod_empresa_vta_fac = vta.cod_emp_atlas
                       AND ind_tipo_imp_vta_fac = cpa.ind_tipo_imp
                       AND cod_impuesto_vta_fac = cpa.cod_impuesto
                       AND cod_clasif_vta_fac = cpa.cod_clasif
                       AND cod_esquema_vta_fac = cpa.cod_esquema
                       AND cod_empresa_vta_fac = cpa.cod_emp_atlas
                UNION
                SELECT SUM (
                          NVL (
                             DECODE (
                                ind_tipo_regimen_fac,
                                'E', imp_margen_canal
                                     * (1
                                        - (1 / (1 + (vta.pct_impuesto / 100)))),
                                imp_venta
                                * (1 - (1 / (1 + (vta.pct_impuesto / 100))))
                                - (imp_venta - imp_margen_canal)
                                  * (1 - (1 / (1 + (cpa.pct_impuesto / 100))))),
                             0))
                       + SUM (
                            NVL (
                               Re_Pk_Admon.Re_Fu_Cambio_Divisa (
                                  rf.semp_cod_emp,
                                  Re_Pk_Admon.Cambio_Res,
                                  r.fec_creacion,
                                  r.gdiv_Cod_Divisa,
                                  'EUR',
                                  DECODE (
                                     ind_tipo_regimen_con,
                                     'E', imp_margen_canco
                                          * (1
                                             - (1
                                                / (1
                                                   + (vta.pct_impuesto
                                                      / 100)))),
                                     (imp_coste + imp_margen_canco)
                                     * (1
                                        - (1
                                           / (1
                                              + (vta.pct_impuesto / 100))))
                                     - (imp_coste)
                                       * (1
                                          - (1
                                             / (1
                                                + (cpa.pct_impuesto / 100)))))),
                               0))
                          impuesto_canco
                  FROM hbgdwc.dwc_bok_t_canco_transfer h,
                       hbgdwc.dwc_oth_v_re_v_impuesto_sap vta,
                       hbgdwc.dwc_oth_v_re_v_impuesto_sap cpa
                 WHERE     seq_rec = r.grec_seq_rec
                       AND seq_reserva = r.seq_reserva
                       AND ind_tipo_imp_vta_fac = vta.ind_tipo_imp
                       AND cod_impuesto_vta_fac = vta.cod_impuesto
                       AND cod_clasif_vta_fac = vta.cod_clasif
                       AND cod_esquema_vta_fac = vta.cod_esquema
                       AND cod_empresa_vta_fac = vta.cod_emp_atlas
                       AND ind_tipo_imp_vta_fac = cpa.ind_tipo_imp
                       AND cod_impuesto_vta_fac = cpa.cod_impuesto
                       AND cod_clasif_vta_fac = cpa.cod_clasif
                       AND cod_esquema_vta_fac = cpa.cod_esquema
                       AND cod_empresa_vta_fac = cpa.cod_emp_atlas
                UNION
                SELECT SUM (
                          NVL (
                             DECODE (
                                ind_tipo_regimen_fac,
                                'E', imp_margen_canal
                                     * (1
                                        - (1 / (1 + (vta.pct_impuesto / 100)))),
                                imp_venta
                                * (1 - (1 / (1 + (vta.pct_impuesto / 100))))
                                - (imp_venta - imp_margen_canal)
                                  * (1 - (1 / (1 + (cpa.pct_impuesto / 100))))),
                             0))
                       + SUM (
                            NVL (
                               Re_Pk_Admon.Re_Fu_Cambio_Divisa (
                                  rf.semp_cod_emp,
                                  Re_Pk_Admon.Cambio_Res,
                                  r.fec_creacion,
                                  r.gdiv_Cod_Divisa,
                                  'EUR',
                                  DECODE (
                                     ind_tipo_regimen_con,
                                     'E', imp_margen_canco
                                          * (1
                                             - (1
                                                / (1
                                                   + (vta.pct_impuesto
                                                      / 100)))),
                                     (imp_coste + imp_margen_canco)
                                     * (1
                                        - (1
                                           / (1
                                              + (vta.pct_impuesto / 100))))
                                     - (imp_coste)
                                       * (1
                                          - (1
                                             / (1
                                                + (cpa.pct_impuesto / 100)))))),
                               0))
                          impuesto_canco
                  FROM hbgdwc.dwc_bok_t_canco_endowments h,
                       hbgdwc.dwc_oth_v_re_v_impuesto_sap vta,
                       hbgdwc.dwc_oth_v_re_v_impuesto_sap cpa
                 WHERE     seq_rec = r.grec_seq_rec
                       AND seq_reserva = r.seq_reserva
                       AND ind_tipo_imp_vta_fac = vta.ind_tipo_imp
                       AND cod_impuesto_vta_fac = vta.cod_impuesto
                       AND cod_clasif_vta_fac = vta.cod_clasif
                       AND cod_esquema_vta_fac = vta.cod_esquema
                       AND cod_empresa_vta_fac = vta.cod_emp_atlas
                       AND ind_tipo_imp_vta_fac = cpa.ind_tipo_imp
                       AND cod_impuesto_vta_fac = cpa.cod_impuesto
                       AND cod_clasif_vta_fac = cpa.cod_clasif
                       AND cod_esquema_vta_fac = cpa.cod_esquema
                       AND cod_empresa_vta_fac = cpa.cod_emp_atlas
                UNION
                SELECT SUM (
                          NVL (
                             DECODE (
                                ind_tipo_regimen_fac,
                                'E', imp_margen_canal
                                     * (1
                                        - (1 / (1 + (vta.pct_impuesto / 100)))),
                                imp_venta
                                * (1 - (1 / (1 + (vta.pct_impuesto / 100))))
                                - (imp_venta - imp_margen_canal)
                                  * (1 - (1 / (1 + (cpa.pct_impuesto / 100))))),
                             0))
                       + SUM (
                            NVL (
                               Re_Pk_Admon.Re_Fu_Cambio_Divisa (
                                  rf.semp_cod_emp,
                                  Re_Pk_Admon.Cambio_Res,
                                  r.fec_creacion,
                                  r.gdiv_Cod_Divisa,
                                  'EUR',
                                  DECODE (
                                     ind_tipo_regimen_con,
                                     'E', imp_margen_canco
                                          * (1
                                             - (1
                                                / (1
                                                   + (vta.pct_impuesto
                                                      / 100)))),
                                     (imp_coste + imp_margen_canco)
                                     * (1
                                        - (1
                                           / (1
                                              + (vta.pct_impuesto / 100))))
                                     - (imp_coste)
                                       * (1
                                          - (1
                                             / (1
                                                + (cpa.pct_impuesto / 100)))))),
                               0))
                          impuesto_canco
                  FROM hbgdwc.dwc_bok_t_canco_extra cce,
                       hbgdwc.dwc_oth_v_re_v_impuesto_sap vta,
                       hbgdwc.dwc_oth_v_re_v_impuesto_sap cpa
                 WHERE     seq_rec = r.grec_seq_rec
                       AND seq_reserva = r.seq_reserva
                       AND ind_tipo_imp_vta_fac = vta.ind_tipo_imp
                       AND cod_impuesto_vta_fac = vta.cod_impuesto
                       AND cod_clasif_vta_fac = vta.cod_clasif
                       AND cod_esquema_vta_fac = vta.cod_esquema
                       AND cod_empresa_vta_fac = vta.cod_emp_atlas
                       AND ind_tipo_imp_vta_fac = cpa.ind_tipo_imp
                       AND cod_impuesto_vta_fac = cpa.cod_impuesto
                       AND cod_clasif_vta_fac = cpa.cod_clasif
                       AND cod_esquema_vta_fac = cpa.cod_esquema
                       AND cod_empresa_vta_fac = cpa.cod_emp_atlas
                       AND (CCE.seq_rec, CCE.seq_reserva, CCE.ord_extra) NOT IN
                              (SELECT EXT.grec_seq_rec,
                                      EXT.rres_seq_reserva,
                                      EXT.ord_extra
                                 FROM hbgdwc.dwc_bok_t_extra EXT,
                                      hbgdwc.dwc_cli_dir_t_cd_discount_bond BON,
                                      hbgdwc.dwc_cli_dir_t_cd_campaign CAM
                                WHERE EXT.num_bono = BON.num_bono
                                      AND EXT.cod_interface =
                                             BON.cod_interface
                                      AND BON.cod_campana = CAM.cod_campana
                                      AND BON.cod_interface =
                                             CAM.cod_interface
                                      AND CAM.ind_rentabilidad = 'N')
                                                                     ))
          Transfer_pricing,
        (SELECT NVL (SUM (Re_Pk_Admon.Re_Fu_Cambio_Divisa (
                             rf.semp_cod_emp,
                             Re_Pk_Admon.Cambio_Res,
                             DECODE (I.IND_FEC_CAM_DIV,
                                     'E', r.fec_desde,
                                     r.fec_creacion),
                             GDiv_Cod_Divisa,
                             r.gdiv_cod_divisa,
                             Re_Pk_Reser1.Re_Fu_Calcular_Noch_Imp (
                                V.Fec_Desde,
                                V.Fec_Hasta,
                                NRO_UNIDADES,
                                NRO_PAX,
                                IND_TIPO_UNID,
                                IND_P_S,
                                IMP_UNITARIO))),
                     0)
           FROM hbgdwc.dwc_bok_t_sale V
          WHERE     V.GRec_Seq_Rec = r.grec_seq_rec
                AND V.RRes_Seq_Reserva = r.seq_reserva
                AND V.Ind_Tipo_Registro IN ('O', 'V')
                AND V.Ind_Facturable = 'S'
                AND v.ind_contra_apunte = 'N')
           Client_commision,
        (SELECT NVL (SUM (DECODE (V.IND_TIPO_REGISTRO,
                                  'V', 0,
                                  Re_Pk_Admon.Re_Fu_Cambio_Divisa (
                                     rf.semp_cod_emp,
                                     Re_Pk_Admon.Cambio_Res,
                                     DECODE (I.IND_FEC_CAM_DIV,
                                             'E', r.fec_desde,
                                             r.fec_creacion),
                                     GDiv_Cod_Divisa,
                                     r.gdiv_cod_divisa,
                                     Re_Pk_Reser1.Re_Fu_Calcular_Noch_Imp (
                                        V.Fec_Desde,
                                        V.Fec_Hasta,
                                        NRO_UNIDADES,
                                        NRO_PAX,
                                        IND_TIPO_UNID,
                                        IND_P_S,
                                        Re_Pk_Admon.RE_FU_REDONDEO (
                                           IMP_UNITARIO
                                           - (IMP_UNITARIO
                                              / (1 + (pct_impuesto / 100))),
                                           r.gdiv_cod_divisa))))),
                     0)
           FROM hbgdwc.dwc_bok_t_sale V, hbgdwc.dwc_oth_v_re_v_impuesto_sap
          WHERE     V.GRec_Seq_Rec = r.grec_seq_rec
                AND V.RRes_Seq_Reserva = r.seq_reserva
                AND V.Ind_Tipo_Registro IN ('O', 'V')
                AND V.Ind_Facturable = 'S'
                AND v.ind_contra_apunte = 'N'
                AND Rvp_Ind_Tipo_Imp = Ind_Tipo_Imp(+)
                AND Rvp_Cod_Impuesto = Cod_Impuesto(+)
                AND Rvp_Cod_Esquema = Cod_Esquema(+)
                AND Rvp_Cod_Clasif = Cod_Clasif(+)
                AND Rvp_Cod_Empresa = Cod_Emp_Atlas(+))
           Tax_Client_commision,
        (SELECT NVL (SUM (Re_Pk_Admon.Re_Fu_Cambio_Divisa (
                             rf.semp_cod_emp,
                             Re_Pk_Admon.Cambio_Res,
                             DECODE (I.IND_FEC_CAM_DIV,
                                     'E', r.fec_desde,
                                     r.fec_creacion),
                             GDiv_Cod_Divisa,
                             r.gdiv_cod_divisa,
                             Re_Pk_Reser1.Re_Fu_Calcular_Noch_Imp (
                                V.Fec_Desde,
                                V.Fec_Hasta,
                                NRO_UNIDADES,
                                NRO_PAX,
                                IND_TIPO_UNID,
                                IND_P_S,
                                IMP_UNITARIO))),
                     0)
           FROM hbgdwc.dwc_bok_t_sale V
          WHERE     V.GRec_Seq_Rec = r.grec_seq_rec
                AND V.RRes_Seq_Reserva = r.seq_reserva
                AND V.Ind_Tipo_Registro IN ('D')
                AND V.Ind_Facturable = 'S'
                AND v.ind_contra_apunte = 'N')
           Client_rappel,
        (SELECT NVL (SUM (DECODE (V.IND_TIPO_REGISTRO,
                                  'V', 0,
                                  Re_Pk_Admon.Re_Fu_Cambio_Divisa (
                                     rf.semp_cod_emp,
                                     Re_Pk_Admon.Cambio_Res,
                                     DECODE (I.IND_FEC_CAM_DIV,
                                             'E', r.fec_desde,
                                             r.fec_creacion),
                                     GDiv_Cod_Divisa,
                                     r.gdiv_cod_divisa,
                                     Re_Pk_Reser1.Re_Fu_Calcular_Noch_Imp (
                                        V.Fec_Desde,
                                        V.Fec_Hasta,
                                        NRO_UNIDADES,
                                        NRO_PAX,
                                        IND_TIPO_UNID,
                                        IND_P_S,
                                        Re_Pk_Admon.RE_FU_REDONDEO (
                                           IMP_UNITARIO
                                           - (IMP_UNITARIO
                                              / (1 + (pct_impuesto / 100))),
                                           r.gdiv_cod_divisa))))),
                     0)
           FROM hbgdwc.dwc_bok_t_sale V, hbgdwc.dwc_oth_v_re_v_impuesto_sap
          WHERE     V.GRec_Seq_Rec = r.grec_seq_rec
                AND V.RRes_Seq_Reserva = r.seq_reserva
                AND V.Ind_Tipo_Registro IN ('D')
                AND V.Ind_Facturable = 'S'
                AND v.ind_contra_apunte = 'N'
                AND Rvp_Ind_Tipo_Imp = Ind_Tipo_Imp(+)
                AND Rvp_Cod_Impuesto = Cod_Impuesto(+)
                AND Rvp_Cod_Esquema = Cod_Esquema(+)
                AND Rvp_Cod_Clasif = Cod_Clasif(+)
                AND Rvp_Cod_Empresa = Cod_Emp_Atlas(+))
           Tax_Client_rappel,
        NVL (
           (SELECT NVL (SUM (DECODE (cst.ind_tipo_registro,
                                     'SC', 0,
                                     Re_Pk_Admon.Re_Fu_Cambio_Divisa (
                                        rf.semp_cod_emp,
                                        Re_Pk_Admon.Cambio_Res,
                                        DECODE (I.IND_FEC_CAM_DIV,
                                                'E', r.fec_desde,
                                                r.fec_creacion),
                                        cst.SDiv_Cod_Divisa,
                                        r.gdiv_cod_divisa,
                                        Re_Pk_Reser1.Re_Fu_Calcular_Noch_Imp (
                                           cst.Fec_Desde,
                                           cst.Fec_Hasta,
                                           cst.NRO_UNIDADES,
                                           cst.NRO_PAX,
                                           cst.IND_TIPO_UNID,
                                           cst.IND_P_S,
                                           cst.IMP_UNITARIO)))),
                        0)
              FROM hbgdwc.dwc_bok_t_cost CST
             WHERE cst.GRec_Seq_Rec = r.grec_seq_rec
                   AND cst.RRes_Seq_Reserva = r.seq_reserva
                   AND cst.Ind_Tipo_Registro NOT IN
                          ('R', 'DR', 'OV', 'OC', 'CA', 'IT', 'PF', 'LL')
                   AND (cst.Ind_Facturable = 'S' OR r.ind_tippag IS NOT NULL)
                   AND cst.ind_contra_apunte = 'N'),
           0)
           Cost_booking_currency,
        NVL (
           (SELECT NVL (
                      SUM (
                         DECODE (
                            cst.ind_tipo_registro,
                            'LL', 0,
                            DECODE (
                               cst.Ind_Tipo_Regimen
                               || NVL (cst.ind_trfprc, 'N'),
                               'GN',                                --TDAF-1347
                                    Re_Pk_Admon.Re_Fu_Cambio_Divisa (
                                        rf.semp_cod_emp,
                                        Re_Pk_Admon.Cambio_Res,
                                        DECODE (I.IND_FEC_CAM_DIV,
                                                'E', r.fec_desde,
                                                r.fec_creacion),
                                        cst.SDiv_Cod_Divisa,
                                        r.gdiv_cod_divisa,
                                        Re_Pk_Reser1.Re_Fu_Calcular_Noch_Imp (
                                           cst.Fec_Desde,
                                           cst.Fec_Hasta,
                                           cst.NRO_UNIDADES,
                                           cst.NRO_PAX,
                                           cst.IND_TIPO_UNID,
                                           cst.IND_P_S,
                                           cst.IMP_UNITARIO
                                           - (cst.IMP_UNITARIO
                                              / (1 + (pct_impuesto / 100)))))))),
                      0)
              FROM hbgdwc.dwc_bok_t_cost CST, hbgdwc.dwc_oth_v_re_v_impuesto_sap IMP
             WHERE   cst.GRec_Seq_Rec = r.grec_seq_rec
                   AND cst.RRes_Seq_Reserva = r.seq_reserva
                   AND cst.Ind_Tipo_Registro NOT IN
                          ('R', 'DR', 'OV', 'OC', 'CA', 'IT', 'PF', 'LL')
                   AND (cst.Ind_Facturable = 'S' OR r.ind_tippag IS NOT NULL)
                   AND cst.ind_contra_apunte = 'N'
                   AND cst.Rvp_Ind_Tipo_Imp = imp.Ind_Tipo_Imp(+)
                   AND cst.Rvp_Cod_Impuesto = imp.Cod_Impuesto(+)
                   AND cst.Rvp_Cod_Esquema = imp.Cod_Esquema(+)
                   AND cst.Rvp_Cod_Clasif = imp.Cod_Clasif(+)
                   AND cst.Rvp_Cod_Empresa = imp.Cod_Emp_Atlas(+)),
           0)
           Tax_Cost,
        NVL (
           (SELECT NVL (
                      SUM (
                         DECODE (
                            cst.ind_tipo_registro,
                            'LL', 0,
                            DECODE (
                               cst.Ind_Tipo_Regimen
                               || NVL (cst.ind_trfprc, 'N'),
                               'EN',                                --TDAF-1347
                                    Re_Pk_Admon.Re_Fu_Cambio_Divisa (
                                        rf.semp_cod_emp,
                                        Re_Pk_Admon.Cambio_Res,
                                        DECODE (I.IND_FEC_CAM_DIV,
                                                'E', r.fec_desde,
                                                r.fec_creacion),
                                        cst.SDiv_Cod_Divisa,
                                        r.gdiv_cod_divisa,
                                        Re_Pk_Reser1.Re_Fu_Calcular_Noch_Imp (
                                           cst.Fec_Desde,
                                           cst.Fec_Hasta,
                                           cst.NRO_UNIDADES,
                                           cst.NRO_PAX,
                                           cst.IND_TIPO_UNID,
                                           cst.IND_P_S,
                                           cst.IMP_UNITARIO
                                           - (cst.IMP_UNITARIO
                                              / (1 + (pct_impuesto / 100)))))))),
                      0)
              FROM hbgdwc.dwc_bok_t_cost CST, hbgdwc.dwc_oth_v_re_v_impuesto_sap IMP
             WHERE   cst.GRec_Seq_Rec = r.grec_seq_rec
                   AND cst.RRes_Seq_Reserva = r.seq_reserva
                   AND cst.Ind_Tipo_Registro NOT IN
                          ('R', 'DR', 'OV', 'OC', 'CA', 'IT', 'PF', 'LL')
                   AND (cst.Ind_Facturable = 'S' OR r.ind_tippag IS NOT NULL)
                   AND cst.ind_contra_apunte = 'N'
                   AND cst.Rvp_Ind_Tipo_Imp = imp.Ind_Tipo_Imp(+)
                   AND cst.Rvp_Cod_Impuesto = imp.Cod_Impuesto(+)
                   AND cst.Rvp_Cod_Esquema = imp.Cod_Esquema(+)
                   AND cst.Rvp_Cod_Clasif = imp.Cod_Clasif(+)
                   AND cst.Rvp_Cod_Empresa = imp.Cod_Emp_Atlas(+)),
           0)
           Tax_Cost_TOMS,
        (SELECT Re_Pk_Admon.RE_FU_REDONDEO (NVL (SUM (impuesto_canco  + (SELECT NVL (SUM (Re_Pk_Admon.Re_Fu_Cambio_Divisa (
                                               rf.semp_cod_emp,
                                               Re_Pk_Admon.Cambio_Res,
                                               r.fec_creacion,
                                               cst.sdiv_cod_divisa,
                                               r.gdiv_Cod_Divisa,
                                               re_pk_reser1.re_fu_calcular_noch_imp (
                                                  cst.fec_desde,
                                                  cst.fec_hasta,
                                                  cst.nro_unidades,
                                                  cst.nro_pax,
                                                  cst.ind_tipo_unid,
                                                  cst.ind_p_s,
                                                  cst.imp_unitario))),
                                       0)
                             FROM hbgdwc.dwc_bok_t_cost cst
                            WHERE     cst.grec_seq_rec = r.grec_seq_rec
                                  AND cst.rres_seq_reserva = r.seq_reserva
                                  AND cst.ind_tipo_registro = 'DR'
                                  AND cst.ind_facturable = 'S'
                                  AND NOT EXISTS
                                             (SELECT 1
                                                FROM hbgdwc.dwc_bok_t_extra EXT,
                                                     hbgdwc.dwc_cli_dir_t_cd_discount_bond BON,
                                                     hbgdwc.dwc_cli_dir_t_cd_campaign CAM
                                               WHERE CST.grec_seq_rec = EXT.grec_seq_rec
                                                     AND CST.rres_seq_reserva = EXT.rres_seq_reserva
                                                     AND CST.rext_ord_extra = EXT.ord_extra
                                                     AND EXT.num_bono = BON.num_bono
                                                     AND EXT.cod_interface = BON.cod_interface
                                                     AND BON.cod_campana = CAM.cod_campana
                                                     AND BON.cod_interface = CAM.cod_interface
                                                     AND CAM.ind_rentabilidad = 'N'))), 0),
                                            r.gdiv_cod_divisa)
           FROM (SELECT SUM (
                           NVL (
                              DECODE (
                                 ind_tipo_regimen_con,
                                 'E', imp_margen_canco * (1- (1 / (1 + (vta.pct_impuesto / 100)))),
                                 (imp_coste + imp_margen_canco) * (1 - (1 / (1 + (vta.pct_impuesto / 100)))) - (imp_coste)* (1 - (1 / (1 + (cpa.pct_impuesto / 100))))),
                              0))
                           impuesto_canco
                   FROM hbgdwc.dwc_bok_t_canco_hotel h,
                        hbgdwc.dwc_oth_v_re_v_impuesto_sap vta,
                        hbgdwc.dwc_oth_v_re_v_impuesto_sap cpa
                  WHERE     seq_rec = r.grec_seq_rec
                        AND seq_reserva = r.seq_reserva
                        AND ind_tipo_imp_vta_fac = vta.ind_tipo_imp
                        AND cod_impuesto_vta_fac = vta.cod_impuesto
                        AND cod_clasif_vta_fac = vta.cod_clasif
                        AND cod_esquema_vta_fac = vta.cod_esquema
                        AND cod_empresa_vta_fac = vta.cod_emp_atlas
                        AND ind_tipo_imp_vta_fac = cpa.ind_tipo_imp
                        AND cod_impuesto_vta_fac = cpa.cod_impuesto
                        AND cod_clasif_vta_fac = cpa.cod_clasif
                        AND cod_esquema_vta_fac = cpa.cod_esquema
                        AND cod_empresa_vta_fac = cpa.cod_emp_atlas
                 UNION
                 SELECT SUM (
                           NVL (
                              DECODE (
                                 ind_tipo_regimen_con,
                                 'E', imp_margen_canco
                                      * (1
                                         - (1 / (1 + (vta.pct_impuesto / 100)))),
                                 (imp_coste + imp_margen_canco)
                                 * (1 - (1 / (1 + (vta.pct_impuesto / 100))))
                                 - (imp_coste)
                                   * (1 - (1 / (1 + (cpa.pct_impuesto / 100))))),
                              0))
                           impuesto_canco
                   FROM hbgdwc.dwc_bok_t_canco_hotel_circuit h,
                        hbgdwc.dwc_oth_v_re_v_impuesto_sap vta,
                        hbgdwc.dwc_oth_v_re_v_impuesto_sap cpa
                  WHERE     seq_rec = r.grec_seq_rec
                        AND seq_reserva = r.seq_reserva
                        AND ind_tipo_imp_vta_fac = vta.ind_tipo_imp
                        AND cod_impuesto_vta_fac = vta.cod_impuesto
                        AND cod_clasif_vta_fac = vta.cod_clasif
                        AND cod_esquema_vta_fac = vta.cod_esquema
                        AND cod_empresa_vta_fac = vta.cod_emp_atlas
                        AND ind_tipo_imp_vta_fac = cpa.ind_tipo_imp
                        AND cod_impuesto_vta_fac = cpa.cod_impuesto
                        AND cod_clasif_vta_fac = cpa.cod_clasif
                        AND cod_esquema_vta_fac = cpa.cod_esquema
                        AND cod_empresa_vta_fac = cpa.cod_emp_atlas
                 UNION
                 SELECT SUM (
                           NVL (
                              DECODE (
                                 ind_tipo_regimen_con,
                                 'E', imp_margen_canco
                                      * (1
                                         - (1 / (1 + (vta.pct_impuesto / 100)))),
                                 (imp_coste + imp_margen_canco)
                                 * (1 - (1 / (1 + (vta.pct_impuesto / 100))))
                                 - (imp_coste)
                                   * (1 - (1 / (1 + (cpa.pct_impuesto / 100))))),
                              0))
                           impuesto_canco
                   FROM hbgdwc.dwc_bok_t_canco_other h,
                        hbgdwc.dwc_oth_v_re_v_impuesto_sap vta,
                        hbgdwc.dwc_oth_v_re_v_impuesto_sap cpa
                  WHERE     seq_rec = r.grec_seq_rec
                        AND seq_reserva = r.seq_reserva
                        AND ind_tipo_imp_vta_fac = vta.ind_tipo_imp
                        AND cod_impuesto_vta_fac = vta.cod_impuesto
                        AND cod_clasif_vta_fac = vta.cod_clasif
                        AND cod_esquema_vta_fac = vta.cod_esquema
                        AND cod_empresa_vta_fac = vta.cod_emp_atlas
                        AND ind_tipo_imp_vta_fac = cpa.ind_tipo_imp
                        AND cod_impuesto_vta_fac = cpa.cod_impuesto
                        AND cod_clasif_vta_fac = cpa.cod_clasif
                        AND cod_esquema_vta_fac = cpa.cod_esquema
                        AND cod_empresa_vta_fac = cpa.cod_emp_atlas
                 UNION
                 SELECT SUM (
                           NVL (
                              DECODE (
                                 ind_tipo_regimen_con,
                                 'E', imp_margen_canco
                                      * (1
                                         - (1 / (1 + (vta.pct_impuesto / 100)))),
                                 (imp_coste + imp_margen_canco)
                                 * (1 - (1 / (1 + (vta.pct_impuesto / 100))))
                                 - (imp_coste)
                                   * (1 - (1 / (1 + (cpa.pct_impuesto / 100))))),
                              0))
                           impuesto_canco
                   FROM hbgdwc.dwc_bok_t_canco_transfer h,
                        hbgdwc.dwc_oth_v_re_v_impuesto_sap vta,
                        hbgdwc.dwc_oth_v_re_v_impuesto_sap cpa
                  WHERE     seq_rec = r.grec_seq_rec
                        AND seq_reserva = r.seq_reserva
                        AND ind_tipo_imp_vta_fac = vta.ind_tipo_imp
                        AND cod_impuesto_vta_fac = vta.cod_impuesto
                        AND cod_clasif_vta_fac = vta.cod_clasif
                        AND cod_esquema_vta_fac = vta.cod_esquema
                        AND cod_empresa_vta_fac = vta.cod_emp_atlas
                        AND ind_tipo_imp_vta_fac = cpa.ind_tipo_imp
                        AND cod_impuesto_vta_fac = cpa.cod_impuesto
                        AND cod_clasif_vta_fac = cpa.cod_clasif
                        AND cod_esquema_vta_fac = cpa.cod_esquema
                        AND cod_empresa_vta_fac = cpa.cod_emp_atlas
                 UNION
                 SELECT SUM (
                           NVL (
                              DECODE (
                                 ind_tipo_regimen_con,
                                 'E', imp_margen_canco
                                      * (1
                                         - (1 / (1 + (vta.pct_impuesto / 100)))),
                                 (imp_coste + imp_margen_canco)
                                 * (1 - (1 / (1 + (vta.pct_impuesto / 100))))
                                 - (imp_coste)
                                   * (1 - (1 / (1 + (cpa.pct_impuesto / 100))))),
                              0))
                           impuesto_canco
                   FROM hbgdwc.dwc_bok_t_canco_endowments h,
                        hbgdwc.dwc_oth_v_re_v_impuesto_sap vta,
                        hbgdwc.dwc_oth_v_re_v_impuesto_sap cpa
                  WHERE     seq_rec = r.grec_seq_rec
                        AND seq_reserva = r.seq_reserva
                        AND ind_tipo_imp_vta_fac = vta.ind_tipo_imp
                        AND cod_impuesto_vta_fac = vta.cod_impuesto
                        AND cod_clasif_vta_fac = vta.cod_clasif
                        AND cod_esquema_vta_fac = vta.cod_esquema
                        AND cod_empresa_vta_fac = vta.cod_emp_atlas
                        AND ind_tipo_imp_vta_fac = cpa.ind_tipo_imp
                        AND cod_impuesto_vta_fac = cpa.cod_impuesto
                        AND cod_clasif_vta_fac = cpa.cod_clasif
                        AND cod_esquema_vta_fac = cpa.cod_esquema
                        AND cod_empresa_vta_fac = cpa.cod_emp_atlas
                 UNION
                 SELECT SUM (
                           NVL (
                              DECODE (
                                 ind_tipo_regimen_con,
                                 'E', imp_margen_canco
                                      * (1
                                         - (1 / (1 + (vta.pct_impuesto / 100)))),
                                 (imp_coste + imp_margen_canco)
                                 * (1 - (1 / (1 + (vta.pct_impuesto / 100))))
                                 - (imp_coste)
                                   * (1 - (1 / (1 + (cpa.pct_impuesto / 100))))),
                              0))
                           impuesto_canco
                   FROM hbgdwc.dwc_bok_t_canco_extra cce,
                        hbgdwc.dwc_oth_v_re_v_impuesto_sap vta,
                        hbgdwc.dwc_oth_v_re_v_impuesto_sap cpa
                  WHERE     seq_rec = r.grec_seq_rec
                        AND seq_reserva = r.seq_reserva
                        AND ind_tipo_imp_vta_fac = vta.ind_tipo_imp
                        AND cod_impuesto_vta_fac = vta.cod_impuesto
                        AND cod_clasif_vta_fac = vta.cod_clasif
                        AND cod_esquema_vta_fac = vta.cod_esquema
                        AND cod_empresa_vta_fac = vta.cod_emp_atlas
                        AND ind_tipo_imp_vta_fac = cpa.ind_tipo_imp
                        AND cod_impuesto_vta_fac = cpa.cod_impuesto
                        AND cod_clasif_vta_fac = cpa.cod_clasif
                        AND cod_esquema_vta_fac = cpa.cod_esquema
                        AND cod_empresa_vta_fac = cpa.cod_emp_atlas
                        AND (CCE.seq_rec, CCE.seq_reserva, CCE.ord_extra) NOT IN
                               (SELECT EXT.grec_seq_rec,
                                       EXT.rres_seq_reserva,
                                       EXT.ord_extra
                                  FROM hbgdwc.dwc_bok_t_extra EXT,
                                       hbgdwc.dwc_cli_dir_t_cd_discount_bond BON,
                                       hbgdwc.dwc_cli_dir_t_cd_campaign CAM
                                 WHERE EXT.num_bono = BON.num_bono
                                       AND EXT.cod_interface =
                                              BON.cod_interface
                                       AND BON.cod_campana = CAM.cod_campana
                                       AND BON.cod_interface =
                                              CAM.cod_interface
                                       AND CAM.ind_rentabilidad = 'N')))
           Tax_Cost_Transfer_pricing,
        NVL (
           (SELECT NVL (SUM (Re_Pk_Admon.Re_Fu_Cambio_Divisa (
                                rf.semp_cod_emp,
                                Re_Pk_Admon.Cambio_Res,
                                r.fec_creacion, --DECODE (I.IND_FEC_CAM_DIV,                                       'E', r.fec_desde,                                       r.fec_creacion),
                                R.GDIV_COD_DIVISA,
                                'EUR',
                                Re_Pk_Reser1.Re_Fu_Calcular_Noch_Imp (
                                   ven.fec_desde,
                                   ven.fec_hasta,
                                   ven.nro_unidades,
                                   ven.nro_pax,
                                   ven.ind_tipo_unid,
                                   ven.ind_p_s,
                                   ven.imp_unitario))),
                        0)
              FROM hbgdwc.dwc_bok_t_sale VEN
             WHERE r.grec_seq_rec = ven.grec_seq_rec
                   AND r.seq_reserva = ven.rres_seq_reserva
                   AND ven.ind_tipo_registro NOT IN
                          ('O', 'V', 'D', 'W', 'Y', 'IT')
                   AND ( (r.ind_tippag IS NULL AND (ven.Ind_Facturable = 'S'))
                        OR (r.ind_tippag IS NOT NULL
                            AND ven.ind_tipo_registro <> 'CH'))
                   AND ven.ind_contra_apunte = 'N'),
           0)
           TTV_EUR_currency,
        NVL (
           (SELECT NVL (SUM (Re_Pk_Admon.Re_Fu_Cambio_Divisa (
                                rf.semp_cod_emp,
                                Re_Pk_Admon.Cambio_Res,
                                r.fec_creacion, --DECODE (I.IND_FEC_CAM_DIV,                                       'E', r.fec_desde,                                       r.fec_creacion),
                                r.gdiv_cod_divisa,
                                'EUR',
                                DECODE (
                                   ind_tipo_registro,
                                   'BT', 0,
                                   DECODE (
                                      ven.ind_tipo_regimen
                                      || NVL (ven.ind_trfprc, 'N'),
                                      'GN', DECODE (
                                               ven.imp_impuesto,
                                               NULL, Re_Pk_Reser1.Re_Fu_Calcular_Noch_Imp (
                                                        ven.fec_desde,
                                                        ven.fec_hasta,
                                                        ven.nro_unidades,
                                                        ven.nro_pax,
                                                        ven.ind_tipo_unid,
                                                        ven.ind_p_s,
                                                        (ven.imp_unitario
                                                         - (ven.imp_unitario
                                                            / (1
                                                               + (pct_impuesto
                                                                  / 100))))),
                                               ven.imp_impuesto))))),
                        0)
              FROM hbgdwc.dwc_bok_t_sale VEN, hbgdwc.dwc_oth_v_re_v_impuesto_sap IMP
             WHERE     r.grec_seq_rec = ven.grec_seq_rec
                   AND r.seq_reserva = ven.rres_seq_reserva
                   AND ven.ind_tipo_registro NOT IN ('O', 'D', 'IT')
                   AND ( (r.ind_tippag IS NULL AND (ven.Ind_Facturable = 'S'))
                        OR (r.ind_tippag IS NOT NULL
                            AND ven.ind_tipo_registro <> 'CH'))
                   AND ven.ind_contra_apunte = 'N'
                   AND ven.Rvp_Ind_Tipo_Imp = imp.Ind_Tipo_Imp(+)
                   AND ven.Rvp_Cod_Impuesto = imp.Cod_Impuesto(+)
                   AND ven.Rvp_Cod_Esquema = imp.Cod_Esquema(+)
                   AND ven.Rvp_Cod_Clasif = imp.Cod_Clasif(+)
                   AND ven.Rvp_Cod_Empresa = imp.Cod_Emp_Atlas(+)),
           0)
           Tax_TTV_EUR,
        NVL (
           (SELECT NVL (SUM (Re_Pk_Admon.Re_Fu_Cambio_Divisa (
                                rf.semp_cod_emp,
                                Re_Pk_Admon.Cambio_Res,
                                r.fec_creacion, --DECODE (I.IND_FEC_CAM_DIV,                                       'E', r.fec_desde,                                       r.fec_creacion),
                                r.gdiv_cod_divisa,
                                'EUR',
                                DECODE (
                                   ind_tipo_registro,
                                   'BT', 0,
                                   DECODE (
                                      ven.ind_tipo_regimen
                                      || NVL (ven.ind_trfprc, 'N'),
                                      'EN', DECODE (
                                               ven.imp_impuesto,
                                               NULL, Re_Pk_Reser1.Re_Fu_Calcular_Noch_Imp (
                                                        ven.fec_desde,
                                                        ven.fec_hasta,
                                                        ven.nro_unidades,
                                                        ven.nro_pax,
                                                        ven.ind_tipo_unid,
                                                        ven.ind_p_s,
                                                        (ven.imp_unitario
                                                         - (ven.imp_unitario
                                                            / (1
                                                               + (pct_impuesto
                                                                  / 100))))),
                                               ven.imp_impuesto))))),
                        0)
              FROM hbgdwc.dwc_bok_t_sale VEN, hbgdwc.dwc_oth_v_re_v_impuesto_sap IMP
             WHERE     r.grec_seq_rec = ven.grec_seq_rec
                   AND r.seq_reserva = ven.rres_seq_reserva
                   AND ven.ind_tipo_registro NOT IN ('O', 'D', 'IT')
                   AND ( (r.ind_tippag IS NULL AND (ven.Ind_Facturable = 'S'))
                        OR (r.ind_tippag IS NOT NULL
                            AND ven.ind_tipo_registro <> 'CH'))
                   AND ven.ind_contra_apunte = 'N'
                   AND ven.Rvp_Ind_Tipo_Imp = imp.Ind_Tipo_Imp(+)
                   AND ven.Rvp_Cod_Impuesto = imp.Cod_Impuesto(+)
                   AND ven.Rvp_Cod_Esquema = imp.Cod_Esquema(+)
                   AND ven.Rvp_Cod_Clasif = imp.Cod_Clasif(+)
                   AND ven.Rvp_Cod_Empresa = imp.Cod_Emp_Atlas(+)),
           0)
           Tax_TTV_EUR_TOMS,
        (SELECT NVL (SUM (impuesto_canal), 0)
           FROM (SELECT SUM (
                           NVL (
                              Re_Pk_Admon.Re_Fu_Cambio_Divisa (
                                 rf.semp_cod_emp,
                                 Re_Pk_Admon.Cambio_Res,
                                 r.fec_creacion,
                                 r.gdiv_Cod_Divisa,
                                 'EUR',
                                 DECODE (
                                    ind_tipo_regimen_fac,
                                    'E', imp_margen_canal
                                         * (1
                                            - (1
                                               / (1
                                                  + (vta.pct_impuesto
                                                     / 100)))),
                                    imp_venta
                                    * (1
                                       - (1
                                          / (1 + (vta.pct_impuesto / 100))))
                                    - (imp_venta - imp_margen_canal)
                                      * (1
                                         - (1
                                            / (1
                                               + (cpa.pct_impuesto / 100)))))),
                              0))
                           impuesto_canal
                   FROM hbgdwc.dwc_bok_t_canco_hotel h,
                        hbgdwc.dwc_oth_v_re_v_impuesto_sap vta,
                        hbgdwc.dwc_oth_v_re_v_impuesto_sap cpa
                  WHERE     seq_rec = r.grec_seq_rec
                        AND seq_reserva = r.seq_reserva
                        AND ind_tipo_imp_vta_fac = vta.ind_tipo_imp
                        AND cod_impuesto_vta_fac = vta.cod_impuesto
                        AND cod_clasif_vta_fac = vta.cod_clasif
                        AND cod_esquema_vta_fac = vta.cod_esquema
                        AND cod_empresa_vta_fac = vta.cod_emp_atlas
                        AND ind_tipo_imp_vta_fac = cpa.ind_tipo_imp
                        AND cod_impuesto_vta_fac = cpa.cod_impuesto
                        AND cod_clasif_vta_fac = cpa.cod_clasif
                        AND cod_esquema_vta_fac = cpa.cod_esquema
                        AND cod_empresa_vta_fac = cpa.cod_emp_atlas
                 UNION
                 SELECT SUM (
                           NVL (
                              Re_Pk_Admon.Re_Fu_Cambio_Divisa (
                                 rf.semp_cod_emp,
                                 Re_Pk_Admon.Cambio_Res,
                                 r.fec_creacion,
                                 r.gdiv_Cod_Divisa,
                                 'EUR',
                                 DECODE (
                                    ind_tipo_regimen_fac,
                                    'E', imp_margen_canal
                                         * (1
                                            - (1
                                               / (1
                                                  + (vta.pct_impuesto
                                                     / 100)))),
                                    imp_venta
                                    * (1
                                       - (1
                                          / (1 + (vta.pct_impuesto / 100))))
                                    - (imp_venta - imp_margen_canal)
                                      * (1
                                         - (1
                                            / (1
                                               + (cpa.pct_impuesto / 100)))))),
                              0))
                           impuesto_canal
                   FROM hbgdwc.dwc_bok_t_canco_hotel_circuit h,
                        hbgdwc.dwc_oth_v_re_v_impuesto_sap vta,
                        hbgdwc.dwc_oth_v_re_v_impuesto_sap cpa
                  WHERE     seq_rec = r.grec_seq_rec
                        AND seq_reserva = r.seq_reserva
                        AND ind_tipo_imp_vta_fac = vta.ind_tipo_imp
                        AND cod_impuesto_vta_fac = vta.cod_impuesto
                        AND cod_clasif_vta_fac = vta.cod_clasif
                        AND cod_esquema_vta_fac = vta.cod_esquema
                        AND cod_empresa_vta_fac = vta.cod_emp_atlas
                        AND ind_tipo_imp_vta_fac = cpa.ind_tipo_imp
                        AND cod_impuesto_vta_fac = cpa.cod_impuesto
                        AND cod_clasif_vta_fac = cpa.cod_clasif
                        AND cod_esquema_vta_fac = cpa.cod_esquema
                        AND cod_empresa_vta_fac = cpa.cod_emp_atlas
                 UNION
                 SELECT SUM (
                           NVL (
                              Re_Pk_Admon.Re_Fu_Cambio_Divisa (
                                 rf.semp_cod_emp,
                                 Re_Pk_Admon.Cambio_Res,
                                 r.fec_creacion,
                                 r.gdiv_Cod_Divisa,
                                 'EUR',
                                 DECODE (
                                    ind_tipo_regimen_fac,
                                    'E', imp_margen_canal
                                         * (1
                                            - (1
                                               / (1
                                                  + (vta.pct_impuesto
                                                     / 100)))),
                                    imp_venta
                                    * (1
                                       - (1
                                          / (1 + (vta.pct_impuesto / 100))))
                                    - (imp_venta - imp_margen_canal)
                                      * (1
                                         - (1
                                            / (1
                                               + (cpa.pct_impuesto / 100)))))),
                              0))
                           impuesto_canal
                   FROM hbgdwc.dwc_bok_t_canco_other h,
                        hbgdwc.dwc_oth_v_re_v_impuesto_sap vta,
                        hbgdwc.dwc_oth_v_re_v_impuesto_sap cpa
                  WHERE     seq_rec = r.grec_seq_rec
                        AND seq_reserva = r.seq_reserva
                        AND ind_tipo_imp_vta_fac = vta.ind_tipo_imp
                        AND cod_impuesto_vta_fac = vta.cod_impuesto
                        AND cod_clasif_vta_fac = vta.cod_clasif
                        AND cod_esquema_vta_fac = vta.cod_esquema
                        AND cod_empresa_vta_fac = vta.cod_emp_atlas
                        AND ind_tipo_imp_vta_fac = cpa.ind_tipo_imp
                        AND cod_impuesto_vta_fac = cpa.cod_impuesto
                        AND cod_clasif_vta_fac = cpa.cod_clasif
                        AND cod_esquema_vta_fac = cpa.cod_esquema
                        AND cod_empresa_vta_fac = cpa.cod_emp_atlas
                 UNION
                 SELECT SUM (
                           NVL (
                              Re_Pk_Admon.Re_Fu_Cambio_Divisa (
                                 rf.semp_cod_emp,
                                 Re_Pk_Admon.Cambio_Res,
                                 r.fec_creacion,
                                 r.gdiv_Cod_Divisa,
                                 'EUR',
                                 DECODE (
                                    ind_tipo_regimen_fac,
                                    'E', imp_margen_canal
                                         * (1
                                            - (1
                                               / (1
                                                  + (vta.pct_impuesto
                                                     / 100)))),
                                    imp_venta
                                    * (1
                                       - (1
                                          / (1 + (vta.pct_impuesto / 100))))
                                    - (imp_venta - imp_margen_canal)
                                      * (1
                                         - (1
                                            / (1
                                               + (cpa.pct_impuesto / 100)))))),
                              0))
                           impuesto_canal
                   FROM hbgdwc.dwc_bok_t_canco_transfer h,
                        hbgdwc.dwc_oth_v_re_v_impuesto_sap vta,
                        hbgdwc.dwc_oth_v_re_v_impuesto_sap cpa
                  WHERE     seq_rec = r.grec_seq_rec
                        AND seq_reserva = r.seq_reserva
                        AND ind_tipo_imp_vta_fac = vta.ind_tipo_imp
                        AND cod_impuesto_vta_fac = vta.cod_impuesto
                        AND cod_clasif_vta_fac = vta.cod_clasif
                        AND cod_esquema_vta_fac = vta.cod_esquema
                        AND cod_empresa_vta_fac = vta.cod_emp_atlas
                        AND ind_tipo_imp_vta_fac = cpa.ind_tipo_imp
                        AND cod_impuesto_vta_fac = cpa.cod_impuesto
                        AND cod_clasif_vta_fac = cpa.cod_clasif
                        AND cod_esquema_vta_fac = cpa.cod_esquema
                        AND cod_empresa_vta_fac = cpa.cod_emp_atlas
                 UNION
                 SELECT SUM (
                           NVL (
                              Re_Pk_Admon.Re_Fu_Cambio_Divisa (
                                 rf.semp_cod_emp,
                                 Re_Pk_Admon.Cambio_Res,
                                 r.fec_creacion,
                                 r.gdiv_Cod_Divisa,
                                 'EUR',
                                 DECODE (
                                    ind_tipo_regimen_fac,
                                    'E', imp_margen_canal
                                         * (1
                                            - (1
                                               / (1
                                                  + (vta.pct_impuesto
                                                     / 100)))),
                                    imp_venta
                                    * (1
                                       - (1
                                          / (1 + (vta.pct_impuesto / 100))))
                                    - (imp_venta - imp_margen_canal)
                                      * (1
                                         - (1
                                            / (1
                                               + (cpa.pct_impuesto / 100)))))),
                              0))
                           impuesto_canal
                   FROM hbgdwc.dwc_bok_t_canco_endowments h,
                        hbgdwc.dwc_oth_v_re_v_impuesto_sap vta,
                        hbgdwc.dwc_oth_v_re_v_impuesto_sap cpa
                  WHERE     seq_rec = r.grec_seq_rec
                        AND seq_reserva = r.seq_reserva
                        AND ind_tipo_imp_vta_fac = vta.ind_tipo_imp
                        AND cod_impuesto_vta_fac = vta.cod_impuesto
                        AND cod_clasif_vta_fac = vta.cod_clasif
                        AND cod_esquema_vta_fac = vta.cod_esquema
                        AND cod_empresa_vta_fac = vta.cod_emp_atlas
                        AND ind_tipo_imp_vta_fac = cpa.ind_tipo_imp
                        AND cod_impuesto_vta_fac = cpa.cod_impuesto
                        AND cod_clasif_vta_fac = cpa.cod_clasif
                        AND cod_esquema_vta_fac = cpa.cod_esquema
                        AND cod_empresa_vta_fac = cpa.cod_emp_atlas
                 UNION
                 SELECT SUM (
                           NVL (
                              Re_Pk_Admon.Re_Fu_Cambio_Divisa (
                                 rf.semp_cod_emp,
                                 Re_Pk_Admon.Cambio_Res,
                                 r.fec_creacion,
                                 r.gdiv_Cod_Divisa,
                                 'EUR',
                                 DECODE (
                                    ind_tipo_regimen_fac,
                                    'E', imp_margen_canal
                                         * (1
                                            - (1
                                               / (1
                                                  + (vta.pct_impuesto
                                                     / 100)))),
                                    imp_venta
                                    * (1
                                       - (1
                                          / (1 + (vta.pct_impuesto / 100))))
                                    - (imp_venta - imp_margen_canal)
                                      * (1
                                         - (1
                                            / (1
                                               + (cpa.pct_impuesto / 100)))))),
                              0))
                           impuesto_canal
                   FROM hbgdwc.dwc_bok_t_canco_extra cce,
                        hbgdwc.dwc_oth_v_re_v_impuesto_sap vta,
                        hbgdwc.dwc_oth_v_re_v_impuesto_sap cpa
                  WHERE     seq_rec = r.grec_seq_rec
                        AND seq_reserva = r.seq_reserva
                        AND ind_tipo_imp_vta_fac = vta.ind_tipo_imp
                        AND cod_impuesto_vta_fac = vta.cod_impuesto
                        AND cod_clasif_vta_fac = vta.cod_clasif
                        AND cod_esquema_vta_fac = vta.cod_esquema
                        AND cod_empresa_vta_fac = vta.cod_emp_atlas
                        AND ind_tipo_imp_vta_fac = cpa.ind_tipo_imp
                        AND cod_impuesto_vta_fac = cpa.cod_impuesto
                        AND cod_clasif_vta_fac = cpa.cod_clasif
                        AND cod_esquema_vta_fac = cpa.cod_esquema
                        AND cod_empresa_vta_fac = cpa.cod_emp_atlas
                        AND (CCE.seq_rec, CCE.seq_reserva, CCE.ord_extra) NOT IN
                               (SELECT EXT.grec_seq_rec,
                                       EXT.rres_seq_reserva,
                                       EXT.ord_extra
                                  FROM hbgdwc.dwc_bok_t_extra EXT,
                                       hbgdwc.dwc_cli_dir_t_cd_discount_bond BON,
                                       hbgdwc.dwc_cli_dir_t_cd_campaign CAM
                                 WHERE EXT.num_bono = BON.num_bono
                                       AND EXT.cod_interface =
                                              BON.cod_interface
                                       AND BON.cod_campana = CAM.cod_campana
                                       AND BON.cod_interface =
                                              CAM.cod_interface
                                       AND CAM.ind_rentabilidad = 'N')))
           Tax_Sales_Transfer_pricing_EUR,
        (SELECT NVL (SUM (impuesto_canco + (SELECT NVL (SUM (Re_Pk_Admon.Re_Fu_Cambio_Divisa (
                                               rf.semp_cod_emp,
                                               Re_Pk_Admon.Cambio_Res,
                                               r.fec_creacion,
                                               cst.sdiv_cod_divisa,
                                               'EUR',
                                               re_pk_reser1.re_fu_calcular_noch_imp (
                                                  cst.fec_desde,
                                                  cst.fec_hasta,
                                                  cst.nro_unidades,
                                                  cst.nro_pax,
                                                  cst.ind_tipo_unid,
                                                  cst.ind_p_s,
                                                  cst.imp_unitario))),
                                       0)
                             FROM hbgdwc.dwc_bok_t_cost cst
                            WHERE     cst.grec_seq_rec = r.grec_seq_rec
                                  AND cst.rres_seq_reserva = r.seq_reserva
                                  AND cst.ind_tipo_registro = 'DR'
                                  AND cst.ind_facturable = 'S'
                                  AND NOT EXISTS
                                             (SELECT 1
                                                FROM hbgdwc.dwc_bok_t_extra EXT,
                                                     hbgdwc.dwc_cli_dir_t_cd_discount_bond BON,
                                                     hbgdwc.dwc_cli_dir_t_cd_campaign CAM
                                               WHERE CST.grec_seq_rec = EXT.grec_seq_rec
                                                     AND CST.rres_seq_reserva = EXT.rres_seq_reserva
                                                     AND CST.rext_ord_extra = EXT.ord_extra
                                                     AND EXT.num_bono = BON.num_bono
                                                     AND EXT.cod_interface = BON.cod_interface
                                                     AND BON.cod_campana = CAM.cod_campana
                                                     AND BON.cod_interface = CAM.cod_interface
                                                     AND CAM.ind_rentabilidad = 'N'))), 0)
           FROM (SELECT SUM (
                           NVL (
                              Re_Pk_Admon.Re_Fu_Cambio_Divisa (
                                 rf.semp_cod_emp,
                                 Re_Pk_Admon.Cambio_Res,
                                 r.fec_creacion,
                                 r.gdiv_Cod_Divisa,
                                 'EUR',
                                 DECODE (
                                    ind_tipo_regimen_fac,
                                    'E', imp_margen_canal
                                         * (1
                                            - (1
                                               / (1
                                                  + (vta.pct_impuesto
                                                     / 100)))),
                                    imp_venta
                                    * (1
                                       - (1
                                          / (1 + (vta.pct_impuesto / 100))))
                                    - (imp_venta - imp_margen_canal)
                                      * (1
                                         - (1
                                            / (1
                                               + (cpa.pct_impuesto / 100)))))),
                              0))
                        + SUM (
                             NVL (
                                Re_Pk_Admon.Re_Fu_Cambio_Divisa (
                                   rf.semp_cod_emp,
                                   Re_Pk_Admon.Cambio_Res,
                                   r.fec_creacion,
                                   r.gdiv_Cod_Divisa,
                                   'EUR',
                                   DECODE (
                                      ind_tipo_regimen_con,
                                      'E', imp_margen_canco
                                           * (1
                                              - (1
                                                 / (1
                                                    + (vta.pct_impuesto
                                                       / 100)))),
                                      (imp_coste + imp_margen_canco)
                                      * (1
                                         - (1
                                            / (1
                                               + (vta.pct_impuesto / 100))))
                                      - (imp_coste)
                                        * (1
                                           - (1
                                              / (1
                                                 + (cpa.pct_impuesto / 100)))))),
                                0))
                           impuesto_canco
                   FROM hbgdwc.dwc_bok_t_canco_hotel h,
                        hbgdwc.dwc_oth_v_re_v_impuesto_sap vta,
                        hbgdwc.dwc_oth_v_re_v_impuesto_sap cpa
                  WHERE     seq_rec = r.grec_seq_rec
                        AND seq_reserva = r.seq_reserva
                        AND ind_tipo_imp_vta_fac = vta.ind_tipo_imp
                        AND cod_impuesto_vta_fac = vta.cod_impuesto
                        AND cod_clasif_vta_fac = vta.cod_clasif
                        AND cod_esquema_vta_fac = vta.cod_esquema
                        AND cod_empresa_vta_fac = vta.cod_emp_atlas
                        AND ind_tipo_imp_vta_fac = cpa.ind_tipo_imp
                        AND cod_impuesto_vta_fac = cpa.cod_impuesto
                        AND cod_clasif_vta_fac = cpa.cod_clasif
                        AND cod_esquema_vta_fac = cpa.cod_esquema
                        AND cod_empresa_vta_fac = cpa.cod_emp_atlas
                 UNION
                 SELECT SUM (
                           NVL (
                              Re_Pk_Admon.Re_Fu_Cambio_Divisa (
                                 rf.semp_cod_emp,
                                 Re_Pk_Admon.Cambio_Res,
                                 r.fec_creacion,
                                 r.gdiv_Cod_Divisa,
                                 'EUR',
                                 DECODE (
                                    ind_tipo_regimen_fac,
                                    'E', imp_margen_canal
                                         * (1
                                            - (1
                                               / (1
                                                  + (vta.pct_impuesto
                                                     / 100)))),
                                    imp_venta
                                    * (1
                                       - (1
                                          / (1 + (vta.pct_impuesto / 100))))
                                    - (imp_venta - imp_margen_canal)
                                      * (1
                                         - (1
                                            / (1
                                               + (cpa.pct_impuesto / 100)))))),
                              0))
                        + SUM (
                             NVL (
                                Re_Pk_Admon.Re_Fu_Cambio_Divisa (
                                   rf.semp_cod_emp,
                                   Re_Pk_Admon.Cambio_Res,
                                   r.fec_creacion,
                                   r.gdiv_Cod_Divisa,
                                   'EUR',
                                   DECODE (
                                      ind_tipo_regimen_con,
                                      'E', imp_margen_canco
                                           * (1
                                              - (1
                                                 / (1
                                                    + (vta.pct_impuesto
                                                       / 100)))),
                                      (imp_coste + imp_margen_canco)
                                      * (1
                                         - (1
                                            / (1
                                               + (vta.pct_impuesto / 100))))
                                      - (imp_coste)
                                        * (1
                                           - (1
                                              / (1
                                                 + (cpa.pct_impuesto / 100)))))),
                                0))
                           impuesto_canco
                   FROM hbgdwc.dwc_bok_t_canco_hotel_circuit h,
                        hbgdwc.dwc_oth_v_re_v_impuesto_sap vta,
                        hbgdwc.dwc_oth_v_re_v_impuesto_sap cpa
                  WHERE     seq_rec = r.grec_seq_rec
                        AND seq_reserva = r.seq_reserva
                        AND ind_tipo_imp_vta_fac = vta.ind_tipo_imp
                        AND cod_impuesto_vta_fac = vta.cod_impuesto
                        AND cod_clasif_vta_fac = vta.cod_clasif
                        AND cod_esquema_vta_fac = vta.cod_esquema
                        AND cod_empresa_vta_fac = vta.cod_emp_atlas
                        AND ind_tipo_imp_vta_fac = cpa.ind_tipo_imp
                        AND cod_impuesto_vta_fac = cpa.cod_impuesto
                        AND cod_clasif_vta_fac = cpa.cod_clasif
                        AND cod_esquema_vta_fac = cpa.cod_esquema
                        AND cod_empresa_vta_fac = cpa.cod_emp_atlas
                 UNION
                 SELECT SUM (
                           NVL (
                              Re_Pk_Admon.Re_Fu_Cambio_Divisa (
                                 rf.semp_cod_emp,
                                 Re_Pk_Admon.Cambio_Res,
                                 r.fec_creacion,
                                 r.gdiv_Cod_Divisa,
                                 'EUR',
                                 DECODE (
                                    ind_tipo_regimen_fac,
                                    'E', imp_margen_canal
                                         * (1
                                            - (1
                                               / (1
                                                  + (vta.pct_impuesto
                                                     / 100)))),
                                    imp_venta
                                    * (1
                                       - (1
                                          / (1 + (vta.pct_impuesto / 100))))
                                    - (imp_venta - imp_margen_canal)
                                      * (1
                                         - (1
                                            / (1
                                               + (cpa.pct_impuesto / 100)))))),
                              0))
                        + SUM (
                             NVL (
                                Re_Pk_Admon.Re_Fu_Cambio_Divisa (
                                   rf.semp_cod_emp,
                                   Re_Pk_Admon.Cambio_Res,
                                   r.fec_creacion,
                                   r.gdiv_Cod_Divisa,
                                   'EUR',
                                   DECODE (
                                      ind_tipo_regimen_con,
                                      'E', imp_margen_canco
                                           * (1
                                              - (1
                                                 / (1
                                                    + (vta.pct_impuesto
                                                       / 100)))),
                                      (imp_coste + imp_margen_canco)
                                      * (1
                                         - (1
                                            / (1
                                               + (vta.pct_impuesto / 100))))
                                      - (imp_coste)
                                        * (1
                                           - (1
                                              / (1
                                                 + (cpa.pct_impuesto / 100)))))),
                                0))
                           impuesto_canco
                   FROM hbgdwc.dwc_bok_t_canco_other h,
                        hbgdwc.dwc_oth_v_re_v_impuesto_sap vta,
                        hbgdwc.dwc_oth_v_re_v_impuesto_sap cpa
                  WHERE     seq_rec = r.grec_seq_rec
                        AND seq_reserva = r.seq_reserva
                        AND ind_tipo_imp_vta_fac = vta.ind_tipo_imp
                        AND cod_impuesto_vta_fac = vta.cod_impuesto
                        AND cod_clasif_vta_fac = vta.cod_clasif
                        AND cod_esquema_vta_fac = vta.cod_esquema
                        AND cod_empresa_vta_fac = vta.cod_emp_atlas
                        AND ind_tipo_imp_vta_fac = cpa.ind_tipo_imp
                        AND cod_impuesto_vta_fac = cpa.cod_impuesto
                        AND cod_clasif_vta_fac = cpa.cod_clasif
                        AND cod_esquema_vta_fac = cpa.cod_esquema
                        AND cod_empresa_vta_fac = cpa.cod_emp_atlas
                 UNION
                 SELECT SUM (
                           NVL (
                              Re_Pk_Admon.Re_Fu_Cambio_Divisa (
                                 rf.semp_cod_emp,
                                 Re_Pk_Admon.Cambio_Res,
                                 r.fec_creacion,
                                 r.gdiv_Cod_Divisa,
                                 'EUR',
                                 DECODE (
                                    ind_tipo_regimen_fac,
                                    'E', imp_margen_canal
                                         * (1
                                            - (1
                                               / (1
                                                  + (vta.pct_impuesto
                                                     / 100)))),
                                    imp_venta
                                    * (1
                                       - (1
                                          / (1 + (vta.pct_impuesto / 100))))
                                    - (imp_venta - imp_margen_canal)
                                      * (1
                                         - (1
                                            / (1
                                               + (cpa.pct_impuesto / 100)))))),
                              0))
                        + SUM (
                             NVL (
                                Re_Pk_Admon.Re_Fu_Cambio_Divisa (
                                   rf.semp_cod_emp,
                                   Re_Pk_Admon.Cambio_Res,
                                   r.fec_creacion,
                                   r.gdiv_Cod_Divisa,
                                   'EUR',
                                   DECODE (
                                      ind_tipo_regimen_con,
                                      'E', imp_margen_canco
                                           * (1
                                              - (1
                                                 / (1
                                                    + (vta.pct_impuesto
                                                       / 100)))),
                                      (imp_coste + imp_margen_canco)
                                      * (1
                                         - (1
                                            / (1
                                               + (vta.pct_impuesto / 100))))
                                      - (imp_coste)
                                        * (1
                                           - (1
                                              / (1
                                                 + (cpa.pct_impuesto / 100)))))),
                                0))
                           impuesto_canco
                   FROM hbgdwc.dwc_bok_t_canco_transfer h,
                        hbgdwc.dwc_oth_v_re_v_impuesto_sap vta,
                        hbgdwc.dwc_oth_v_re_v_impuesto_sap cpa
                  WHERE     seq_rec = r.grec_seq_rec
                        AND seq_reserva = r.seq_reserva
                        AND ind_tipo_imp_vta_fac = vta.ind_tipo_imp
                        AND cod_impuesto_vta_fac = vta.cod_impuesto
                        AND cod_clasif_vta_fac = vta.cod_clasif
                        AND cod_esquema_vta_fac = vta.cod_esquema
                        AND cod_empresa_vta_fac = vta.cod_emp_atlas
                        AND ind_tipo_imp_vta_fac = cpa.ind_tipo_imp
                        AND cod_impuesto_vta_fac = cpa.cod_impuesto
                        AND cod_clasif_vta_fac = cpa.cod_clasif
                        AND cod_esquema_vta_fac = cpa.cod_esquema
                        AND cod_empresa_vta_fac = cpa.cod_emp_atlas
                 UNION
                 SELECT SUM (
                           NVL (
                              Re_Pk_Admon.Re_Fu_Cambio_Divisa (
                                 rf.semp_cod_emp,
                                 Re_Pk_Admon.Cambio_Res,
                                 r.fec_creacion,
                                 r.gdiv_Cod_Divisa,
                                 'EUR',
                                 DECODE (
                                    ind_tipo_regimen_fac,
                                    'E', imp_margen_canal
                                         * (1
                                            - (1
                                               / (1
                                                  + (vta.pct_impuesto
                                                     / 100)))),
                                    imp_venta
                                    * (1
                                       - (1
                                          / (1 + (vta.pct_impuesto / 100))))
                                    - (imp_venta - imp_margen_canal)
                                      * (1
                                         - (1
                                            / (1
                                               + (cpa.pct_impuesto / 100)))))),
                              0))
                        + SUM (
                             NVL (
                                Re_Pk_Admon.Re_Fu_Cambio_Divisa (
                                   rf.semp_cod_emp,
                                   Re_Pk_Admon.Cambio_Res,
                                   r.fec_creacion,
                                   r.gdiv_Cod_Divisa,
                                   'EUR',
                                   DECODE (
                                      ind_tipo_regimen_con,
                                      'E', imp_margen_canco
                                           * (1
                                              - (1
                                                 / (1
                                                    + (vta.pct_impuesto
                                                       / 100)))),
                                      (imp_coste + imp_margen_canco)
                                      * (1
                                         - (1
                                            / (1
                                               + (vta.pct_impuesto / 100))))
                                      - (imp_coste)
                                        * (1
                                           - (1
                                              / (1
                                                 + (cpa.pct_impuesto / 100)))))),
                                0))
                           impuesto_canco
                   FROM hbgdwc.dwc_bok_t_canco_endowments h,
                        hbgdwc.dwc_oth_v_re_v_impuesto_sap vta,
                        hbgdwc.dwc_oth_v_re_v_impuesto_sap cpa
                  WHERE     seq_rec = r.grec_seq_rec
                        AND seq_reserva = r.seq_reserva
                        AND ind_tipo_imp_vta_fac = vta.ind_tipo_imp
                        AND cod_impuesto_vta_fac = vta.cod_impuesto
                        AND cod_clasif_vta_fac = vta.cod_clasif
                        AND cod_esquema_vta_fac = vta.cod_esquema
                        AND cod_empresa_vta_fac = vta.cod_emp_atlas
                        AND ind_tipo_imp_vta_fac = cpa.ind_tipo_imp
                        AND cod_impuesto_vta_fac = cpa.cod_impuesto
                        AND cod_clasif_vta_fac = cpa.cod_clasif
                        AND cod_esquema_vta_fac = cpa.cod_esquema
                        AND cod_empresa_vta_fac = cpa.cod_emp_atlas
                 UNION
                 SELECT SUM (
                           NVL (
                              Re_Pk_Admon.Re_Fu_Cambio_Divisa (
                                 rf.semp_cod_emp,
                                 Re_Pk_Admon.Cambio_Res,
                                 r.fec_creacion,
                                 r.gdiv_Cod_Divisa,
                                 'EUR',
                                 DECODE (
                                    ind_tipo_regimen_fac,
                                    'E', imp_margen_canal
                                         * (1
                                            - (1
                                               / (1
                                                  + (vta.pct_impuesto
                                                     / 100)))),
                                    imp_venta
                                    * (1
                                       - (1
                                          / (1 + (vta.pct_impuesto / 100))))
                                    - (imp_venta - imp_margen_canal)
                                      * (1
                                         - (1
                                            / (1
                                               + (cpa.pct_impuesto / 100)))))),
                              0))
                        + SUM (
                             NVL (
                                Re_Pk_Admon.Re_Fu_Cambio_Divisa (
                                   rf.semp_cod_emp,
                                   Re_Pk_Admon.Cambio_Res,
                                   r.fec_creacion,
                                   r.gdiv_Cod_Divisa,
                                   'EUR',
                                   DECODE (
                                      ind_tipo_regimen_con,
                                      'E', imp_margen_canco
                                           * (1
                                              - (1
                                                 / (1
                                                    + (vta.pct_impuesto
                                                       / 100)))),
                                      (imp_coste + imp_margen_canco)
                                      * (1
                                         - (1
                                            / (1
                                               + (vta.pct_impuesto / 100))))
                                      - (imp_coste)
                                        * (1
                                           - (1
                                              / (1
                                                 + (cpa.pct_impuesto / 100)))))),
                                0))
                           impuesto_canco
                   FROM hbgdwc.dwc_bok_t_canco_extra cce,
                        hbgdwc.dwc_oth_v_re_v_impuesto_sap vta,
                        hbgdwc.dwc_oth_v_re_v_impuesto_sap cpa
                  WHERE     seq_rec = r.grec_seq_rec
                        AND seq_reserva = r.seq_reserva
                        AND ind_tipo_imp_vta_fac = vta.ind_tipo_imp
                        AND cod_impuesto_vta_fac = vta.cod_impuesto
                        AND cod_clasif_vta_fac = vta.cod_clasif
                        AND cod_esquema_vta_fac = vta.cod_esquema
                        AND cod_empresa_vta_fac = vta.cod_emp_atlas
                        AND ind_tipo_imp_vta_fac = cpa.ind_tipo_imp
                        AND cod_impuesto_vta_fac = cpa.cod_impuesto
                        AND cod_clasif_vta_fac = cpa.cod_clasif
                        AND cod_esquema_vta_fac = cpa.cod_esquema
                        AND cod_empresa_vta_fac = cpa.cod_emp_atlas
                        AND (CCE.seq_rec, CCE.seq_reserva, CCE.ord_extra) NOT IN
                               (SELECT EXT.grec_seq_rec,
                                       EXT.rres_seq_reserva,
                                       EXT.ord_extra
                                  FROM hbgdwc.dwc_bok_t_extra EXT,
                                       hbgdwc.dwc_cli_dir_t_cd_discount_bond BON,
                                       hbgdwc.dwc_cli_dir_t_cd_campaign CAM
                                 WHERE EXT.num_bono = BON.num_bono
                                       AND EXT.cod_interface =
                                              BON.cod_interface
                                       AND BON.cod_campana = CAM.cod_campana
                                       AND BON.cod_interface =
                                              CAM.cod_interface
                                       AND CAM.ind_rentabilidad = 'N')))
           Tax_Transfer_pricing_EUR,
        (SELECT NVL (SUM (Re_Pk_Admon.Re_Fu_Cambio_Divisa (
                             rf.semp_cod_emp,
                             Re_Pk_Admon.Cambio_Res,
                             r.fec_creacion, --DECODE (I.IND_FEC_CAM_DIV,                                       'E', r.fec_desde,                                       r.fec_creacion),
                             GDiv_Cod_Divisa,
                             'EUR',
                             Re_Pk_Reser1.Re_Fu_Calcular_Noch_Imp (
                                V.Fec_Desde,
                                V.Fec_Hasta,
                                NRO_UNIDADES,
                                NRO_PAX,
                                IND_TIPO_UNID,
                                IND_P_S,
                                IMP_UNITARIO))),
                     0)
           FROM hbgdwc.dwc_bok_t_sale V
          WHERE     V.GRec_Seq_Rec = r.grec_seq_rec
                AND V.RRes_Seq_Reserva = r.seq_reserva
                AND V.Ind_Tipo_Registro IN ('O', 'V')
                AND V.Ind_Facturable = 'S'
                AND v.ind_contra_apunte = 'N')
           Client_EUR_commision,
        (SELECT NVL (SUM (DECODE (V.IND_TIPO_REGISTRO,
                                  'V', 0,
                                  Re_Pk_Admon.Re_Fu_Cambio_Divisa (
                                     rf.semp_cod_emp,
                                     Re_Pk_Admon.Cambio_Res,
                                     r.fec_creacion, --DECODE (I.IND_FEC_CAM_DIV,                                       'E', r.fec_desde,                                       r.fec_creacion),
                                     GDiv_Cod_Divisa,
                                     'EUR',
                                     Re_Pk_Reser1.Re_Fu_Calcular_Noch_Imp (
                                        V.Fec_Desde,
                                        V.Fec_Hasta,
                                        NRO_UNIDADES,
                                        NRO_PAX,
                                        IND_TIPO_UNID,
                                        IND_P_S,
                                        Re_Pk_Admon.RE_FU_REDONDEO (
                                           IMP_UNITARIO
                                           - (IMP_UNITARIO
                                              / (1 + (pct_impuesto / 100))),
                                           r.gdiv_cod_divisa))))),
                     0)
           FROM hbgdwc.dwc_bok_t_sale V, hbgdwc.dwc_oth_v_re_v_impuesto_sap
          WHERE     V.GRec_Seq_Rec = r.grec_seq_rec
                AND V.RRes_Seq_Reserva = r.seq_reserva
                AND V.Ind_Tipo_Registro IN ('O', 'V')
                AND V.Ind_Facturable = 'S'
                AND v.ind_contra_apunte = 'N'
                AND Rvp_Ind_Tipo_Imp = Ind_Tipo_Imp(+)
                AND Rvp_Cod_Impuesto = Cod_Impuesto(+)
                AND Rvp_Cod_Esquema = Cod_Esquema(+)
                AND Rvp_Cod_Clasif = Cod_Clasif(+)
                AND Rvp_Cod_Empresa = Cod_Emp_Atlas(+))
           Tax_Client_EUR_commision,
        (SELECT NVL (SUM (Re_Pk_Admon.Re_Fu_Cambio_Divisa (
                             rf.semp_cod_emp,
                             Re_Pk_Admon.Cambio_Res,
                             r.fec_creacion, --DECODE (I.IND_FEC_CAM_DIV,                                       'E', r.fec_desde,                                       r.fec_creacion),
                             GDiv_Cod_Divisa,
                             'EUR',
                             Re_Pk_Reser1.Re_Fu_Calcular_Noch_Imp (
                                V.Fec_Desde,
                                V.Fec_Hasta,
                                NRO_UNIDADES,
                                NRO_PAX,
                                IND_TIPO_UNID,
                                IND_P_S,
                                IMP_UNITARIO))),
                     0)
           FROM hbgdwc.dwc_bok_t_sale V
          WHERE     V.GRec_Seq_Rec = r.grec_seq_rec
                AND V.RRes_Seq_Reserva = r.seq_reserva
                AND V.Ind_Tipo_Registro IN ('D')
                AND V.Ind_Facturable = 'S'
                AND v.ind_contra_apunte = 'N')
           Client_EUR_rappel,
        (SELECT NVL (SUM (DECODE (V.IND_TIPO_REGISTRO,
                                  'V', 0,
                                  Re_Pk_Admon.Re_Fu_Cambio_Divisa (
                                     rf.semp_cod_emp,
                                     Re_Pk_Admon.Cambio_Res,
                                     r.fec_creacion, --DECODE (I.IND_FEC_CAM_DIV,                                       'E', r.fec_desde,                                       r.fec_creacion),
                                     GDiv_Cod_Divisa,
                                     'EUR',
                                     Re_Pk_Reser1.Re_Fu_Calcular_Noch_Imp (
                                        V.Fec_Desde,
                                        V.Fec_Hasta,
                                        NRO_UNIDADES,
                                        NRO_PAX,
                                        IND_TIPO_UNID,
                                        IND_P_S,
                                        Re_Pk_Admon.RE_FU_REDONDEO (
                                           IMP_UNITARIO
                                           - (IMP_UNITARIO
                                              / (1 + (pct_impuesto / 100))),
                                           r.gdiv_cod_divisa))))),
                     0)
           FROM hbgdwc.dwc_bok_t_sale V, hbgdwc.dwc_oth_v_re_v_impuesto_sap
          WHERE     V.GRec_Seq_Rec = r.grec_seq_rec
                AND V.RRes_Seq_Reserva = r.seq_reserva
                AND V.Ind_Tipo_Registro IN ('D')
                AND V.Ind_Facturable = 'S'
                AND v.ind_contra_apunte = 'N'
                AND Rvp_Ind_Tipo_Imp = Ind_Tipo_Imp(+)
                AND Rvp_Cod_Impuesto = Cod_Impuesto(+)
                AND Rvp_Cod_Esquema = Cod_Esquema(+)
                AND Rvp_Cod_Clasif = Cod_Clasif(+)
                AND Rvp_Cod_Empresa = Cod_Emp_Atlas(+))
           Tax_Client_EUR_rappel,
        NVL (
           (SELECT NVL (SUM (DECODE (cst.ind_tipo_registro,
                                     'SC', 0,
                                     Re_Pk_Admon.Re_Fu_Cambio_Divisa (
                                        rf.semp_cod_emp,
                                        Re_Pk_Admon.Cambio_Res,
                                        r.fec_creacion, --DECODE (I.IND_FEC_CAM_DIV,                                       'E', r.fec_desde,                                       r.fec_creacion),
                                        cst.SDiv_Cod_Divisa,
                                        'EUR',
                                        Re_Pk_Reser1.Re_Fu_Calcular_Noch_Imp (
                                           cst.Fec_Desde,
                                           cst.Fec_Hasta,
                                           cst.NRO_UNIDADES,
                                           cst.NRO_PAX,
                                           cst.IND_TIPO_UNID,
                                           cst.IND_P_S,
                                           cst.IMP_UNITARIO)))),
                        0)
              FROM hbgdwc.dwc_bok_t_cost CST, hbgdwc.dwc_oth_v_re_v_impuesto_sap IMP
             WHERE   cst.GRec_Seq_Rec = r.grec_seq_rec
                   AND cst.RRes_Seq_Reserva = r.seq_reserva
                   AND cst.Ind_Tipo_Registro NOT IN
                          ('R', 'DR', 'OV', 'OC', 'CA', 'IT', 'PF', 'LL')
                   AND (cst.Ind_Facturable = 'S' OR r.ind_tippag IS NOT NULL)
                   AND cst.ind_contra_apunte = 'N'
                   AND cst.Rvp_Ind_Tipo_Imp = imp.Ind_Tipo_Imp(+)
                   AND cst.Rvp_Cod_Impuesto = imp.Cod_Impuesto(+)
                   AND cst.Rvp_Cod_Esquema = imp.Cod_Esquema(+)
                   AND cst.Rvp_Cod_Clasif = imp.Cod_Clasif(+)
                   AND cst.Rvp_Cod_Empresa = imp.Cod_Emp_Atlas(+)),
           0)
           Cost_EUR_currency,
        NVL (
           (SELECT NVL (
                      SUM (
                         DECODE (
                            cst.ind_tipo_registro,
                            'LL', 0,
                            DECODE (
                               cst.Ind_Tipo_Regimen
                               || NVL (cst.ind_trfprc, 'N'),
                               'GN',                                --TDAF-1347
                                    Re_Pk_Admon.Re_Fu_Cambio_Divisa (
                                        rf.semp_cod_emp,
                                        Re_Pk_Admon.Cambio_Res,
                                        r.fec_creacion, --DECODE (I.IND_FEC_CAM_DIV,                                       'E', r.fec_desde,                                       r.fec_creacion),
                                        cst.SDiv_Cod_Divisa,
                                        'EUR',
                                        Re_Pk_Reser1.Re_Fu_Calcular_Noch_Imp (
                                           cst.Fec_Desde,
                                           cst.Fec_Hasta,
                                           cst.NRO_UNIDADES,
                                           cst.NRO_PAX,
                                           cst.IND_TIPO_UNID,
                                           cst.IND_P_S,
                                           cst.IMP_UNITARIO
                                           - (cst.IMP_UNITARIO
                                              / (1 + (pct_impuesto / 100)))))))),
                      0)
              FROM hbgdwc.dwc_bok_t_cost CST, hbgdwc.dwc_oth_v_re_v_impuesto_sap IMP
             WHERE   cst.GRec_Seq_Rec = r.grec_seq_rec
                   AND cst.RRes_Seq_Reserva = r.seq_reserva
                   AND cst.Ind_Tipo_Registro NOT IN
                          ('R', 'DR', 'OV', 'OC', 'CA', 'IT', 'PF', 'LL')
                   AND (cst.Ind_Facturable = 'S' OR r.ind_tippag IS NOT NULL)
                   AND cst.ind_contra_apunte = 'N'
                   AND cst.Rvp_Ind_Tipo_Imp = imp.Ind_Tipo_Imp(+)
                   AND cst.Rvp_Cod_Impuesto = imp.Cod_Impuesto(+)
                   AND cst.Rvp_Cod_Esquema = imp.Cod_Esquema(+)
                   AND cst.Rvp_Cod_Clasif = imp.Cod_Clasif(+)
                   AND cst.Rvp_Cod_Empresa = imp.Cod_Emp_Atlas(+)),
           0)
           Tax_Cost_EUR,
        NVL (
           (SELECT NVL (
                      SUM (
                         DECODE (
                            cst.ind_tipo_registro,
                            'LL', 0,
                            DECODE (
                               cst.Ind_Tipo_Regimen
                               || NVL (cst.ind_trfprc, 'N'),
                               'EN',                                --TDAF-1347
                                    Re_Pk_Admon.Re_Fu_Cambio_Divisa (
                                        rf.semp_cod_emp,
                                        Re_Pk_Admon.Cambio_Res,
                                        r.fec_creacion, --DECODE (I.IND_FEC_CAM_DIV,                                       'E', r.fec_desde,                                       r.fec_creacion),
                                        cst.SDiv_Cod_Divisa,
                                        'EUR',
                                        Re_Pk_Reser1.Re_Fu_Calcular_Noch_Imp (
                                           cst.Fec_Desde,
                                           cst.Fec_Hasta,
                                           cst.NRO_UNIDADES,
                                           cst.NRO_PAX,
                                           cst.IND_TIPO_UNID,
                                           cst.IND_P_S,
                                           cst.IMP_UNITARIO
                                           - (cst.IMP_UNITARIO
                                              / (1 + (pct_impuesto / 100)))))))),
                      0)
              FROM hbgdwc.dwc_bok_t_cost CST, hbgdwc.dwc_oth_v_re_v_impuesto_sap IMP
             WHERE   cst.GRec_Seq_Rec = r.grec_seq_rec
                   AND cst.RRes_Seq_Reserva = r.seq_reserva
                   AND cst.Ind_Tipo_Registro NOT IN
                          ('R', 'DR', 'OV', 'OC', 'CA', 'IT', 'PF', 'LL')
                   AND (cst.Ind_Facturable = 'S' OR r.ind_tippag IS NOT NULL)
                   AND cst.ind_contra_apunte = 'N'
                   AND cst.Rvp_Ind_Tipo_Imp = imp.Ind_Tipo_Imp(+)
                   AND cst.Rvp_Cod_Impuesto = imp.Cod_Impuesto(+)
                   AND cst.Rvp_Cod_Esquema = imp.Cod_Esquema(+)
                   AND cst.Rvp_Cod_Clasif = imp.Cod_Clasif(+)
                   AND cst.Rvp_Cod_Empresa = imp.Cod_Emp_Atlas(+)),
           0)
           Tax_Cost_EUR_TOMS,
        (SELECT NVL (SUM (impuesto_canco + (SELECT NVL (SUM (Re_Pk_Admon.Re_Fu_Cambio_Divisa (
                                               rf.semp_cod_emp,
                                               Re_Pk_Admon.Cambio_Res,
                                               r.fec_creacion,
                                               cst.sdiv_cod_divisa,
                                               'EUR',
                                               re_pk_reser1.re_fu_calcular_noch_imp (
                                                  cst.fec_desde,
                                                  cst.fec_hasta,
                                                  cst.nro_unidades,
                                                  cst.nro_pax,
                                                  cst.ind_tipo_unid,
                                                  cst.ind_p_s,
                                                  cst.imp_unitario))),
                                       0)
                             FROM hbgdwc.dwc_bok_t_cost cst
                            WHERE     cst.grec_seq_rec = r.grec_seq_rec
                                  AND cst.rres_seq_reserva = r.seq_reserva
                                  AND cst.ind_tipo_registro = 'DR'
                                  AND cst.ind_facturable = 'S'
                                  AND NOT EXISTS
                                             (SELECT 1
                                                FROM hbgdwc.dwc_bok_t_extra EXT,
                                                     hbgdwc.dwc_cli_dir_t_cd_discount_bond BON,
                                                     hbgdwc.dwc_cli_dir_t_cd_campaign CAM
                                               WHERE CST.grec_seq_rec = EXT.grec_seq_rec
                                                     AND CST.rres_seq_reserva = EXT.rres_seq_reserva
                                                     AND CST.rext_ord_extra = EXT.ord_extra
                                                     AND EXT.num_bono = BON.num_bono
                                                     AND EXT.cod_interface = BON.cod_interface
                                                     AND BON.cod_campana = CAM.cod_campana
                                                     AND BON.cod_interface = CAM.cod_interface
                                                     AND CAM.ind_rentabilidad = 'N'))), 0)
           FROM (SELECT SUM (
                           NVL (
                              Re_Pk_Admon.Re_Fu_Cambio_Divisa (
                                 rf.semp_cod_emp,
                                 Re_Pk_Admon.Cambio_Res,
                                 r.fec_creacion,
                                 r.gdiv_Cod_Divisa,
                                 'EUR',
                                 DECODE (
                                    ind_tipo_regimen_con,
                                    'E', imp_margen_canco
                                         * (1
                                            - (1
                                               / (1
                                                  + (vta.pct_impuesto
                                                     / 100)))),
                                    (imp_coste + imp_margen_canco)
                                    * (1
                                       - (1
                                          / (1 + (vta.pct_impuesto / 100))))
                                    - (imp_coste)
                                      * (1
                                         - (1
                                            / (1
                                               + (cpa.pct_impuesto / 100)))))),
                              0))
                           impuesto_canco
                   FROM hbgdwc.dwc_bok_t_canco_hotel h,
                        hbgdwc.dwc_oth_v_re_v_impuesto_sap vta,
                        hbgdwc.dwc_oth_v_re_v_impuesto_sap cpa
                  WHERE     seq_rec = r.grec_seq_rec
                        AND seq_reserva = r.seq_reserva
                        AND ind_tipo_imp_vta_fac = vta.ind_tipo_imp
                        AND cod_impuesto_vta_fac = vta.cod_impuesto
                        AND cod_clasif_vta_fac = vta.cod_clasif
                        AND cod_esquema_vta_fac = vta.cod_esquema
                        AND cod_empresa_vta_fac = vta.cod_emp_atlas
                        AND ind_tipo_imp_vta_fac = cpa.ind_tipo_imp
                        AND cod_impuesto_vta_fac = cpa.cod_impuesto
                        AND cod_clasif_vta_fac = cpa.cod_clasif
                        AND cod_esquema_vta_fac = cpa.cod_esquema
                        AND cod_empresa_vta_fac = cpa.cod_emp_atlas
                 UNION
                 SELECT SUM (
                           NVL (
                              Re_Pk_Admon.Re_Fu_Cambio_Divisa (
                                 rf.semp_cod_emp,
                                 Re_Pk_Admon.Cambio_Res,
                                 r.fec_creacion,
                                 r.gdiv_Cod_Divisa,
                                 'EUR',
                                 DECODE (
                                    ind_tipo_regimen_con,
                                    'E', imp_margen_canco
                                         * (1
                                            - (1
                                               / (1
                                                  + (vta.pct_impuesto
                                                     / 100)))),
                                    (imp_coste + imp_margen_canco)
                                    * (1
                                       - (1
                                          / (1 + (vta.pct_impuesto / 100))))
                                    - (imp_coste)
                                      * (1
                                         - (1
                                            / (1
                                               + (cpa.pct_impuesto / 100)))))),
                              0))
                           impuesto_canco
                   FROM hbgdwc.dwc_bok_t_canco_hotel_circuit h,
                        hbgdwc.dwc_oth_v_re_v_impuesto_sap vta,
                        hbgdwc.dwc_oth_v_re_v_impuesto_sap cpa
                  WHERE     seq_rec = r.grec_seq_rec
                        AND seq_reserva = r.seq_reserva
                        AND ind_tipo_imp_vta_fac = vta.ind_tipo_imp
                        AND cod_impuesto_vta_fac = vta.cod_impuesto
                        AND cod_clasif_vta_fac = vta.cod_clasif
                        AND cod_esquema_vta_fac = vta.cod_esquema
                        AND cod_empresa_vta_fac = vta.cod_emp_atlas
                        AND ind_tipo_imp_vta_fac = cpa.ind_tipo_imp
                        AND cod_impuesto_vta_fac = cpa.cod_impuesto
                        AND cod_clasif_vta_fac = cpa.cod_clasif
                        AND cod_esquema_vta_fac = cpa.cod_esquema
                        AND cod_empresa_vta_fac = cpa.cod_emp_atlas
                 UNION
                 SELECT SUM (
                           NVL (
                              Re_Pk_Admon.Re_Fu_Cambio_Divisa (
                                 rf.semp_cod_emp,
                                 Re_Pk_Admon.Cambio_Res,
                                 r.fec_creacion,
                                 r.gdiv_Cod_Divisa,
                                 'EUR',
                                 DECODE (
                                    ind_tipo_regimen_con,
                                    'E', imp_margen_canco
                                         * (1
                                            - (1
                                               / (1
                                                  + (vta.pct_impuesto
                                                     / 100)))),
                                    (imp_coste + imp_margen_canco)
                                    * (1
                                       - (1
                                          / (1 + (vta.pct_impuesto / 100))))
                                    - (imp_coste)
                                      * (1
                                         - (1
                                            / (1
                                               + (cpa.pct_impuesto / 100)))))),
                              0))
                           impuesto_canco
                   FROM hbgdwc.dwc_bok_t_canco_other h,
                        hbgdwc.dwc_oth_v_re_v_impuesto_sap vta,
                        hbgdwc.dwc_oth_v_re_v_impuesto_sap cpa
                  WHERE     seq_rec = r.grec_seq_rec
                        AND seq_reserva = r.seq_reserva
                        AND ind_tipo_imp_vta_fac = vta.ind_tipo_imp
                        AND cod_impuesto_vta_fac = vta.cod_impuesto
                        AND cod_clasif_vta_fac = vta.cod_clasif
                        AND cod_esquema_vta_fac = vta.cod_esquema
                        AND cod_empresa_vta_fac = vta.cod_emp_atlas
                        AND ind_tipo_imp_vta_fac = cpa.ind_tipo_imp
                        AND cod_impuesto_vta_fac = cpa.cod_impuesto
                        AND cod_clasif_vta_fac = cpa.cod_clasif
                        AND cod_esquema_vta_fac = cpa.cod_esquema
                        AND cod_empresa_vta_fac = cpa.cod_emp_atlas
                 UNION
                 SELECT SUM (
                           NVL (
                              Re_Pk_Admon.Re_Fu_Cambio_Divisa (
                                 rf.semp_cod_emp,
                                 Re_Pk_Admon.Cambio_Res,
                                 r.fec_creacion,
                                 r.gdiv_Cod_Divisa,
                                 'EUR',
                                 DECODE (
                                    ind_tipo_regimen_con,
                                    'E', imp_margen_canco
                                         * (1
                                            - (1
                                               / (1
                                                  + (vta.pct_impuesto
                                                     / 100)))),
                                    (imp_coste + imp_margen_canco)
                                    * (1
                                       - (1
                                          / (1 + (vta.pct_impuesto / 100))))
                                    - (imp_coste)
                                      * (1
                                         - (1
                                            / (1
                                               + (cpa.pct_impuesto / 100)))))),
                              0))
                           impuesto_canco
                   FROM hbgdwc.dwc_bok_t_canco_transfer h,
                        hbgdwc.dwc_oth_v_re_v_impuesto_sap vta,
                        hbgdwc.dwc_oth_v_re_v_impuesto_sap cpa
                  WHERE     seq_rec = r.grec_seq_rec
                        AND seq_reserva = r.seq_reserva
                        AND ind_tipo_imp_vta_fac = vta.ind_tipo_imp
                        AND cod_impuesto_vta_fac = vta.cod_impuesto
                        AND cod_clasif_vta_fac = vta.cod_clasif
                        AND cod_esquema_vta_fac = vta.cod_esquema
                        AND cod_empresa_vta_fac = vta.cod_emp_atlas
                        AND ind_tipo_imp_vta_fac = cpa.ind_tipo_imp
                        AND cod_impuesto_vta_fac = cpa.cod_impuesto
                        AND cod_clasif_vta_fac = cpa.cod_clasif
                        AND cod_esquema_vta_fac = cpa.cod_esquema
                        AND cod_empresa_vta_fac = cpa.cod_emp_atlas
                 UNION
                 SELECT SUM (
                           NVL (
                              Re_Pk_Admon.Re_Fu_Cambio_Divisa (
                                 rf.semp_cod_emp,
                                 Re_Pk_Admon.Cambio_Res,
                                 r.fec_creacion,
                                 r.gdiv_Cod_Divisa,
                                 'EUR',
                                 DECODE (
                                    ind_tipo_regimen_con,
                                    'E', imp_margen_canco
                                         * (1
                                            - (1
                                               / (1
                                                  + (vta.pct_impuesto
                                                     / 100)))),
                                    (imp_coste + imp_margen_canco)
                                    * (1
                                       - (1
                                          / (1 + (vta.pct_impuesto / 100))))
                                    - (imp_coste)
                                      * (1
                                         - (1
                                            / (1
                                               + (cpa.pct_impuesto / 100)))))),
                              0))
                           impuesto_canco
                   FROM hbgdwc.dwc_bok_t_canco_endowments h,
                        hbgdwc.dwc_oth_v_re_v_impuesto_sap vta,
                        hbgdwc.dwc_oth_v_re_v_impuesto_sap cpa
                  WHERE     seq_rec = r.grec_seq_rec
                        AND seq_reserva = r.seq_reserva
                        AND ind_tipo_imp_vta_fac = vta.ind_tipo_imp
                        AND cod_impuesto_vta_fac = vta.cod_impuesto
                        AND cod_clasif_vta_fac = vta.cod_clasif
                        AND cod_esquema_vta_fac = vta.cod_esquema
                        AND cod_empresa_vta_fac = vta.cod_emp_atlas
                        AND ind_tipo_imp_vta_fac = cpa.ind_tipo_imp
                        AND cod_impuesto_vta_fac = cpa.cod_impuesto
                        AND cod_clasif_vta_fac = cpa.cod_clasif
                        AND cod_esquema_vta_fac = cpa.cod_esquema
                        AND cod_empresa_vta_fac = cpa.cod_emp_atlas
                 UNION
                 SELECT SUM (
                           NVL (
                              Re_Pk_Admon.Re_Fu_Cambio_Divisa (
                                 rf.semp_cod_emp,
                                 Re_Pk_Admon.Cambio_Res,
                                 r.fec_creacion,
                                 r.gdiv_Cod_Divisa,
                                 'EUR',
                                 DECODE (
                                    ind_tipo_regimen_con,
                                    'E', imp_margen_canco
                                         * (1
                                            - (1
                                               / (1
                                                  + (vta.pct_impuesto
                                                     / 100)))),
                                    (imp_coste + imp_margen_canco)
                                    * (1
                                       - (1
                                          / (1 + (vta.pct_impuesto / 100))))
                                    - (imp_coste)
                                      * (1
                                         - (1
                                            / (1
                                               + (cpa.pct_impuesto / 100)))))),
                              0))
                           impuesto_canco
                   FROM hbgdwc.dwc_bok_t_canco_extra cce,
                        hbgdwc.dwc_oth_v_re_v_impuesto_sap vta,
                        hbgdwc.dwc_oth_v_re_v_impuesto_sap cpa
                  WHERE     seq_rec = r.grec_seq_rec
                        AND seq_reserva = r.seq_reserva
                        AND ind_tipo_imp_vta_fac = vta.ind_tipo_imp
                        AND cod_impuesto_vta_fac = vta.cod_impuesto
                        AND cod_clasif_vta_fac = vta.cod_clasif
                        AND cod_esquema_vta_fac = vta.cod_esquema
                        AND cod_empresa_vta_fac = vta.cod_emp_atlas
                        AND ind_tipo_imp_vta_fac = cpa.ind_tipo_imp
                        AND cod_impuesto_vta_fac = cpa.cod_impuesto
                        AND cod_clasif_vta_fac = cpa.cod_clasif
                        AND cod_esquema_vta_fac = cpa.cod_esquema
                        AND cod_empresa_vta_fac = cpa.cod_emp_atlas
                        AND (CCE.seq_rec, CCE.seq_reserva, CCE.ord_extra) NOT IN
                               (SELECT EXT.grec_seq_rec,
                                       EXT.rres_seq_reserva,
                                       EXT.ord_extra
                                  FROM hbgdwc.dwc_bok_t_extra EXT,
                                       hbgdwc.dwc_cli_dir_t_cd_discount_bond BON,
                                       hbgdwc.dwc_cli_dir_t_cd_campaign CAM
                                 WHERE EXT.num_bono = BON.num_bono
                                       AND EXT.cod_interface =
                                              BON.cod_interface
                                       AND BON.cod_campana = CAM.cod_campana
                                       AND BON.cod_interface =
                                              CAM.cod_interface
                                       AND CAM.ind_rentabilidad = 'N')))
           Tax_Cost_Transfer_pricing_EUR,
        (SELECT RI.APLICACION
           FROM hbgdwc.dwc_bok_t_booking_information ri
          WHERE     ri.seq_rec = r.grec_seq_rec
                AND ri.seq_reserva = r.seq_reserva
                AND tipo_op = 'A'
                AND ROWNUM = 1)
           Application,
        (SELECT DECODE (
                   DECODE (RV.ind_status,
                           'RR', 'R',
                           'CN', 'RL',
                           'RV', DECODE (FEC_CANCELACION, NULL, 'RS', 'C'),
                           IND_STATUS),
                   'RS', 'Reventa',
                   'C', 'Cancelada',
                   'RL', 'Liberada',
                   'R', 'Reventa')
                   IND_STATUS
           FROM hbgdwc.dwc_cry_t_cancellation_recovery_res_resale RV
          WHERE     R.GREC_SEQ_REC = RV.SEQ_REC
                AND R.SEQ_RESERVA = RV.SEQ_RESERVA
                AND RV.SEQ_REC = r.grec_seq_rec
                AND RV.SEQ_RESERVA = r.seq_reserva)
           canrec_status,
        (SELECT NVL (SUM (Re_Pk_Admon.Re_Fu_Cambio_Divisa (
                             rf.semp_cod_emp,
                             Re_Pk_Admon.Cambio_Res,
                             DECODE (I.IND_FEC_CAM_DIV,
                                     'E', r.fec_desde,
                                     r.fec_creacion),
                             r.COD_DIVISA_P,
                             r.gdiv_cod_divisa,
                             c.imp_com_age)),
                     0)
           FROM hbgdwc.dwc_bok_t_commission C
          WHERE     C.SEQ_REC = r.grec_seq_rec
                AND C.SEQ_RESERVA = r.seq_reserva)
           GSA_Commision,
        (SELECT NVL (SUM (Re_Pk_Admon.Re_Fu_Cambio_Divisa (
                             rf.semp_cod_emp,
                             Re_Pk_Admon.Cambio_Res,
                             DECODE (I.IND_FEC_CAM_DIV,
                                     'E', r.fec_desde,
                                     r.fec_creacion),
                             R.COD_DIVISA_P,
                             r.gdiv_cod_divisa,
                             c.imp_comision_imp)),
                     0)
           FROM hbgdwc.dwc_bok_t_commission C
          WHERE     C.SEQ_REC = r.grec_seq_rec
                AND C.SEQ_RESERVA = r.seq_reserva)
           Tax_GSA_Commision,
        (SELECT NVL (SUM (Re_Pk_Admon.Re_Fu_Cambio_Divisa (
                             rf.semp_cod_emp,
                             Re_Pk_Admon.Cambio_Res,
                             r.fec_creacion, --DECODE (I.IND_FEC_CAM_DIV,                                       'E', r.fec_desde,                                       r.fec_creacion),
                             R.COD_DIVISA_P,
                             'EUR',
                             c.imp_com_age)),
                     0)
           FROM hbgdwc.dwc_bok_t_commission C
          WHERE     C.SEQ_REC = r.grec_seq_rec
                AND C.SEQ_RESERVA = r.seq_reserva)
           GSA_EUR_Commision,
        (SELECT NVL (SUM (Re_Pk_Admon.Re_Fu_Cambio_Divisa (
                             rf.semp_cod_emp,
                             Re_Pk_Admon.Cambio_Res,
                             r.fec_creacion, --DECODE (I.IND_FEC_CAM_DIV,                                       'E', r.fec_desde,                                       r.fec_creacion),
                             R.COD_DIVISA_P,
                             'EUR',
                             c.imp_comision_imp)),
                     0)
                   IMPCOMAGE
           FROM hbgdwc.dwc_bok_t_commission C
          WHERE     C.SEQ_REC = r.grec_seq_rec
                AND C.SEQ_RESERVA = r.seq_reserva)
           Tax_GSA_EUR_Commision,
        -- Hotel Payment
        (SELECT NVL (SUM (Re_Pk_Admon.Re_Fu_Cambio_Divisa (
                             rf.semp_cod_emp,
                             Re_Pk_Admon.Cambio_Res,
                             DECODE (I.IND_FEC_CAM_DIV,
                                     'E', r.fec_desde,
                                     r.fec_creacion),
                             SDiv_Cod_Divisa,
                             r.gdiv_cod_divisa,
                             Re_Pk_Reser1.Re_Fu_Calcular_Noch_Imp (
                                Fec_Desde,
                                Fec_Hasta,
                                NRO_UNIDADES,
                                NRO_PAX,
                                IND_TIPO_UNID,
                                IND_P_S,
                                IMP_UNITARIO))),
                     0)
           FROM hbgdwc.dwc_bok_t_cost CST, hbgdwc.dwc_oth_v_re_v_impuesto_sap IMP
          WHERE     cst.grec_seq_rec = r.grec_seq_rec
                AND cst.rres_seq_reserva = r.seq_reserva
                AND cst.ind_tipo_registro = 'CA'
                AND cst.ind_facturable = 'S'
                AND cst.ind_contra_apunte = 'N'
                AND cst.rvp_ind_tipo_imp = imp.ind_tipo_imp(+)
                AND cst.rvp_cod_impuesto = imp.cod_impuesto(+)
                AND cst.rvp_cod_esquema = imp.cod_esquema(+)
                AND cst.rvp_cod_clasif = imp.cod_clasif(+)
                AND cst.rvp_cod_empresa = imp.cod_emp_atlas(+))
           Agency_commision_hotel_payment,
        (SELECT NVL (SUM (Re_Pk_Admon.Re_Fu_Cambio_Divisa (
                             rf.semp_cod_emp,
                             Re_Pk_Admon.Cambio_Res,
                             DECODE (I.IND_FEC_CAM_DIV,
                                     'E', r.fec_desde,
                                     r.fec_creacion),
                             SDiv_Cod_Divisa,
                             r.gdiv_cod_divisa,
                             Re_Pk_Reser1.Re_Fu_Calcular_Noch_Imp (
                                Fec_Desde,
                                Fec_Hasta,
                                NRO_UNIDADES,
                                NRO_PAX,
                                IND_TIPO_UNID,
                                IND_P_S,
                                Re_Pk_Admon.RE_FU_REDONDEO (
                                   IMP_UNITARIO
                                   - (IMP_UNITARIO
                                      / (1 + (pct_impuesto / 100))),
                                   r.gdiv_cod_divisa)))),
                     0)
           FROM hbgdwc.dwc_bok_t_cost CST, hbgdwc.dwc_oth_v_re_v_impuesto_sap IMP
          WHERE     cst.grec_seq_rec = r.grec_seq_rec
                AND cst.rres_seq_reserva = r.seq_reserva
                AND cst.ind_tipo_registro = 'CA'
                AND cst.ind_facturable = 'S'
                AND cst.ind_contra_apunte = 'N'
                AND cst.rvp_ind_tipo_imp = imp.ind_tipo_imp(+)
                AND cst.rvp_cod_impuesto = imp.cod_impuesto(+)
                AND cst.rvp_cod_esquema = imp.cod_esquema(+)
                AND cst.rvp_cod_clasif = imp.cod_clasif(+)
                AND cst.rvp_cod_empresa = imp.cod_emp_atlas(+))
           Tax_Agency_commision_hotel_pay,
        (SELECT NVL (SUM (Re_Pk_Admon.Re_Fu_Cambio_Divisa (
                             rf.semp_cod_emp,
                             Re_Pk_Admon.Cambio_Res,
                             DECODE (I.IND_FEC_CAM_DIV,
                                     'E', r.fec_desde,
                                     r.fec_creacion),
                             SDiv_Cod_Divisa,
                             r.gdiv_cod_divisa,
                             Re_Pk_Reser1.Re_Fu_Calcular_Noch_Imp (
                                Fec_Desde,
                                Fec_Hasta,
                                NRO_UNIDADES,
                                NRO_PAX,
                                IND_TIPO_UNID,
                                IND_P_S,
                                IMP_UNITARIO))),
                     0)
           FROM hbgdwc.dwc_bok_t_cost CST, hbgdwc.dwc_oth_v_re_v_impuesto_sap IMP
          WHERE     cst.grec_seq_rec = r.grec_seq_rec
                AND cst.rres_seq_reserva = r.seq_reserva
                AND cst.ind_tipo_registro = 'OC'
                AND cst.ind_facturable = 'S'
                AND cst.ind_contra_apunte = 'N'
                AND cst.rvp_ind_tipo_imp = imp.ind_tipo_imp(+)
                AND cst.rvp_cod_impuesto = imp.cod_impuesto(+)
                AND cst.rvp_cod_esquema = imp.cod_esquema(+)
                AND cst.rvp_cod_clasif = imp.cod_clasif(+)
                AND cst.rvp_cod_empresa = imp.cod_emp_atlas(+))
           Fix_override_hotel_payment,
        (SELECT NVL (SUM (Re_Pk_Admon.Re_Fu_Cambio_Divisa (
                             rf.semp_cod_emp,
                             Re_Pk_Admon.Cambio_Res,
                             DECODE (I.IND_FEC_CAM_DIV,
                                     'E', r.fec_desde,
                                     r.fec_creacion),
                             SDiv_Cod_Divisa,
                             r.gdiv_cod_divisa,
                             Re_Pk_Reser1.Re_Fu_Calcular_Noch_Imp (
                                Fec_Desde,
                                Fec_Hasta,
                                NRO_UNIDADES,
                                NRO_PAX,
                                IND_TIPO_UNID,
                                IND_P_S,
                                Re_Pk_Admon.RE_FU_REDONDEO (
                                   IMP_UNITARIO
                                   - (IMP_UNITARIO
                                      / (1 + (pct_impuesto / 100))),
                                   r.gdiv_cod_divisa)))),
                     0)
           FROM hbgdwc.dwc_bok_t_cost CST, hbgdwc.dwc_oth_v_re_v_impuesto_sap IMP
          WHERE     cst.grec_seq_rec = r.grec_seq_rec
                AND cst.rres_seq_reserva = r.seq_reserva
                AND cst.ind_tipo_registro = 'OC'
                AND cst.ind_facturable = 'S'
                AND cst.ind_contra_apunte = 'N'
                AND cst.rvp_ind_tipo_imp = imp.ind_tipo_imp(+)
                AND cst.rvp_cod_impuesto = imp.cod_impuesto(+)
                AND cst.rvp_cod_esquema = imp.cod_esquema(+)
                AND cst.rvp_cod_clasif = imp.cod_clasif(+)
                AND cst.rvp_cod_empresa = imp.cod_emp_atlas(+))
           Tax_Fix_override_hotel_pay,
        (SELECT NVL (SUM (Re_Pk_Admon.Re_Fu_Cambio_Divisa (
                             rf.semp_cod_emp,
                             Re_Pk_Admon.Cambio_Res,
                             DECODE (I.IND_FEC_CAM_DIV,
                                     'E', r.fec_desde,
                                     r.fec_creacion),
                             SDiv_Cod_Divisa,
                             r.gdiv_cod_divisa,
                             Re_Pk_Reser1.Re_Fu_Calcular_Noch_Imp (
                                Fec_Desde,
                                Fec_Hasta,
                                NRO_UNIDADES,
                                NRO_PAX,
                                IND_TIPO_UNID,
                                IND_P_S,
                                IMP_UNITARIO))),
                     0)
           FROM hbgdwc.dwc_bok_t_cost CST, hbgdwc.dwc_oth_v_re_v_impuesto_sap IMP
          WHERE     cst.grec_seq_rec = r.grec_seq_rec
                AND cst.rres_seq_reserva = r.seq_reserva
                AND cst.ind_tipo_registro = 'OV'
                AND cst.ind_facturable = 'S'
                AND cst.ind_contra_apunte = 'N'
                AND cst.rvp_ind_tipo_imp = imp.ind_tipo_imp(+)
                AND cst.rvp_cod_impuesto = imp.cod_impuesto(+)
                AND cst.rvp_cod_esquema = imp.cod_esquema(+)
                AND cst.rvp_cod_clasif = imp.cod_clasif(+)
                AND cst.rvp_cod_empresa = imp.cod_emp_atlas(+))
           Var_override_hotel_payment,
        (SELECT NVL (SUM (Re_Pk_Admon.Re_Fu_Cambio_Divisa (
                             rf.semp_cod_emp,
                             Re_Pk_Admon.Cambio_Res,
                             DECODE (I.IND_FEC_CAM_DIV,
                                     'E', r.fec_desde,
                                     r.fec_creacion),
                             SDiv_Cod_Divisa,
                             r.gdiv_cod_divisa,
                             Re_Pk_Reser1.Re_Fu_Calcular_Noch_Imp (
                                Fec_Desde,
                                Fec_Hasta,
                                NRO_UNIDADES,
                                NRO_PAX,
                                IND_TIPO_UNID,
                                IND_P_S,
                                Re_Pk_Admon.RE_FU_REDONDEO (
                                   IMP_UNITARIO
                                   - (IMP_UNITARIO
                                      / (1 + (pct_impuesto / 100))),
                                   r.gdiv_cod_divisa)))),
                     0)
           FROM hbgdwc.dwc_bok_t_cost CST, hbgdwc.dwc_oth_v_re_v_impuesto_sap IMP
          WHERE     cst.grec_seq_rec = r.grec_seq_rec
                AND cst.rres_seq_reserva = r.seq_reserva
                AND cst.ind_tipo_registro = 'OV'
                AND cst.ind_facturable = 'S'
                AND cst.ind_contra_apunte = 'N'
                AND cst.rvp_ind_tipo_imp = imp.ind_tipo_imp(+)
                AND cst.rvp_cod_impuesto = imp.cod_impuesto(+)
                AND cst.rvp_cod_esquema = imp.cod_esquema(+)
                AND cst.rvp_cod_clasif = imp.cod_clasif(+)
                AND cst.rvp_cod_empresa = imp.cod_emp_atlas(+))
           Tax_Var_override_hotel_pay,
        (SELECT NVL (SUM (Re_Pk_Admon.Re_Fu_Cambio_Divisa (
                             rf.semp_cod_emp,
                             Re_Pk_Admon.Cambio_Res,
                             DECODE (I.IND_FEC_CAM_DIV,
                                     'E', r.fec_desde,
                                     r.fec_creacion),
                             SDiv_Cod_Divisa,
                             r.gdiv_cod_divisa,
                             Re_Pk_Reser1.Re_Fu_Calcular_Noch_Imp (
                                Fec_Desde,
                                Fec_Hasta,
                                NRO_UNIDADES,
                                NRO_PAX,
                                IND_TIPO_UNID,
                                IND_P_S,
                                IMP_UNITARIO))),
                     0)
           FROM hbgdwc.dwc_bok_t_cost CST, hbgdwc.dwc_oth_v_re_v_impuesto_sap IMP
          WHERE     cst.grec_seq_rec = r.grec_seq_rec
                AND cst.rres_seq_reserva = r.seq_reserva
                AND cst.ind_tipo_registro = 'CH'
                AND cst.ind_facturable = 'S'
                AND cst.ind_contra_apunte = 'N'
                AND cst.rvp_ind_tipo_imp = imp.ind_tipo_imp(+)
                AND cst.rvp_cod_impuesto = imp.cod_impuesto(+)
                AND cst.rvp_cod_esquema = imp.cod_esquema(+)
                AND cst.rvp_cod_clasif = imp.cod_clasif(+)
                AND cst.rvp_cod_empresa = imp.cod_emp_atlas(+))
           Hotel_commision_hotel_payment,
        (SELECT NVL (SUM (Re_Pk_Admon.Re_Fu_Cambio_Divisa (
                             rf.semp_cod_emp,
                             Re_Pk_Admon.Cambio_Res,
                             DECODE (I.IND_FEC_CAM_DIV,
                                     'E', r.fec_desde,
                                     r.fec_creacion),
                             SDiv_Cod_Divisa,
                             r.gdiv_cod_divisa,
                             Re_Pk_Reser1.Re_Fu_Calcular_Noch_Imp (
                                Fec_Desde,
                                Fec_Hasta,
                                NRO_UNIDADES,
                                NRO_PAX,
                                IND_TIPO_UNID,
                                IND_P_S,
                                Re_Pk_Admon.RE_FU_REDONDEO (
                                   IMP_UNITARIO
                                   - (IMP_UNITARIO
                                      / (1 + (pct_impuesto / 100))),
                                   r.gdiv_cod_divisa)))),
                     0)
           FROM hbgdwc.dwc_bok_t_cost CST, hbgdwc.dwc_oth_v_re_v_impuesto_sap IMP
          WHERE     cst.grec_seq_rec = r.grec_seq_rec
                AND cst.rres_seq_reserva = r.seq_reserva
                AND cst.ind_tipo_registro = 'CH'
                AND cst.ind_facturable = 'S'
                AND cst.ind_contra_apunte = 'N'
                AND cst.rvp_ind_tipo_imp = imp.ind_tipo_imp(+)
                AND cst.rvp_cod_impuesto = imp.cod_impuesto(+)
                AND cst.rvp_cod_esquema = imp.cod_esquema(+)
                AND cst.rvp_cod_clasif = imp.cod_clasif(+)
                AND cst.rvp_cod_empresa = imp.cod_emp_atlas(+))
           Tax_Hotel_commision_Hotel_pay,
        (SELECT NVL (SUM (Re_Pk_Admon.Re_Fu_Cambio_Divisa (
                             rf.semp_cod_emp,
                             Re_Pk_Admon.Cambio_Res,
                             DECODE (I.IND_FEC_CAM_DIV,
                                     'E', r.fec_desde,
                                     r.fec_creacion),
                             SDiv_Cod_Divisa,
                             r.gdiv_cod_divisa,
                             Re_Pk_Reser1.Re_Fu_Calcular_Noch_Imp (
                                Fec_Desde,
                                Fec_Hasta,
                                NRO_UNIDADES,
                                NRO_PAX,
                                IND_TIPO_UNID,
                                IND_P_S,
                                IMP_UNITARIO))),
                     0)
           FROM hbgdwc.dwc_bok_t_cost CST, hbgdwc.dwc_oth_v_re_v_impuesto_sap IMP
          WHERE     cst.grec_seq_rec = r.grec_seq_rec
                AND cst.rres_seq_reserva = r.seq_reserva
                AND cst.ind_tipo_registro = 'L'
                AND cst.ind_facturable = 'S'
                AND cst.ind_contra_apunte = 'N'
                AND cst.rvp_ind_tipo_imp = imp.ind_tipo_imp(+)
                AND cst.rvp_cod_impuesto = imp.cod_impuesto(+)
                AND cst.rvp_cod_esquema = imp.cod_esquema(+)
                AND cst.rvp_cod_clasif = imp.cod_clasif(+)
                AND cst.rvp_cod_empresa = imp.cod_emp_atlas(+))
           Marketing_contribution,
        (SELECT NVL (SUM (Re_Pk_Admon.Re_Fu_Cambio_Divisa (
                             rf.semp_cod_emp,
                             Re_Pk_Admon.Cambio_Res,
                             DECODE (I.IND_FEC_CAM_DIV,
                                     'E', r.fec_desde,
                                     r.fec_creacion),
                             SDiv_Cod_Divisa,
                             r.gdiv_cod_divisa,
                             Re_Pk_Reser1.Re_Fu_Calcular_Noch_Imp (
                                Fec_Desde,
                                Fec_Hasta,
                                NRO_UNIDADES,
                                NRO_PAX,
                                IND_TIPO_UNID,
                                IND_P_S,
                                Re_Pk_Admon.RE_FU_REDONDEO (
                                   IMP_UNITARIO
                                   - (IMP_UNITARIO
                                      / (1 + (pct_impuesto / 100))),
                                   r.gdiv_cod_divisa)))),
                     0)
           FROM hbgdwc.dwc_bok_t_cost CST, hbgdwc.dwc_oth_v_re_v_impuesto_sap IMP
          WHERE     cst.grec_seq_rec = r.grec_seq_rec
                AND cst.rres_seq_reserva = r.seq_reserva
                AND cst.ind_tipo_registro = 'L'
                AND cst.ind_facturable = 'S'
                AND cst.ind_contra_apunte = 'N'
                AND cst.rvp_ind_tipo_imp = imp.ind_tipo_imp(+)
                AND cst.rvp_cod_impuesto = imp.cod_impuesto(+)
                AND cst.rvp_cod_esquema = imp.cod_esquema(+)
                AND cst.rvp_cod_clasif = imp.cod_clasif(+)
                AND cst.rvp_cod_empresa = imp.cod_emp_atlas(+))
           Tax_marketing_contribution,
        (SELECT NVL (SUM (Re_Pk_Admon.Re_Fu_Cambio_Divisa (
                             rf.semp_cod_emp,
                             Re_Pk_Admon.Cambio_Res,
                             DECODE (I.IND_FEC_CAM_DIV,
                                     'E', r.fec_desde,
                                     r.fec_creacion),
                             SDiv_Cod_Divisa,
                             r.gdiv_cod_divisa,
                             Re_Pk_Reser1.Re_Fu_Calcular_Noch_Imp (
                                Fec_Desde,
                                Fec_Hasta,
                                NRO_UNIDADES,
                                NRO_PAX,
                                IND_TIPO_UNID,
                                IND_P_S,
                                IMP_UNITARIO))),
                     0)
           FROM hbgdwc.dwc_bok_t_cost CST, hbgdwc.dwc_oth_v_re_v_impuesto_sap IMP
          WHERE     cst.grec_seq_rec = r.grec_seq_rec
                AND cst.rres_seq_reserva = r.seq_reserva
                AND cst.ind_tipo_registro = 'K'
                AND cst.ind_facturable = 'S'
                AND cst.ind_contra_apunte = 'N'
                AND cst.rvp_ind_tipo_imp = imp.ind_tipo_imp(+)
                AND cst.rvp_cod_impuesto = imp.cod_impuesto(+)
                AND cst.rvp_cod_esquema = imp.cod_esquema(+)
                AND cst.rvp_cod_clasif = imp.cod_clasif(+)
                AND cst.rvp_cod_empresa = imp.cod_emp_atlas(+))
           bank_expenses,
        (SELECT NVL (SUM (Re_Pk_Admon.Re_Fu_Cambio_Divisa (
                             rf.semp_cod_emp,
                             Re_Pk_Admon.Cambio_Res,
                             DECODE (I.IND_FEC_CAM_DIV,
                                     'E', r.fec_desde,
                                     r.fec_creacion),
                             SDiv_Cod_Divisa,
                             r.gdiv_cod_divisa,
                             Re_Pk_Reser1.Re_Fu_Calcular_Noch_Imp (
                                Fec_Desde,
                                Fec_Hasta,
                                NRO_UNIDADES,
                                NRO_PAX,
                                IND_TIPO_UNID,
                                IND_P_S,
                                Re_Pk_Admon.RE_FU_REDONDEO (
                                   IMP_UNITARIO
                                   - (IMP_UNITARIO
                                      / (1 + (pct_impuesto / 100))),
                                   r.gdiv_cod_divisa)))),
                     0)
           FROM hbgdwc.dwc_bok_t_cost CST, hbgdwc.dwc_oth_v_re_v_impuesto_sap IMP
          WHERE     cst.grec_seq_rec = r.grec_seq_rec
                AND cst.rres_seq_reserva = r.seq_reserva
                AND cst.ind_tipo_registro = 'K'
                AND cst.ind_facturable = 'S'
                AND cst.ind_contra_apunte = 'N'
                AND cst.rvp_ind_tipo_imp = imp.ind_tipo_imp(+)
                AND cst.rvp_cod_impuesto = imp.cod_impuesto(+)
                AND cst.rvp_cod_esquema = imp.cod_esquema(+)
                AND cst.rvp_cod_clasif = imp.cod_clasif(+)
                AND cst.rvp_cod_empresa = imp.cod_emp_atlas(+))
           Tax_Bank_expenses,
        (SELECT NVL (SUM (Re_Pk_Admon.Re_Fu_Cambio_Divisa (
                             rf.semp_cod_emp,
                             Re_Pk_Admon.Cambio_Res,
                             DECODE (I.IND_FEC_CAM_DIV,
                                     'E', r.fec_desde,
                                     r.fec_creacion),
                             SDiv_Cod_Divisa,
                             r.gdiv_cod_divisa,
                             Re_Pk_Reser1.Re_Fu_Calcular_Noch_Imp (
                                Fec_Desde,
                                Fec_Hasta,
                                NRO_UNIDADES,
                                NRO_PAX,
                                IND_TIPO_UNID,
                                IND_P_S,
                                IMP_UNITARIO))),
                     0)
           FROM hbgdwc.dwc_bok_t_cost CST, hbgdwc.dwc_oth_v_re_v_impuesto_sap IMP
          WHERE     cst.grec_seq_rec = r.grec_seq_rec
                AND cst.rres_seq_reserva = r.seq_reserva
                AND cst.ind_tipo_registro = 'PF'
                AND cst.ind_facturable = 'S'
                AND cst.ind_contra_apunte = 'N'
                AND cst.rvp_ind_tipo_imp = imp.ind_tipo_imp(+)
                AND cst.rvp_cod_impuesto = imp.cod_impuesto(+)
                AND cst.rvp_cod_esquema = imp.cod_esquema(+)
                AND cst.rvp_cod_clasif = imp.cod_clasif(+)
                AND cst.rvp_cod_empresa = imp.cod_emp_atlas(+))
           Platform_Fee,
        (SELECT NVL (SUM (Re_Pk_Admon.Re_Fu_Cambio_Divisa (
                             rf.semp_cod_emp,
                             Re_Pk_Admon.Cambio_Res,
                             DECODE (I.IND_FEC_CAM_DIV,
                                     'E', r.fec_desde,
                                     r.fec_creacion),
                             SDiv_Cod_Divisa,
                             r.gdiv_cod_divisa,
                             Re_Pk_Reser1.Re_Fu_Calcular_Noch_Imp (
                                Fec_Desde,
                                Fec_Hasta,
                                NRO_UNIDADES,
                                NRO_PAX,
                                IND_TIPO_UNID,
                                IND_P_S,
                                Re_Pk_Admon.RE_FU_REDONDEO (
                                   IMP_UNITARIO
                                   - (IMP_UNITARIO
                                      / (1 + (pct_impuesto / 100))),
                                   r.gdiv_cod_divisa)))),
                     0)
           FROM hbgdwc.dwc_bok_t_cost CST, hbgdwc.dwc_oth_v_re_v_impuesto_sap IMP
          WHERE     cst.grec_seq_rec = r.grec_seq_rec
                AND cst.rres_seq_reserva = r.seq_reserva
                AND cst.ind_tipo_registro = 'PF'
                AND cst.ind_facturable = 'S'
                AND cst.ind_contra_apunte = 'N'
                AND cst.rvp_ind_tipo_imp = imp.ind_tipo_imp(+)
                AND cst.rvp_cod_impuesto = imp.cod_impuesto(+)
                AND cst.rvp_cod_esquema = imp.cod_esquema(+)
                AND cst.rvp_cod_clasif = imp.cod_clasif(+)
                AND cst.rvp_cod_empresa = imp.cod_emp_atlas(+))
           Tax_Platform_fee,
        (SELECT NVL (SUM (Re_Pk_Admon.Re_Fu_Cambio_Divisa (
                             rf.semp_cod_emp,
                             Re_Pk_Admon.Cambio_Res,
                             DECODE (I.IND_FEC_CAM_DIV,
                                     'E', r.fec_desde,
                                     r.fec_creacion),
                             SDiv_Cod_Divisa,
                             r.gdiv_cod_divisa,
                             Re_Pk_Reser1.Re_Fu_Calcular_Noch_Imp (
                                Fec_Desde,
                                Fec_Hasta,
                                NRO_UNIDADES,
                                NRO_PAX,
                                IND_TIPO_UNID,
                                IND_P_S,
                                IMP_UNITARIO))),
                     0)
           FROM hbgdwc.dwc_bok_t_cost CST, hbgdwc.dwc_oth_v_re_v_impuesto_sap IMP
          WHERE     cst.grec_seq_rec = r.grec_seq_rec
                AND cst.rres_seq_reserva = r.seq_reserva
                AND cst.ind_tipo_registro = 'CT'
                AND cst.ind_facturable = 'S'
                AND cst.ind_contra_apunte = 'N'
                AND cst.rvp_ind_tipo_imp = imp.ind_tipo_imp(+)
                AND cst.rvp_cod_impuesto = imp.cod_impuesto(+)
                AND cst.rvp_cod_esquema = imp.cod_esquema(+)
                AND cst.rvp_cod_clasif = imp.cod_clasif(+)
                AND cst.rvp_cod_empresa = imp.cod_emp_atlas(+))
           credit_card_fee,
        (SELECT NVL (SUM (Re_Pk_Admon.Re_Fu_Cambio_Divisa (
                             rf.semp_cod_emp,
                             Re_Pk_Admon.Cambio_Res,
                             DECODE (I.IND_FEC_CAM_DIV,
                                     'E', r.fec_desde,
                                     r.fec_creacion),
                             SDiv_Cod_Divisa,
                             r.gdiv_cod_divisa,
                             Re_Pk_Reser1.Re_Fu_Calcular_Noch_Imp (
                                Fec_Desde,
                                Fec_Hasta,
                                NRO_UNIDADES,
                                NRO_PAX,
                                IND_TIPO_UNID,
                                IND_P_S,
                                Re_Pk_Admon.RE_FU_REDONDEO (
                                   IMP_UNITARIO
                                   - (IMP_UNITARIO
                                      / (1 + (pct_impuesto / 100))),
                                   r.gdiv_cod_divisa)))),
                     0)
           FROM hbgdwc.dwc_bok_t_cost CST, hbgdwc.dwc_oth_v_re_v_impuesto_sap IMP
          WHERE     cst.grec_seq_rec = r.grec_seq_rec
                AND cst.rres_seq_reserva = r.seq_reserva
                AND cst.ind_tipo_registro = 'CT'
                AND cst.ind_facturable = 'S'
                AND cst.ind_contra_apunte = 'N'
                AND cst.rvp_ind_tipo_imp = imp.ind_tipo_imp(+)
                AND cst.rvp_cod_impuesto = imp.cod_impuesto(+)
                AND cst.rvp_cod_esquema = imp.cod_esquema(+)
                AND cst.rvp_cod_clasif = imp.cod_clasif(+)
                AND cst.rvp_cod_empresa = imp.cod_emp_atlas(+))
           Tax_credit_card_fee,
        (SELECT NVL (SUM (Re_Pk_Admon.Re_Fu_Cambio_Divisa (
                             rf.semp_cod_emp,
                             Re_Pk_Admon.Cambio_Res,
                             DECODE (I.IND_FEC_CAM_DIV,
                                     'E', r.fec_desde,
                                     r.fec_creacion),
                             SDiv_Cod_Divisa,
                             r.gdiv_cod_divisa,
                             Re_Pk_Reser1.Re_Fu_Calcular_Noch_Imp (
                                Fec_Desde,
                                Fec_Hasta,
                                NRO_UNIDADES,
                                NRO_PAX,
                                IND_TIPO_UNID,
                                IND_P_S,
                                IMP_UNITARIO))),
                     0)
           FROM hbgdwc.dwc_bok_t_cost CST, hbgdwc.dwc_oth_v_re_v_impuesto_sap IMP
          WHERE     cst.grec_seq_rec = r.grec_seq_rec
                AND cst.rres_seq_reserva = r.seq_reserva
                AND cst.ind_tipo_registro = 'SC'
                AND cst.ind_facturable = 'S'
                AND cst.ind_contra_apunte = 'N'
                AND cst.rvp_ind_tipo_imp = imp.ind_tipo_imp(+)
                AND cst.rvp_cod_impuesto = imp.cod_impuesto(+)
                AND cst.rvp_cod_esquema = imp.cod_esquema(+)
                AND cst.rvp_cod_clasif = imp.cod_clasif(+)
                AND cst.rvp_cod_empresa = imp.cod_emp_atlas(+))
           Withholding,
        (SELECT NVL (SUM (Re_Pk_Admon.Re_Fu_Cambio_Divisa (
                             rf.semp_cod_emp,
                             Re_Pk_Admon.Cambio_Res,
                             DECODE (I.IND_FEC_CAM_DIV,
                                     'E', r.fec_desde,
                                     r.fec_creacion),
                             SDiv_Cod_Divisa,
                             r.gdiv_cod_divisa,
                             Re_Pk_Reser1.Re_Fu_Calcular_Noch_Imp (
                                Fec_Desde,
                                Fec_Hasta,
                                NRO_UNIDADES,
                                NRO_PAX,
                                IND_TIPO_UNID,
                                IND_P_S,
                                Re_Pk_Admon.RE_FU_REDONDEO (
                                   IMP_UNITARIO
                                   - (IMP_UNITARIO
                                      / (1 + (pct_impuesto / 100))),
                                   r.gdiv_cod_divisa)))),
                     0)
           FROM hbgdwc.dwc_bok_t_cost CST, hbgdwc.dwc_oth_v_re_v_impuesto_sap IMP
          WHERE     cst.grec_seq_rec = r.grec_seq_rec
                AND cst.rres_seq_reserva = r.seq_reserva
                AND cst.ind_tipo_registro = 'SC'
                AND cst.ind_facturable = 'S'
                AND cst.ind_contra_apunte = 'N'
                AND cst.rvp_ind_tipo_imp = imp.ind_tipo_imp(+)
                AND cst.rvp_cod_impuesto = imp.cod_impuesto(+)
                AND cst.rvp_cod_esquema = imp.cod_esquema(+)
                AND cst.rvp_cod_clasif = imp.cod_clasif(+)
                AND cst.rvp_cod_empresa = imp.cod_emp_atlas(+))
           Tax_withholding,
        (SELECT NVL (SUM (Re_Pk_Admon.Re_Fu_Cambio_Divisa (
                             rf.semp_cod_emp,
                             Re_Pk_Admon.Cambio_Res,
                             DECODE (I.IND_FEC_CAM_DIV,
                                     'E', r.fec_desde,
                                     r.fec_creacion),
                             SDiv_Cod_Divisa,
                             r.gdiv_cod_divisa,
                             Re_Pk_Reser1.Re_Fu_Calcular_Noch_Imp (
                                Fec_Desde,
                                Fec_Hasta,
                                NRO_UNIDADES,
                                NRO_PAX,
                                IND_TIPO_UNID,
                                IND_P_S,
                                IMP_UNITARIO))),
                     0)
           FROM hbgdwc.dwc_bok_t_cost CST, hbgdwc.dwc_oth_v_re_v_impuesto_sap IMP
          WHERE     cst.grec_seq_rec = r.grec_seq_rec
                AND cst.rres_seq_reserva = r.seq_reserva
                AND cst.ind_tipo_registro = 'LL'
                AND cst.ind_facturable = 'S'
                AND cst.ind_contra_apunte = 'N'
                AND cst.rvp_ind_tipo_imp = imp.ind_tipo_imp(+)
                AND cst.rvp_cod_impuesto = imp.cod_impuesto(+)
                AND cst.rvp_cod_esquema = imp.cod_esquema(+)
                AND cst.rvp_cod_clasif = imp.cod_clasif(+)
                AND cst.rvp_cod_empresa = imp.cod_emp_atlas(+))
           Local_Levy,
        (SELECT NVL (SUM (Re_Pk_Admon.Re_Fu_Cambio_Divisa (
                             rf.semp_cod_emp,
                             Re_Pk_Admon.Cambio_Res,
                             DECODE (I.IND_FEC_CAM_DIV,
                                     'E', r.fec_desde,
                                     r.fec_creacion),
                             SDiv_Cod_Divisa,
                             r.gdiv_cod_divisa,
                             Re_Pk_Reser1.Re_Fu_Calcular_Noch_Imp (
                                Fec_Desde,
                                Fec_Hasta,
                                NRO_UNIDADES,
                                NRO_PAX,
                                IND_TIPO_UNID,
                                IND_P_S,
                                Re_Pk_Admon.RE_FU_REDONDEO (
                                   IMP_UNITARIO
                                   - (IMP_UNITARIO
                                      / (1 + (pct_impuesto / 100))),
                                   r.gdiv_cod_divisa)))),
                     0)
           FROM hbgdwc.dwc_bok_t_cost CST, hbgdwc.dwc_oth_v_re_v_impuesto_sap IMP
          WHERE     cst.grec_seq_rec = r.grec_seq_rec
                AND cst.rres_seq_reserva = r.seq_reserva
                AND cst.ind_tipo_registro = 'LL'
                AND cst.ind_facturable = 'S'
                AND cst.ind_contra_apunte = 'N'
                AND cst.rvp_ind_tipo_imp = imp.ind_tipo_imp(+)
                AND cst.rvp_cod_impuesto = imp.cod_impuesto(+)
                AND cst.rvp_cod_esquema = imp.cod_esquema(+)
                AND cst.rvp_cod_clasif = imp.cod_clasif(+)
                AND cst.rvp_cod_empresa = imp.cod_emp_atlas(+))
           Tax_Local_Levy,
        (SELECT NVL (SUM (Re_Pk_Admon.Re_Fu_Cambio_Divisa (
                             rf.semp_cod_emp,
                             Re_Pk_Admon.Cambio_Res,
                             DECODE (I.IND_FEC_CAM_DIV,
                                     'E', r.fec_desde,
                                     r.fec_creacion),
                             SDiv_Cod_Divisa,
                             r.gdiv_cod_divisa,
                             Re_Pk_Reser1.Re_Fu_Calcular_Noch_Imp (
                                Fec_Desde,
                                Fec_Hasta,
                                NRO_UNIDADES,
                                NRO_PAX,
                                IND_TIPO_UNID,
                                IND_P_S,
                                IMP_UNITARIO))),
                     0)
           FROM hbgdwc.dwc_bok_t_cost CST, hbgdwc.dwc_oth_v_re_v_impuesto_sap IMP
          WHERE     cst.grec_seq_rec = r.grec_seq_rec
                AND cst.rres_seq_reserva = r.seq_reserva
                AND cst.ind_tipo_registro = 'I'
                AND cst.ind_facturable = 'S'
                AND cst.ind_contra_apunte = 'N'
                AND cst.rvp_ind_tipo_imp = imp.ind_tipo_imp(+)
                AND cst.rvp_cod_impuesto = imp.cod_impuesto(+)
                AND cst.rvp_cod_esquema = imp.cod_esquema(+)
                AND cst.rvp_cod_clasif = imp.cod_clasif(+)
                AND cst.rvp_cod_empresa = imp.cod_emp_atlas(+))
           Partner_Third_commision,
        (SELECT NVL (SUM (Re_Pk_Admon.Re_Fu_Cambio_Divisa (
                             rf.semp_cod_emp,
                             Re_Pk_Admon.Cambio_Res,
                             DECODE (I.IND_FEC_CAM_DIV,
                                     'E', r.fec_desde,
                                     r.fec_creacion),
                             SDiv_Cod_Divisa,
                             r.gdiv_cod_divisa,
                             Re_Pk_Reser1.Re_Fu_Calcular_Noch_Imp (
                                Fec_Desde,
                                Fec_Hasta,
                                NRO_UNIDADES,
                                NRO_PAX,
                                IND_TIPO_UNID,
                                IND_P_S,
                                Re_Pk_Admon.RE_FU_REDONDEO (
                                   IMP_UNITARIO
                                   - (IMP_UNITARIO
                                      / (1 + (pct_impuesto / 100))),
                                   r.gdiv_cod_divisa)))),
                     0)
           FROM hbgdwc.dwc_bok_t_cost CST, hbgdwc.dwc_oth_v_re_v_impuesto_sap IMP
          WHERE     cst.grec_seq_rec = r.grec_seq_rec
                AND cst.rres_seq_reserva = r.seq_reserva
                AND cst.ind_tipo_registro = 'I'
                AND cst.ind_facturable = 'S'
                AND cst.ind_contra_apunte = 'N'
                AND cst.rvp_ind_tipo_imp = imp.ind_tipo_imp(+)
                AND cst.rvp_cod_impuesto = imp.cod_impuesto(+)
                AND cst.rvp_cod_esquema = imp.cod_esquema(+)
                AND cst.rvp_cod_clasif = imp.cod_clasif(+)
                AND cst.rvp_cod_empresa = imp.cod_emp_atlas(+))
           Tax_partner_third_commision,
        (SELECT NVL (SUM (Re_Pk_Admon.Re_Fu_Cambio_Divisa (
                             rf.semp_cod_emp,
                             Re_Pk_Admon.Cambio_Res,
                             r.fec_creacion, --DECODE (I.IND_FEC_CAM_DIV,                                       'E', r.fec_desde,                                       r.fec_creacion),
                             SDiv_Cod_Divisa,
                             'EUR',
                             Re_Pk_Reser1.Re_Fu_Calcular_Noch_Imp (
                                Fec_Desde,
                                Fec_Hasta,
                                NRO_UNIDADES,
                                NRO_PAX,
                                IND_TIPO_UNID,
                                IND_P_S,
                                IMP_UNITARIO))),
                     0)
           FROM hbgdwc.dwc_bok_t_cost CST, hbgdwc.dwc_oth_v_re_v_impuesto_sap IMP
          WHERE     cst.grec_seq_rec = r.grec_seq_rec
                AND cst.rres_seq_reserva = r.seq_reserva
                AND cst.ind_tipo_registro = 'CA'
                AND cst.ind_facturable = 'S'
                AND cst.ind_contra_apunte = 'N'
                AND cst.rvp_ind_tipo_imp = imp.ind_tipo_imp(+)
                AND cst.rvp_cod_impuesto = imp.cod_impuesto(+)
                AND cst.rvp_cod_esquema = imp.cod_esquema(+)
                AND cst.rvp_cod_clasif = imp.cod_clasif(+)
                AND cst.rvp_cod_empresa = imp.cod_emp_atlas(+))
           Agency_comm_hotel_pay_EUR,
        (SELECT NVL (SUM (Re_Pk_Admon.Re_Fu_Cambio_Divisa (
                             rf.semp_cod_emp,
                             Re_Pk_Admon.Cambio_Res,
                             r.fec_creacion, --DECODE (I.IND_FEC_CAM_DIV,                                       'E', r.fec_desde,                                       r.fec_creacion),
                             SDiv_Cod_Divisa,
                             'EUR',
                             Re_Pk_Reser1.Re_Fu_Calcular_Noch_Imp (
                                Fec_Desde,
                                Fec_Hasta,
                                NRO_UNIDADES,
                                NRO_PAX,
                                IND_TIPO_UNID,
                                IND_P_S,
                                Re_Pk_Admon.RE_FU_REDONDEO (
                                   IMP_UNITARIO
                                   - (IMP_UNITARIO
                                      / (1 + (pct_impuesto / 100))),
                                   'EUR')))),
                     0)
           FROM hbgdwc.dwc_bok_t_cost CST, hbgdwc.dwc_oth_v_re_v_impuesto_sap IMP
          WHERE     cst.grec_seq_rec = r.grec_seq_rec
                AND cst.rres_seq_reserva = r.seq_reserva
                AND cst.ind_tipo_registro = 'CA'
                AND cst.ind_facturable = 'S'
                AND cst.ind_contra_apunte = 'N'
                AND cst.rvp_ind_tipo_imp = imp.ind_tipo_imp(+)
                AND cst.rvp_cod_impuesto = imp.cod_impuesto(+)
                AND cst.rvp_cod_esquema = imp.cod_esquema(+)
                AND cst.rvp_cod_clasif = imp.cod_clasif(+)
                AND cst.rvp_cod_empresa = imp.cod_emp_atlas(+))
           Tax_Agency_comm_hotel_pay_EUR,
        (SELECT NVL (SUM (Re_Pk_Admon.Re_Fu_Cambio_Divisa (
                             rf.semp_cod_emp,
                             Re_Pk_Admon.Cambio_Res,
                             r.fec_creacion, --DECODE (I.IND_FEC_CAM_DIV,                                       'E', r.fec_desde,                                       r.fec_creacion),
                             SDiv_Cod_Divisa,
                             'EUR',
                             Re_Pk_Reser1.Re_Fu_Calcular_Noch_Imp (
                                Fec_Desde,
                                Fec_Hasta,
                                NRO_UNIDADES,
                                NRO_PAX,
                                IND_TIPO_UNID,
                                IND_P_S,
                                IMP_UNITARIO))),
                     0)
           FROM hbgdwc.dwc_bok_t_cost CST, hbgdwc.dwc_oth_v_re_v_impuesto_sap IMP
          WHERE     cst.grec_seq_rec = r.grec_seq_rec
                AND cst.rres_seq_reserva = r.seq_reserva
                AND cst.ind_tipo_registro = 'OC'
                AND cst.ind_facturable = 'S'
                AND cst.ind_contra_apunte = 'N'
                AND cst.rvp_ind_tipo_imp = imp.ind_tipo_imp(+)
                AND cst.rvp_cod_impuesto = imp.cod_impuesto(+)
                AND cst.rvp_cod_esquema = imp.cod_esquema(+)
                AND cst.rvp_cod_clasif = imp.cod_clasif(+)
                AND cst.rvp_cod_empresa = imp.cod_emp_atlas(+))
           Fix_override_hotel_pay_EUR,
        (SELECT NVL (SUM (Re_Pk_Admon.Re_Fu_Cambio_Divisa (
                             rf.semp_cod_emp,
                             Re_Pk_Admon.Cambio_Res,
                             r.fec_creacion, --DECODE (I.IND_FEC_CAM_DIV,                                       'E', r.fec_desde,                                       r.fec_creacion),
                             SDiv_Cod_Divisa,
                             'EUR',
                             Re_Pk_Reser1.Re_Fu_Calcular_Noch_Imp (
                                Fec_Desde,
                                Fec_Hasta,
                                NRO_UNIDADES,
                                NRO_PAX,
                                IND_TIPO_UNID,
                                IND_P_S,
                                Re_Pk_Admon.RE_FU_REDONDEO (
                                   IMP_UNITARIO
                                   - (IMP_UNITARIO
                                      / (1 + (pct_impuesto / 100))),
                                   'EUR')))),
                     0)
           FROM hbgdwc.dwc_bok_t_cost CST, hbgdwc.dwc_oth_v_re_v_impuesto_sap IMP
          WHERE     cst.grec_seq_rec = r.grec_seq_rec
                AND cst.rres_seq_reserva = r.seq_reserva
                AND cst.ind_tipo_registro = 'OC'
                AND cst.ind_facturable = 'S'
                AND cst.ind_contra_apunte = 'N'
                AND cst.rvp_ind_tipo_imp = imp.ind_tipo_imp(+)
                AND cst.rvp_cod_impuesto = imp.cod_impuesto(+)
                AND cst.rvp_cod_esquema = imp.cod_esquema(+)
                AND cst.rvp_cod_clasif = imp.cod_clasif(+)
                AND cst.rvp_cod_empresa = imp.cod_emp_atlas(+))
           Tax_Fix_overr_hotel_pay_EUR,
        (SELECT NVL (SUM (Re_Pk_Admon.Re_Fu_Cambio_Divisa (
                             rf.semp_cod_emp,
                             Re_Pk_Admon.Cambio_Res,
                             r.fec_creacion, --DECODE (I.IND_FEC_CAM_DIV,                                       'E', r.fec_desde,                                       r.fec_creacion),
                             SDiv_Cod_Divisa,
                             'EUR',
                             Re_Pk_Reser1.Re_Fu_Calcular_Noch_Imp (
                                Fec_Desde,
                                Fec_Hasta,
                                NRO_UNIDADES,
                                NRO_PAX,
                                IND_TIPO_UNID,
                                IND_P_S,
                                IMP_UNITARIO))),
                     0)
           FROM hbgdwc.dwc_bok_t_cost CST, hbgdwc.dwc_oth_v_re_v_impuesto_sap IMP
          WHERE     cst.grec_seq_rec = r.grec_seq_rec
                AND cst.rres_seq_reserva = r.seq_reserva
                AND cst.ind_tipo_registro = 'OV'
                AND cst.ind_facturable = 'S'
                AND cst.ind_contra_apunte = 'N'
                AND cst.rvp_ind_tipo_imp = imp.ind_tipo_imp(+)
                AND cst.rvp_cod_impuesto = imp.cod_impuesto(+)
                AND cst.rvp_cod_esquema = imp.cod_esquema(+)
                AND cst.rvp_cod_clasif = imp.cod_clasif(+)
                AND cst.rvp_cod_empresa = imp.cod_emp_atlas(+))
           Var_override_hotel_pay_EUR,
        (SELECT NVL (SUM (Re_Pk_Admon.Re_Fu_Cambio_Divisa (
                             rf.semp_cod_emp,
                             Re_Pk_Admon.Cambio_Res,
                             r.fec_creacion, --DECODE (I.IND_FEC_CAM_DIV,                                       'E', r.fec_desde,                                       r.fec_creacion),
                             SDiv_Cod_Divisa,
                             'EUR',
                             Re_Pk_Reser1.Re_Fu_Calcular_Noch_Imp (
                                Fec_Desde,
                                Fec_Hasta,
                                NRO_UNIDADES,
                                NRO_PAX,
                                IND_TIPO_UNID,
                                IND_P_S,
                                Re_Pk_Admon.RE_FU_REDONDEO (
                                   IMP_UNITARIO
                                   - (IMP_UNITARIO
                                      / (1 + (pct_impuesto / 100))),
                                   'EUR')))),
                     0)
           FROM hbgdwc.dwc_bok_t_cost CST, hbgdwc.dwc_oth_v_re_v_impuesto_sap IMP
          WHERE     cst.grec_seq_rec = r.grec_seq_rec
                AND cst.rres_seq_reserva = r.seq_reserva
                AND cst.ind_tipo_registro = 'OV'
                AND cst.ind_facturable = 'S'
                AND cst.ind_contra_apunte = 'N'
                AND cst.rvp_ind_tipo_imp = imp.ind_tipo_imp(+)
                AND cst.rvp_cod_impuesto = imp.cod_impuesto(+)
                AND cst.rvp_cod_esquema = imp.cod_esquema(+)
                AND cst.rvp_cod_clasif = imp.cod_clasif(+)
                AND cst.rvp_cod_empresa = imp.cod_emp_atlas(+))
           Tax_Var_override_hotel_pay_EUR,
        (SELECT NVL (SUM (Re_Pk_Admon.Re_Fu_Cambio_Divisa (
                             rf.semp_cod_emp,
                             Re_Pk_Admon.Cambio_Res,
                             r.fec_creacion, --DECODE (I.IND_FEC_CAM_DIV,                                       'E', r.fec_desde,                                       r.fec_creacion),
                             SDiv_Cod_Divisa,
                             'EUR',
                             Re_Pk_Reser1.Re_Fu_Calcular_Noch_Imp (
                                Fec_Desde,
                                Fec_Hasta,
                                NRO_UNIDADES,
                                NRO_PAX,
                                IND_TIPO_UNID,
                                IND_P_S,
                                IMP_UNITARIO))),
                     0)
           FROM hbgdwc.dwc_bok_t_cost CST, hbgdwc.dwc_oth_v_re_v_impuesto_sap IMP
          WHERE     cst.grec_seq_rec = r.grec_seq_rec
                AND cst.rres_seq_reserva = r.seq_reserva
                AND cst.ind_tipo_registro = 'CH'
                AND cst.ind_facturable = 'S'
                AND cst.ind_contra_apunte = 'N'
                AND cst.rvp_ind_tipo_imp = imp.ind_tipo_imp(+)
                AND cst.rvp_cod_impuesto = imp.cod_impuesto(+)
                AND cst.rvp_cod_esquema = imp.cod_esquema(+)
                AND cst.rvp_cod_clasif = imp.cod_clasif(+)
                AND cst.rvp_cod_empresa = imp.cod_emp_atlas(+))
           Hotel_commision_hotel_pay_EUR,
        (SELECT NVL (SUM (Re_Pk_Admon.Re_Fu_Cambio_Divisa (
                             rf.semp_cod_emp,
                             Re_Pk_Admon.Cambio_Res,
                             r.fec_creacion, --DECODE (I.IND_FEC_CAM_DIV,                                       'E', r.fec_desde,                                       r.fec_creacion),
                             SDiv_Cod_Divisa,
                             'EUR',
                             Re_Pk_Reser1.Re_Fu_Calcular_Noch_Imp (
                                Fec_Desde,
                                Fec_Hasta,
                                NRO_UNIDADES,
                                NRO_PAX,
                                IND_TIPO_UNID,
                                IND_P_S,
                                Re_Pk_Admon.RE_FU_REDONDEO (
                                   IMP_UNITARIO
                                   - (IMP_UNITARIO
                                      / (1 + (pct_impuesto / 100))),
                                   'EUR')))),
                     0)
           FROM hbgdwc.dwc_bok_t_cost CST, hbgdwc.dwc_oth_v_re_v_impuesto_sap IMP
          WHERE     cst.grec_seq_rec = r.grec_seq_rec
                AND cst.rres_seq_reserva = r.seq_reserva
                AND cst.ind_tipo_registro = 'CH'
                AND cst.ind_facturable = 'S'
                AND cst.ind_contra_apunte = 'N'
                AND cst.rvp_ind_tipo_imp = imp.ind_tipo_imp(+)
                AND cst.rvp_cod_impuesto = imp.cod_impuesto(+)
                AND cst.rvp_cod_esquema = imp.cod_esquema(+)
                AND cst.rvp_cod_clasif = imp.cod_clasif(+)
                AND cst.rvp_cod_empresa = imp.cod_emp_atlas(+))
           Tax_Hotel_comm_Hotel_pay_EUR,
        (SELECT NVL (SUM (Re_Pk_Admon.Re_Fu_Cambio_Divisa (
                             rf.semp_cod_emp,
                             Re_Pk_Admon.Cambio_Res,
                             r.fec_creacion, --DECODE (I.IND_FEC_CAM_DIV,                                       'E', r.fec_desde,                                       r.fec_creacion),
                             SDiv_Cod_Divisa,
                             'EUR',
                             Re_Pk_Reser1.Re_Fu_Calcular_Noch_Imp (
                                Fec_Desde,
                                Fec_Hasta,
                                NRO_UNIDADES,
                                NRO_PAX,
                                IND_TIPO_UNID,
                                IND_P_S,
                                IMP_UNITARIO))),
                     0)
           FROM hbgdwc.dwc_bok_t_cost CST, hbgdwc.dwc_oth_v_re_v_impuesto_sap IMP
          WHERE     cst.grec_seq_rec = r.grec_seq_rec
                AND cst.rres_seq_reserva = r.seq_reserva
                AND cst.ind_tipo_registro = 'L'
                AND cst.ind_facturable = 'S'
                AND cst.ind_contra_apunte = 'N'
                AND cst.rvp_ind_tipo_imp = imp.ind_tipo_imp(+)
                AND cst.rvp_cod_impuesto = imp.cod_impuesto(+)
                AND cst.rvp_cod_esquema = imp.cod_esquema(+)
                AND cst.rvp_cod_clasif = imp.cod_clasif(+)
                AND cst.rvp_cod_empresa = imp.cod_emp_atlas(+))
           Marketing_contribution_EUR,
        (SELECT NVL (SUM (Re_Pk_Admon.Re_Fu_Cambio_Divisa (
                             rf.semp_cod_emp,
                             Re_Pk_Admon.Cambio_Res,
                             r.fec_creacion, --DECODE (I.IND_FEC_CAM_DIV,                                       'E', r.fec_desde,                                       r.fec_creacion),
                             SDiv_Cod_Divisa,
                             'EUR',
                             Re_Pk_Reser1.Re_Fu_Calcular_Noch_Imp (
                                Fec_Desde,
                                Fec_Hasta,
                                NRO_UNIDADES,
                                NRO_PAX,
                                IND_TIPO_UNID,
                                IND_P_S,
                                Re_Pk_Admon.RE_FU_REDONDEO (
                                   IMP_UNITARIO
                                   - (IMP_UNITARIO
                                      / (1 + (pct_impuesto / 100))),
                                   'EUR')))),
                     0)
           FROM hbgdwc.dwc_bok_t_cost CST, hbgdwc.dwc_oth_v_re_v_impuesto_sap IMP
          WHERE     cst.grec_seq_rec = r.grec_seq_rec
                AND cst.rres_seq_reserva = r.seq_reserva
                AND cst.ind_tipo_registro = 'L'
                AND cst.ind_facturable = 'S'
                AND cst.ind_contra_apunte = 'N'
                AND cst.rvp_ind_tipo_imp = imp.ind_tipo_imp(+)
                AND cst.rvp_cod_impuesto = imp.cod_impuesto(+)
                AND cst.rvp_cod_esquema = imp.cod_esquema(+)
                AND cst.rvp_cod_clasif = imp.cod_clasif(+)
                AND cst.rvp_cod_empresa = imp.cod_emp_atlas(+))
           Tax_marketing_contribution_EUR,
        (SELECT NVL (SUM (Re_Pk_Admon.Re_Fu_Cambio_Divisa (
                             rf.semp_cod_emp,
                             Re_Pk_Admon.Cambio_Res,
                             r.fec_creacion, --DECODE (I.IND_FEC_CAM_DIV,                                       'E', r.fec_desde,                                       r.fec_creacion),
                             SDiv_Cod_Divisa,
                             'EUR',
                             Re_Pk_Reser1.Re_Fu_Calcular_Noch_Imp (
                                Fec_Desde,
                                Fec_Hasta,
                                NRO_UNIDADES,
                                NRO_PAX,
                                IND_TIPO_UNID,
                                IND_P_S,
                                IMP_UNITARIO))),
                     0)
           FROM hbgdwc.dwc_bok_t_cost CST, hbgdwc.dwc_oth_v_re_v_impuesto_sap IMP
          WHERE     cst.grec_seq_rec = r.grec_seq_rec
                AND cst.rres_seq_reserva = r.seq_reserva
                AND cst.ind_tipo_registro = 'K'
                AND cst.ind_facturable = 'S'
                AND cst.ind_contra_apunte = 'N'
                AND cst.rvp_ind_tipo_imp = imp.ind_tipo_imp(+)
                AND cst.rvp_cod_impuesto = imp.cod_impuesto(+)
                AND cst.rvp_cod_esquema = imp.cod_esquema(+)
                AND cst.rvp_cod_clasif = imp.cod_clasif(+)
                AND cst.rvp_cod_empresa = imp.cod_emp_atlas(+))
           bank_expenses_EUR,
        (SELECT NVL (SUM (Re_Pk_Admon.Re_Fu_Cambio_Divisa (
                             rf.semp_cod_emp,
                             Re_Pk_Admon.Cambio_Res,
                             r.fec_creacion, --DECODE (I.IND_FEC_CAM_DIV,                                       'E', r.fec_desde,                                       r.fec_creacion),
                             SDiv_Cod_Divisa,
                             'EUR',
                             Re_Pk_Reser1.Re_Fu_Calcular_Noch_Imp (
                                Fec_Desde,
                                Fec_Hasta,
                                NRO_UNIDADES,
                                NRO_PAX,
                                IND_TIPO_UNID,
                                IND_P_S,
                                Re_Pk_Admon.RE_FU_REDONDEO (
                                   IMP_UNITARIO
                                   - (IMP_UNITARIO
                                      / (1 + (pct_impuesto / 100))),
                                   'EUR')))),
                     0)
           FROM hbgdwc.dwc_bok_t_cost CST, hbgdwc.dwc_oth_v_re_v_impuesto_sap IMP
          WHERE     cst.grec_seq_rec = r.grec_seq_rec
                AND cst.rres_seq_reserva = r.seq_reserva
                AND cst.ind_tipo_registro = 'K'
                AND cst.ind_facturable = 'S'
                AND cst.ind_contra_apunte = 'N'
                AND cst.rvp_ind_tipo_imp = imp.ind_tipo_imp(+)
                AND cst.rvp_cod_impuesto = imp.cod_impuesto(+)
                AND cst.rvp_cod_esquema = imp.cod_esquema(+)
                AND cst.rvp_cod_clasif = imp.cod_clasif(+)
                AND cst.rvp_cod_empresa = imp.cod_emp_atlas(+))
           Tax_Bank_expenses_EUR,
        (SELECT NVL (SUM (Re_Pk_Admon.Re_Fu_Cambio_Divisa (
                             rf.semp_cod_emp,
                             Re_Pk_Admon.Cambio_Res,
                             r.fec_creacion, --DECODE (I.IND_FEC_CAM_DIV,                                       'E', r.fec_desde,                                       r.fec_creacion),
                             SDiv_Cod_Divisa,
                             'EUR',
                             Re_Pk_Reser1.Re_Fu_Calcular_Noch_Imp (
                                Fec_Desde,
                                Fec_Hasta,
                                NRO_UNIDADES,
                                NRO_PAX,
                                IND_TIPO_UNID,
                                IND_P_S,
                                IMP_UNITARIO))),
                     0)
           FROM hbgdwc.dwc_bok_t_cost CST, hbgdwc.dwc_oth_v_re_v_impuesto_sap IMP
          WHERE     cst.grec_seq_rec = r.grec_seq_rec
                AND cst.rres_seq_reserva = r.seq_reserva
                AND cst.ind_tipo_registro = 'PF'
                AND cst.ind_facturable = 'S'
                AND cst.ind_contra_apunte = 'N'
                AND cst.rvp_ind_tipo_imp = imp.ind_tipo_imp(+)
                AND cst.rvp_cod_impuesto = imp.cod_impuesto(+)
                AND cst.rvp_cod_esquema = imp.cod_esquema(+)
                AND cst.rvp_cod_clasif = imp.cod_clasif(+)
                AND cst.rvp_cod_empresa = imp.cod_emp_atlas(+))
           Platform_Fee_EUR,
        (SELECT NVL (SUM (Re_Pk_Admon.Re_Fu_Cambio_Divisa (
                             rf.semp_cod_emp,
                             Re_Pk_Admon.Cambio_Res,
                             r.fec_creacion, --DECODE (I.IND_FEC_CAM_DIV,                                       'E', r.fec_desde,                                       r.fec_creacion),
                             SDiv_Cod_Divisa,
                             'EUR',
                             Re_Pk_Reser1.Re_Fu_Calcular_Noch_Imp (
                                Fec_Desde,
                                Fec_Hasta,
                                NRO_UNIDADES,
                                NRO_PAX,
                                IND_TIPO_UNID,
                                IND_P_S,
                                Re_Pk_Admon.RE_FU_REDONDEO (
                                   IMP_UNITARIO
                                   - (IMP_UNITARIO
                                      / (1 + (pct_impuesto / 100))),
                                   'EUR')))),
                     0)
           FROM hbgdwc.dwc_bok_t_cost CST, hbgdwc.dwc_oth_v_re_v_impuesto_sap IMP
          WHERE     cst.grec_seq_rec = r.grec_seq_rec
                AND cst.rres_seq_reserva = r.seq_reserva
                AND cst.ind_tipo_registro = 'PF'
                AND cst.ind_facturable = 'S'
                AND cst.ind_contra_apunte = 'N'
                AND cst.rvp_ind_tipo_imp = imp.ind_tipo_imp(+)
                AND cst.rvp_cod_impuesto = imp.cod_impuesto(+)
                AND cst.rvp_cod_esquema = imp.cod_esquema(+)
                AND cst.rvp_cod_clasif = imp.cod_clasif(+)
                AND cst.rvp_cod_empresa = imp.cod_emp_atlas(+))
           Tax_Platform_fee_EUR,
        (SELECT NVL (SUM (Re_Pk_Admon.Re_Fu_Cambio_Divisa (
                             rf.semp_cod_emp,
                             Re_Pk_Admon.Cambio_Res,
                             r.fec_creacion, --DECODE (I.IND_FEC_CAM_DIV,                                       'E', r.fec_desde,                                       r.fec_creacion),
                             SDiv_Cod_Divisa,
                             'EUR',
                             Re_Pk_Reser1.Re_Fu_Calcular_Noch_Imp (
                                Fec_Desde,
                                Fec_Hasta,
                                NRO_UNIDADES,
                                NRO_PAX,
                                IND_TIPO_UNID,
                                IND_P_S,
                                IMP_UNITARIO))),
                     0)
           FROM hbgdwc.dwc_bok_t_cost CST, hbgdwc.dwc_oth_v_re_v_impuesto_sap IMP
          WHERE     cst.grec_seq_rec = r.grec_seq_rec
                AND cst.rres_seq_reserva = r.seq_reserva
                AND cst.ind_tipo_registro = 'CT'
                AND cst.ind_facturable = 'S'
                AND cst.ind_contra_apunte = 'N'
                AND cst.rvp_ind_tipo_imp = imp.ind_tipo_imp(+)
                AND cst.rvp_cod_impuesto = imp.cod_impuesto(+)
                AND cst.rvp_cod_esquema = imp.cod_esquema(+)
                AND cst.rvp_cod_clasif = imp.cod_clasif(+)
                AND cst.rvp_cod_empresa = imp.cod_emp_atlas(+))
           credit_card_fee_EUR,
        (SELECT NVL (SUM (Re_Pk_Admon.Re_Fu_Cambio_Divisa (
                             rf.semp_cod_emp,
                             Re_Pk_Admon.Cambio_Res,
                             r.fec_creacion, --DECODE (I.IND_FEC_CAM_DIV,                                       'E', r.fec_desde,                                       r.fec_creacion),
                             SDiv_Cod_Divisa,
                             'EUR',
                             Re_Pk_Reser1.Re_Fu_Calcular_Noch_Imp (
                                Fec_Desde,
                                Fec_Hasta,
                                NRO_UNIDADES,
                                NRO_PAX,
                                IND_TIPO_UNID,
                                IND_P_S,
                                Re_Pk_Admon.RE_FU_REDONDEO (
                                   IMP_UNITARIO
                                   - (IMP_UNITARIO
                                      / (1 + (pct_impuesto / 100))),
                                   'EUR')))),
                     0)
           FROM hbgdwc.dwc_bok_t_cost CST, hbgdwc.dwc_oth_v_re_v_impuesto_sap IMP
          WHERE     cst.grec_seq_rec = r.grec_seq_rec
                AND cst.rres_seq_reserva = r.seq_reserva
                AND cst.ind_tipo_registro = 'CT'
                AND cst.ind_facturable = 'S'
                AND cst.ind_contra_apunte = 'N'
                AND cst.rvp_ind_tipo_imp = imp.ind_tipo_imp(+)
                AND cst.rvp_cod_impuesto = imp.cod_impuesto(+)
                AND cst.rvp_cod_esquema = imp.cod_esquema(+)
                AND cst.rvp_cod_clasif = imp.cod_clasif(+)
                AND cst.rvp_cod_empresa = imp.cod_emp_atlas(+))
           Tax_credit_card_fee_EUR,
        (SELECT NVL (SUM (Re_Pk_Admon.Re_Fu_Cambio_Divisa (
                             rf.semp_cod_emp,
                             Re_Pk_Admon.Cambio_Res,
                             r.fec_creacion, --DECODE (I.IND_FEC_CAM_DIV,                                       'E', r.fec_desde,                                       r.fec_creacion),
                             SDiv_Cod_Divisa,
                             'EUR',
                             Re_Pk_Reser1.Re_Fu_Calcular_Noch_Imp (
                                Fec_Desde,
                                Fec_Hasta,
                                NRO_UNIDADES,
                                NRO_PAX,
                                IND_TIPO_UNID,
                                IND_P_S,
                                IMP_UNITARIO))),
                     0)
           FROM hbgdwc.dwc_bok_t_cost CST, hbgdwc.dwc_oth_v_re_v_impuesto_sap IMP
          WHERE     cst.grec_seq_rec = r.grec_seq_rec
                AND cst.rres_seq_reserva = r.seq_reserva
                AND cst.ind_tipo_registro = 'SC'
                AND cst.ind_facturable = 'S'
                AND cst.ind_contra_apunte = 'N'
                AND cst.rvp_ind_tipo_imp = imp.ind_tipo_imp(+)
                AND cst.rvp_cod_impuesto = imp.cod_impuesto(+)
                AND cst.rvp_cod_esquema = imp.cod_esquema(+)
                AND cst.rvp_cod_clasif = imp.cod_clasif(+)
                AND cst.rvp_cod_empresa = imp.cod_emp_atlas(+))
           Withholding_EUR,
        (SELECT NVL (SUM (Re_Pk_Admon.Re_Fu_Cambio_Divisa (
                             rf.semp_cod_emp,
                             Re_Pk_Admon.Cambio_Res,
                             r.fec_creacion, --DECODE (I.IND_FEC_CAM_DIV,                                       'E', r.fec_desde,                                       r.fec_creacion),
                             SDiv_Cod_Divisa,
                             'EUR',
                             Re_Pk_Reser1.Re_Fu_Calcular_Noch_Imp (
                                Fec_Desde,
                                Fec_Hasta,
                                NRO_UNIDADES,
                                NRO_PAX,
                                IND_TIPO_UNID,
                                IND_P_S,
                                Re_Pk_Admon.RE_FU_REDONDEO (
                                   IMP_UNITARIO
                                   - (IMP_UNITARIO
                                      / (1 + (pct_impuesto / 100))),
                                   'EUR')))),
                     0)
           FROM hbgdwc.dwc_bok_t_cost CST, hbgdwc.dwc_oth_v_re_v_impuesto_sap IMP
          WHERE     cst.grec_seq_rec = r.grec_seq_rec
                AND cst.rres_seq_reserva = r.seq_reserva
                AND cst.ind_tipo_registro = 'SC'
                AND cst.ind_facturable = 'S'
                AND cst.ind_contra_apunte = 'N'
                AND cst.rvp_ind_tipo_imp = imp.ind_tipo_imp(+)
                AND cst.rvp_cod_impuesto = imp.cod_impuesto(+)
                AND cst.rvp_cod_esquema = imp.cod_esquema(+)
                AND cst.rvp_cod_clasif = imp.cod_clasif(+)
                AND cst.rvp_cod_empresa = imp.cod_emp_atlas(+))
           Tax_withholding_EUR,
        (SELECT NVL (SUM (Re_Pk_Admon.Re_Fu_Cambio_Divisa (
                             rf.semp_cod_emp,
                             Re_Pk_Admon.Cambio_Res,
                             r.fec_creacion, --DECODE (I.IND_FEC_CAM_DIV,                                       'E', r.fec_desde,                                       r.fec_creacion),
                             SDiv_Cod_Divisa,
                             'EUR',
                             Re_Pk_Reser1.Re_Fu_Calcular_Noch_Imp (
                                Fec_Desde,
                                Fec_Hasta,
                                NRO_UNIDADES,
                                NRO_PAX,
                                IND_TIPO_UNID,
                                IND_P_S,
                                IMP_UNITARIO))),
                     0)
           FROM hbgdwc.dwc_bok_t_cost CST, hbgdwc.dwc_oth_v_re_v_impuesto_sap IMP
          WHERE     cst.grec_seq_rec = r.grec_seq_rec
                AND cst.rres_seq_reserva = r.seq_reserva
                AND cst.ind_tipo_registro = 'LL'
                AND cst.ind_facturable = 'S'
                AND cst.ind_contra_apunte = 'N'
                AND cst.rvp_ind_tipo_imp = imp.ind_tipo_imp(+)
                AND cst.rvp_cod_impuesto = imp.cod_impuesto(+)
                AND cst.rvp_cod_esquema = imp.cod_esquema(+)
                AND cst.rvp_cod_clasif = imp.cod_clasif(+)
                AND cst.rvp_cod_empresa = imp.cod_emp_atlas(+))
           Local_Levy_EUR,
        (SELECT NVL (SUM (Re_Pk_Admon.Re_Fu_Cambio_Divisa (
                             rf.semp_cod_emp,
                             Re_Pk_Admon.Cambio_Res,
                             r.fec_creacion, --DECODE (I.IND_FEC_CAM_DIV,                                       'E', r.fec_desde,                                       r.fec_creacion),
                             SDiv_Cod_Divisa,
                             'EUR',
                             Re_Pk_Reser1.Re_Fu_Calcular_Noch_Imp (
                                Fec_Desde,
                                Fec_Hasta,
                                NRO_UNIDADES,
                                NRO_PAX,
                                IND_TIPO_UNID,
                                IND_P_S,
                                Re_Pk_Admon.RE_FU_REDONDEO (
                                   IMP_UNITARIO
                                   - (IMP_UNITARIO
                                      / (1 + (pct_impuesto / 100))),
                                   'EUR')))),
                     0)
           FROM hbgdwc.dwc_bok_t_cost CST, hbgdwc.dwc_oth_v_re_v_impuesto_sap IMP
          WHERE     cst.grec_seq_rec = r.grec_seq_rec
                AND cst.rres_seq_reserva = r.seq_reserva
                AND cst.ind_tipo_registro = 'LL'
                AND cst.ind_facturable = 'S'
                AND cst.ind_contra_apunte = 'N'
                AND cst.rvp_ind_tipo_imp = imp.ind_tipo_imp(+)
                AND cst.rvp_cod_impuesto = imp.cod_impuesto(+)
                AND cst.rvp_cod_esquema = imp.cod_esquema(+)
                AND cst.rvp_cod_clasif = imp.cod_clasif(+)
                AND cst.rvp_cod_empresa = imp.cod_emp_atlas(+))
           Tax_Local_Levy_EUR,
        (SELECT NVL (SUM (Re_Pk_Admon.Re_Fu_Cambio_Divisa (
                             rf.semp_cod_emp,
                             Re_Pk_Admon.Cambio_Res,
                             r.fec_creacion, --DECODE (I.IND_FEC_CAM_DIV,                                       'E', r.fec_desde,                                       r.fec_creacion),
                             SDiv_Cod_Divisa,
                             'EUR',
                             Re_Pk_Reser1.Re_Fu_Calcular_Noch_Imp (
                                Fec_Desde,
                                Fec_Hasta,
                                NRO_UNIDADES,
                                NRO_PAX,
                                IND_TIPO_UNID,
                                IND_P_S,
                                IMP_UNITARIO))),
                     0)
           FROM hbgdwc.dwc_bok_t_cost CST, hbgdwc.dwc_oth_v_re_v_impuesto_sap IMP
          WHERE     cst.grec_seq_rec = r.grec_seq_rec
                AND cst.rres_seq_reserva = r.seq_reserva
                AND cst.ind_tipo_registro = 'I'
                AND cst.ind_facturable = 'S'
                AND cst.ind_contra_apunte = 'N'
                AND cst.rvp_ind_tipo_imp = imp.ind_tipo_imp(+)
                AND cst.rvp_cod_impuesto = imp.cod_impuesto(+)
                AND cst.rvp_cod_esquema = imp.cod_esquema(+)
                AND cst.rvp_cod_clasif = imp.cod_clasif(+)
                AND cst.rvp_cod_empresa = imp.cod_emp_atlas(+))
           Partner_Third_comm_EUR,
        (SELECT NVL (SUM (Re_Pk_Admon.Re_Fu_Cambio_Divisa (
                             rf.semp_cod_emp,
                             Re_Pk_Admon.Cambio_Res,
                             r.fec_creacion, --DECODE (I.IND_FEC_CAM_DIV,                                       'E', r.fec_desde,                                       r.fec_creacion),
                             SDiv_Cod_Divisa,
                             'EUR',
                             Re_Pk_Reser1.Re_Fu_Calcular_Noch_Imp (
                                Fec_Desde,
                                Fec_Hasta,
                                NRO_UNIDADES,
                                NRO_PAX,
                                IND_TIPO_UNID,
                                IND_P_S,
                                Re_Pk_Admon.RE_FU_REDONDEO (
                                   IMP_UNITARIO
                                   - (IMP_UNITARIO
                                      / (1 + (pct_impuesto / 100))),
                                   'EUR')))),
                     0)
           FROM hbgdwc.dwc_bok_t_cost CST, hbgdwc.dwc_oth_v_re_v_impuesto_sap IMP
          WHERE     cst.grec_seq_rec = r.grec_seq_rec
                AND cst.rres_seq_reserva = r.seq_reserva
                AND cst.ind_tipo_registro = 'I'
                AND cst.ind_facturable = 'S'
                AND cst.ind_contra_apunte = 'N'
                AND cst.rvp_ind_tipo_imp = imp.ind_tipo_imp(+)
                AND cst.rvp_cod_impuesto = imp.cod_impuesto(+)
                AND cst.rvp_cod_esquema = imp.cod_esquema(+)
                AND cst.rvp_cod_clasif = imp.cod_clasif(+)
                AND cst.rvp_cod_empresa = imp.cod_emp_atlas(+))
           Tax_partner_third_comm_EUR,
        (SELECT COUNT (1)
           FROM hbgdwc.dwc_bok_t_hotel_sale hv
          WHERE     hv.grec_seq_rec = r.grec_seq_rec
                AND hv.rres_seq_reserva = r.seq_reserva
                AND hv.fec_cancelacion IS NULL)
           NUMBER_ACTIVE_ACC_SERV
  FROM hbgdwc.dwc_bok_t_booking r,
       hbgdwc.dwc_mtd_t_ttoo t,
       hbgdwc.dwc_mtd_t_receptive re,
       hbgdwc.dwc_mtd_t_receptive rf,
       hbgdwc.dwc_itf_t_fc_interface i,
       hbgdwc.dwc_gen_t_general_country p
 WHERE     r.gtto_seq_ttoo = t.seq_ttoo
   AND r.grec_Seq_rec = re.seq_rec
   AND r.seq_rec_hbeds = rf.seq_rec
   AND NVL (R.COD_PAIS_CLIENTE, t.gpai_cod_pais_mercado) = p.COD_PAIS
   and r.fec_modifica between :pFecini and :pFecFin
   AND r.fec_creacion BETWEEN :pFecini AND :pFecFin
   AND R.FINT_COD_INTERFACE = i.cod_interface
   --AND r.grec_Seq_rec = 256
   --AND r.seq_reserva = 2910186