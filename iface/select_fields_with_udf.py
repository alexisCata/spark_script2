import sys
from pyspark.sql import functions as func
from pyspark.sql.types import *
from functions import *

EUR = 'EUR'
uEUR = u'EUR'
# # udf_currency_exchange = func.udf(currency_exchange, DecimalType(15, 3)) # replace with exchange_amount (cristobal)
udf_currency_exchange = func.udf(exchange_amount_decimal, FloatType())
udf_calculate_night_amount = func.udf(calculate_night_amount, DecimalType(15, 3))
# # udf_round_ccy = func.udf(round_ccy, DecimalType()
udf_round_ccy = func.udf(round_ccy, StringType())  # The results have different number of decimals
udf_date_to_string = func.udf(date_to_datetime, StringType())


# udf_date_to_string = func.udf(date_to_string, StringType())


def load_tables(manager):
    """
    It loads in spark context UDFs and tables retrieved from RedShift
    :param manager: 
    :return: 
    """
    load_round_ccy(manager)

    manager.session.udf.register("decode", decode_sql)
    # manager.session.udf.register("exchange_rate", exchange_amount_decimal)
    # manager.session.udf.register("night_amount", calculate_night_amount)
    # manager.session.udf.register("round_ccy", round_ccy)

    df = manager.get_dataframe(tables['dwc_oth_v_re_v_impuesto_sap'])
    df_bok = manager.get_dataframe(tables['dwc_bok_t_extra'].format(sys.argv[1]))
    df_discount = manager.get_dataframe(tables['dwc_cli_dir_t_cd_discount_bond'])
    df_campaignt = manager.get_dataframe(tables['dwc_cli_dir_t_cd_campaign'])
    df_hotelt = manager.get_dataframe(tables['dwc_bok_t_canco_hotel'].format(sys.argv[1]))
    df_circuitt = manager.get_dataframe(tables['dwc_bok_t_canco_hotel_circuit'].format(sys.argv[1]))
    df_othert = manager.get_dataframe(tables['dwc_bok_t_canco_other'].format(sys.argv[1]))
    df_transfert = manager.get_dataframe(tables['dwc_bok_t_canco_transfer'].format(sys.argv[1]))
    df_endowt = manager.get_dataframe(tables['dwc_bok_t_canco_endowments'].format(sys.argv[1]))
    df_extrat = manager.get_dataframe(tables['dwc_bok_t_canco_extra'].format(sys.argv[1]))
    df_cost = manager.get_dataframe(tables['dwc_bok_t_cost'].format(sys.argv[1]))

    df.createOrReplaceTempView("impuesto_sap")
    df_bok.createOrReplaceTempView("bok_extra")
    df_discount.createOrReplaceTempView("discount_bond")
    df_campaignt.createOrReplaceTempView("campaign")
    df_hotelt.createOrReplaceTempView("hotel")
    df_circuitt.createOrReplaceTempView("circuit")
    df_othert.createOrReplaceTempView("other")
    df_transfert.createOrReplaceTempView("transfer")
    df_endowt.createOrReplaceTempView("endowments")
    df_extrat.createOrReplaceTempView("extra")
    df_cost.createOrReplaceTempView("cost")

    # df.cache()
    # df_bok.cache()
    # df_discount.cache()
    # df_campaignt.cache()
    # df_hotelt.cache()
    # df_circuitt.cache()
    # df_othert.cache()
    # df_transfert.cache()
    # df_endowt.cache()
    # df_extrat.cache()


def direct(manager):
    """
    It retrieves data from Redshift of a given day as main query.
    It also remove the empty fields to calculate
    :param manager: 
    :return: dataframe
    """
    df_direct_fields = manager.get_dataframe(tables["booking"].format(sys.argv[1]))

    df_direct_fields = remove_fields(df_direct_fields)

    # df_direct_fields.cache()

    return df_direct_fields


def load_dataframe(manager, query):
    """
    It returns a dataframe for the given query
    :param manager: 
    :param query: 
    :param table: 
    :return: dataframe
    """
    df_res = manager.session.sql(query)

    return df_res


#
# def sub_tax_sales_transfer_pricing_aux(manager, table):
#     """
#     It retrieves the subqueries for field tax_sales_transfer_pricing
#     :param manager:
#     :param table:
#     :return: dataframe
#     """
#     df_res = manager.session.sql(tables["sales_aux"].format(table))
#
#     return df_res
#
#
# def sub_tax_sales_transfer_pricing_aux_extra(manager, table):
#     """
#     It retrieves the subquery from dwc_bok_t_canco_extra for field tax_sales_transfer_pricing
#     :param manager:
#     :param table:
#     :return: dataframe
#     """
#     df_res = manager.session.sql(tables["sales_aux_extra"].format(table))
#
#     return df_res


def sub_tax_sales_transfer_pricing(manager, df_fields):
    """
    It calculates the subquery for the field Tax_Sales_Transfer_pricing 
    :param manager: 
    :param df_fields: 
    :return: dataframe
    """
    df_aux = df_fields.select("operative_incoming", "booking_id")
    # df_aux.cache()
    df_aux.createOrReplaceTempView("aux")

    df_hotel = load_dataframe(manager, tables["sales_aux"].format("hotel"))
    df_circuit = load_dataframe(manager, tables["sales_aux"].format("circuit"))
    df_other = load_dataframe(manager, tables["sales_aux"].format("other"))
    df_transfer = load_dataframe(manager, tables["sales_aux"].format("transfer"))
    df_endow = load_dataframe(manager, tables["sales_aux"].format("endowments"))
    df_extra = load_dataframe(manager, tables["sales_aux_extra"].format("extra"))

    df_impuesto_can = df_hotel.union(df_circuit).union(df_other).union(df_transfer).union(df_endow).union(df_extra)

    df_impuesto_canal = df_impuesto_can.groupBy("seq_rec", "seq_reserva") \
        .agg({'impuesto_canal': 'sum'}).withColumnRenamed("SUM(impuesto_canal)", "tax_sales_transfer_pricing")

    df_fields = df_fields.join(df_impuesto_canal, [df_fields.operative_incoming == df_impuesto_canal.seq_rec,
                                                   df_fields.booking_id == df_impuesto_canal.seq_reserva],
                               'left_outer').drop(df_impuesto_canal.seq_rec).drop(df_impuesto_canal.seq_reserva)

    df_fields1 = df_fields.na.fill({"tax_sales_transfer_pricing": 0})

    df_fields1.cache()

    return df_fields1


def sub_tax_sales_transfer_pricing_eur(manager, df_fields):
    """
    It calculate the field tax_sales_transfer_pricing_eur and rounds both tax_sales_transfer_pricing 
    :param manager: 
    :param table: 
    :return: dataframe
    """
    mylist = df_fields.collect()
    # newlist = []
    # for row in mylist:
    #     new_row = []
    #     for field in row:
    #         new_row.append(field)
    #     ccy = new_row[-2]
    #     amount = new_row[-1]
    #     new_row[4] = round_ccy(amount, ccy)
    #     pricing_eur = round_ccy(exchange_amount_decimal(new_row[3], EUR, new_row[2], new_row[4]), uEUR)
    #     final = [new_row[0], new_row[1], new_row[4], pricing_eur]
    #     newlist.append(final)
    #
    # schema = StructType([StructField("operative_incoming", IntegerType()),
    #                      StructField("booking_id", IntegerType()),
    #                      StructField("tax_sales_transfer_pricing", StringType()),
    #                      StructField("tax_sales_transfer_pricing_eur", StringType())])
    #
    # df_fields2 = manager.session.createDataFrame(newlist, schema)

    df_fields = df_fields.withColumn('tax_sales_transfer_pricing', udf_round_ccy(df_fields.tax_sales_transfer_pricing,
                                                                                 df_fields.booking_currency))

    # df_fields = df_fields.withColumn('tax_sales_transfer_pricing_eur',
    #                                  udf_round_ccy(udf_currency_exchange(df_fields.booking_currency,
    #                                                                      func.lit(EUR),
    #                                                                      df_fields.creation_date,
    #                                                                      df_fields.tax_sales_transfer_pricing),
    #                                                func.lit(uEUR)))

    df_fields = df_fields.withColumn('tax_sales_transfer_pricing_eur', df_fields.tax_sales_transfer_pricing)

    df_fields2 = df_fields.na.fill({'tax_sales_transfer_pricing_eur': 0}).select("operative_incoming", "booking_id",
                                                                                 "tax_sales_transfer_pricing",
                                                                                 "tax_sales_transfer_pricing_eur")

    df_fields2.cache()

    d = df_fields2.collect()

    return df_fields2


# def sub_tax_cost_transfer_pricing_aux(manager, table):
#     """
#     It retrieves the subqueries for field tax_cost_transfer_pricing
#     :param manager:
#     :param table:
#     :return: dataframe
#     """
#     df_res = manager.session.sql(tables["cost_transfer_pricing_aux"].format(table))
#
#     return df_res
#
#
# def sub_tax_cost_transfer_pricing_aux_extra(manager, table):
#     """
#     It retrieves the subquery from dwc_bok_t_canco_extra for field tax_sales_transfer_pricing
#     :param manager:
#     :param table:
#     :return: dataframe
#     """
#     df_res = manager.session.sql(tables["cost_transfer_pricing_aux_extra"].format(table))
#
#     return df_res


def sub_tax_cost_transfer_pricing(manager, df_fields):
    """
    It retrieves the query data for field Tax_Cost_Transfer_pricing 
    :param manager: 
    :param df_fields: 
    :return: dataframe
    """

    df_aux = df_fields.select("operative_incoming", "booking_id", "creation_date",
                              "booking_currency")
    df_aux.createOrReplaceTempView("aux")

    df_hotel = load_dataframe(manager, tables["cost_transfer_pricing_aux"].format("hotel"))
    df_circuit = load_dataframe(manager, tables["cost_transfer_pricing_aux"].format("circuit"))
    df_other = load_dataframe(manager, tables["cost_transfer_pricing_aux"].format("other"))
    df_transfer = load_dataframe(manager, tables["cost_transfer_pricing_aux"].format("transfer"))
    df_endow = load_dataframe(manager, tables["cost_transfer_pricing_aux"].format("endowments"))
    df_extra = load_dataframe(manager, tables["cost_transfer_pricing_aux_extra"].format("extra"))

    df_impuesto_canco = df_hotel.union(df_circuit).union(df_other).union(df_transfer).union(df_endow).union(df_extra)

    df_impuesto_canco = df_impuesto_canco.groupBy("seq_rec", "seq_reserva") \
        .agg({'impuesto_canco': 'sum'}).withColumnRenamed("SUM(impuesto_canco)", "impuesto_canco")

    df_addcanco = manager.session.sql(tables["add_canco"])

    df_addcanco.cache()

    df_addcanco1 = df_addcanco.withColumn("night_amount",
                                          udf_calculate_night_amount(df_addcanco.fec_desde,
                                                                     df_addcanco.fec_hasta,
                                                                     df_addcanco.nro_unidades,
                                                                     df_addcanco.nro_pax,
                                                                     df_addcanco.ind_tipo_unid,
                                                                     df_addcanco.ind_p_s,
                                                                     df_addcanco.imp_unitario))

    df_addcanco = df_addcanco1.withColumn("cost_field",
                                          udf_currency_exchange(df_addcanco1.sdiv_cod_divisa,
                                                                df_addcanco1.booking_currency,
                                                                df_addcanco1.creation_date,
                                                                df_addcanco1.night_amount))
    df_addcanco.cache()

    # mylist = df_addcanco.collect()
    # newlist = []
    #
    # for row in mylist:
    #     new_row = []
    #     for field in row:
    #         new_row.append(field)
    #     night_amount = calculate_night_amount(new_row[5], new_row[6], new_row[7], new_row[8],
    #                                           new_row[9], new_row[10], new_row[11])
    #     exchange = exchange_amount_decimal(new_row[5], new_row[6], new_row[7], night_amount or 0)
    #     final_row = [new_row[0], new_row[1], exchange]
    #     newlist.append(final_row)
    #
    # schema = StructType([StructField("operative_incoming", IntegerType()),
    #                      StructField("booking_id", IntegerType()),
    #                      StructField("cost_field", FloatType())])
    #
    # df_addcanco = manager.session.createDataFrame(newlist, schema)

    df_aux.select("operative_incoming", "booking_id", "booking_currency").createOrReplaceTempView("aux")
    df_impuesto_canco.createOrReplaceTempView("canco")
    df_addcanco.createOrReplaceTempView("add_canco")
    # df_addcanco2.createOrReplaceTempView("add_canco")
    df_r = manager.session.sql(tables["cost_transfer_pricing"])

    df_res = df_fields.join(df_r, ["operative_incoming", "booking_id"]) \
        .select(df_fields.operative_incoming, df_fields.booking_id, df_fields.creation_date,
                df_fields.booking_currency, df_r.tax_cost_transfer_pricing)

    df_res.cache()

    return df_res


def sub_tax_cost_transfer_pricing_eur(manager, df_fields):
    """
    It calculate the field tax_cost_transfer_pricing_eur and rounds both tax_cost_transfer_pricing 
    :param manager: 
    :param table: 
    :return: dataframe
    """
    # mylist = df_fields.collect()
    # newlist = []
    # for row in mylist:
    #     new_row = []
    #     for field in row:
    #         new_row.append(field)
    #     ccy = new_row[-2]
    #     amount = new_row[-1]
    #     new_row[4] = round_ccy(amount, ccy)
    #     pricing_eur = round_ccy(exchange_amount_decimal(new_row[3], EUR, new_row[2], new_row[4]), uEUR)
    #     final = [new_row[0], new_row[1], new_row[4], pricing_eur]
    #     newlist.append(final)
    #
    # schema = StructType([StructField("operative_incoming", IntegerType()),
    #                      StructField("booking_id", IntegerType()),
    #                      StructField("tax_cost_transfer_pricing", StringType()),
    #                      StructField("tax_cost_transfer_pricing_eur", StringType())])
    #
    # df_fields2 = manager.session.createDataFrame(newlist, schema)

    df_fields1 = df_fields.withColumn('tax_cost_transfer_pricing', udf_round_ccy(df_fields.tax_cost_transfer_pricing,
                                                                                 df_fields.booking_currency))

    df_fields2 = df_fields1.withColumn('tax_cost_transfer_pricing_eur',
                                       udf_round_ccy(udf_currency_exchange(df_fields1.booking_currency,
                                                                           func.lit(EUR),
                                                                           df_fields1.creation_date,
                                                                           df_fields1.tax_cost_transfer_pricing),
                                                     func.lit(uEUR)))

    df_fields2 = df_fields2.na.fill({'tax_cost_transfer_pricing_eur': 0}).select("operative_incoming", "booking_id",
                                                                                 "tax_cost_transfer_pricing",
                                                                                 "tax_cost_transfer_pricing_eur")

    df_fields2.cache()

    d = df_fields2.collect()

    return df_fields2


def sub_transfer_pricing_aux(manager, table):
    """
    It retrieves the subqueries for field transfer_pricing
    :param manager: 
    :param table: 
    :return: dataframe
    """
    df_tab = load_dataframe(manager, tables["transfer_pricing_aux"].format(table))

    df_tab.cache()

    df_tab = df_tab.withColumn("amount_2", udf_currency_exchange(df_tab.booking_currency,
                                                                 func.lit(EUR),
                                                                 df_tab.creation_date,
                                                                 df_tab.amount_2))
    df_tab = df_tab.withColumn("impuesto_canco", df_tab.amount_1 + df_tab.amount_2)

    df_res = df_tab.select("seq_rec", "seq_reserva", "impuesto_canco")

    df_res.cache()

    # mylist = df_tab.collect()
    # newlist = []
    #
    # for row in mylist:
    #     new_row = []
    #     for field in row:
    #         new_row.append(field)
    #
    #     exchange = exchange_amount_decimal(new_row[3], EUR, new_row[4], new_row[5])
    #     final_row = [new_row[0], new_row[1], float(new_row[2]) + exchange]
    #     newlist.append(final_row)
    #
    # schema = StructType([StructField("seq_rec", IntegerType()),
    #                      StructField("seq_reserva", IntegerType()),
    #                      StructField("impuesto_canco", FloatType())])
    #
    # df_res = manager.session.createDataFrame(newlist, schema)

    return df_res


def sub_transfer_pricing_aux_extra(manager, table):
    """
    It retrieves the subquery from dwc_bok_t_canco_extra for field transfer_pricing
    :param manager: 
    :param table: 
    :return: dataframe
    """
    df_tab = load_dataframe(manager, tables["transfer_pricing_aux_extra"].format(table))

    df_tab.cache()

    df_tab = df_tab.withColumn("amount_2", udf_currency_exchange(df_tab.booking_currency,
                                                                 func.lit(EUR),
                                                                 df_tab.creation_date,
                                                                 df_tab.amount_2))
    df_tab = df_tab.withColumn("impuesto_canco", df_tab.amount_1 + df_tab.amount_2)

    df_res = df_tab.select("seq_rec", "seq_reserva", "impuesto_canco")

    df_res.cache()

    #
    # mylist = df_tab.collect()
    # newlist = []
    #
    # for row in mylist:
    #     new_row = []
    #     for field in row:
    #         new_row.append(field)
    #
    #     exchange = exchange_amount_decimal(new_row[3], EUR, new_row[4], new_row[5])
    #     final_row = [new_row[0], new_row[1], float(new_row[2]) + exchange]
    #     newlist.append(final_row)
    #
    # schema = StructType([StructField("seq_rec", IntegerType()),
    #                      StructField("seq_reserva", IntegerType()),
    #                      StructField("impuesto_canco", FloatType())])
    #
    # df_res = manager.session.createDataFrame(newlist, schema)

    return df_res


def sub_transfer_pricing(manager, df_fields):
    """
    It calculates the subquery for the field Transfer_pricing 
    :param manager: 
    :param df_fields: 
    :param seq_recs: 
    :param seq_reservas: 
    :return: dataframe
    """
    df_aux = df_fields.select("operative_incoming", "booking_id", "creation_date", "booking_currency")
    df_aux.cache()
    df_aux.createOrReplaceTempView("aux")

    df_hotel = sub_transfer_pricing_aux(manager, "hotel")
    df_circuit = sub_transfer_pricing_aux(manager, "circuit")
    df_other = sub_transfer_pricing_aux(manager, "other")
    df_transfer = sub_transfer_pricing_aux(manager, "transfer")
    df_endow = sub_transfer_pricing_aux(manager, "endowments")
    df_extra = sub_transfer_pricing_aux_extra(manager, "extra")

    df_impuesto_canco = df_hotel.union(df_circuit).union(df_other).union(df_transfer).union(df_endow).union(
        df_extra)

    df_impuesto_canco = df_impuesto_canco.groupBy("seq_rec", "seq_reserva") \
        .agg({'impuesto_canco': 'sum'}).withColumnRenamed("SUM(impuesto_canco)", "impuesto_canco")

    df_addcanco = manager.session.sql(tables["add_canco"])

    df_addcanco.cache()

    df_addcanco1 = df_addcanco.withColumn("night_amount",
                                          udf_calculate_night_amount(df_addcanco.fec_desde,
                                                                     df_addcanco.fec_hasta,
                                                                     df_addcanco.nro_unidades,
                                                                     df_addcanco.nro_pax,
                                                                     df_addcanco.ind_tipo_unid,
                                                                     df_addcanco.ind_p_s,
                                                                     df_addcanco.imp_unitario))

    df_addcanco = df_addcanco1.withColumn("cost_field",
                                          udf_currency_exchange(df_addcanco1.sdiv_cod_divisa,
                                                                df_addcanco1.booking_currency,
                                                                df_addcanco1.creation_date,
                                                                df_addcanco1.night_amount))
    df_addcanco.cache()
    # mylist = df_addcanco.collect()
    # newlist = []
    #
    # for row in mylist:
    #     new_row = []
    #     for field in row:
    #         new_row.append(field)
    #     night_amount = calculate_night_amount(new_row[5], new_row[6], new_row[7], new_row[8],
    #                                           new_row[9], new_row[10], new_row[11])
    #     exchange = exchange_amount_decimal(new_row[5], new_row[6], new_row[7], night_amount or 0)
    #     final_row = [new_row[0], new_row[1], exchange]
    #     newlist.append(final_row)
    #
    # schema = StructType([StructField("operative_incoming", IntegerType()),
    #                      StructField("booking_id", IntegerType()),
    #                      StructField("cost_field", FloatType())])
    #
    # df_addcanco = manager.session.createDataFrame(newlist, schema)

    df_aux.select("operative_incoming", "booking_id", "booking_currency").createOrReplaceTempView("aux")
    df_impuesto_canco.createOrReplaceTempView("canco")
    df_addcanco.createOrReplaceTempView("add_canco")

    df_r = manager.session.sql(tables["tax_transfer_pricing"])

    df_res = df_fields.join(df_r, ["operative_incoming", "booking_id"]) \
        .select(df_fields.operative_incoming, df_fields.booking_id, df_fields.creation_date,
                df_fields.booking_currency, df_r.tax_transfer_pricing)

    df_res.cache()

    return df_res


def sub_tax_transfer_pricing_eur(manager, df_fields):
    """
    It calculate the field tax_transfer_pricing_eur and rounds both tax_transfer_pricing
    :param manager: 
    :param table: 
    :return: dataframe
    """
    # mylist = df_fields.collect()
    # newlist = []
    # for row in mylist:
    #     new_row = []
    #     for field in row:
    #         new_row.append(field)
    #     ccy = new_row[-2]
    #     amount = new_row[-1]
    #     new_row[4] = round_ccy(amount, ccy)
    #     pricing_eur = round_ccy(exchange_amount_decimal(new_row[3], EUR, new_row[2], new_row[4]), uEUR)
    #     final = [new_row[0], new_row[1], new_row[4], pricing_eur]
    #     newlist.append(final)
    #
    # schema = StructType([StructField("operative_incoming", IntegerType()),
    #                      StructField("booking_id", IntegerType()),
    #                      StructField("tax_transfer_pricing", FloatType()),
    #                      StructField("tax_transfer_pricing_eur", FloatType())])
    #
    # df_fields2 = manager.session.createDataFrame(newlist, schema)
    df_fields1 = df_fields.withColumn('tax_transfer_pricing', udf_round_ccy(df_fields.tax_transfer_pricing,
                                                                            df_fields.booking_currency))

    df_fields2 = df_fields1.withColumn('tax_transfer_pricing_eur',
                                       udf_round_ccy(udf_currency_exchange(df_fields1.booking_currency,
                                                                           func.lit(EUR),
                                                                           df_fields1.creation_date,
                                                                           df_fields1.tax_transfer_pricing),
                                                     func.lit(uEUR)))

    df_fields2 = df_fields2.na.fill({'tax_transfer_pricing_eur': 0}).select("operative_incoming", "booking_id",
                                                                            "tax_transfer_pricing",
                                                                            "tax_transfer_pricing_eur")

    df_fields2.cache()

    d = df_fields2.collect()

    return df_fields2


def join_all_fields(df_query, df_fields2, df_fields4, df_fields6):
    df_r1 = df_query.join(df_fields2, [df_query.operative_incoming == df_fields2.operative_incoming,
                                       df_query.booking_id == df_fields2.booking_id]) \
        .drop(df_fields2.operative_incoming).drop(df_fields2.booking_id)

    df_r2 = df_r1.join(df_fields4, [df_r1.operative_incoming == df_fields4.operative_incoming,
                                    df_r1.booking_id == df_fields4.booking_id]) \
        .drop(df_fields4.operative_incoming).drop(df_fields4.booking_id)

    df_r3 = df_r2.join(df_fields6, [df_r2.operative_incoming == df_fields6.operative_incoming,
                                    df_r2.booking_id == df_fields6.booking_id]) \
        .drop(df_fields6.operative_incoming).drop(df_fields6.booking_id)

    df_r3.cache()

    return df_r3


def change_date_type(dataframe):
    dataframe = dataframe.withColumn("creation_date", dataframe.creation_date.cast("string"))
    dataframe = dataframe.withColumn("modification_date", dataframe.modification_date.cast("string"))
    dataframe = dataframe.withColumn("cancellation_date", dataframe.cancellation_date.cast("string"))
    dataframe = dataframe.withColumn("status_date", dataframe.status_date.cast("string"))
    dataframe = dataframe.withColumn("booking_service_from", dataframe.booking_service_from.cast("string"))
    dataframe = dataframe.withColumn("booking_service_to", dataframe.booking_service_to.cast("string"))
    dataframe = dataframe.withColumn("creation_ts", dataframe.creation_ts.cast("string"))
    dataframe = dataframe.withColumn("first_booking_ts", dataframe.creation_ts.cast("string"))

    return dataframe


def remove_fields(dataframe):
    dataframe = dataframe.drop("Tax_Sales_Transfer_pricing") \
        .drop("Tax_Sales_Transfer_pricing_EUR") \
        .drop("tax_transfer_pricing") \
        .drop("tax_transfer_pricing_eur") \
        .drop("Tax_Cost_Transfer_pricing") \
        .drop("Tax_Cost_Transfer_pricing_EUR")

    return dataframe


def select_columns_ordered(df_fields):
    return df_fields.select("interface_id", "operative_company", "operative_office", "operative_office_desc",
                            "operative_incoming", "booking_id", "interface", "invoicing_company", "invoicing_office",
                            "invoicing_incoming", "creation_date", "creation_ts", "first_booking_ts",
                            "modification_date", "modification_ts", "cancellation_date", "cancellation_ts",
                            "cancelled_booking", "status_date", "booking_service_from", "booking_service_to",
                            "client_code", "customer_name", "source_market", "source_market_iso", "holder",
                            "num_adults", "num_childrens", "department_code", "booking_type", "invoicing_booking",
                            "invoicing_admin", "client_commision_esp", "client_override_esp", "confirmed_booking",
                            "partner_booking", "partner_booking_currency", "partner_code", "partner_brand",
                            "partner_agency_code", "partner_agency_brand", "booking_file_incoming",
                            "booking_file_number", "accomodation_model", "destination_code", "booking_currency",
                            "ttv_booking_currency", "tax_ttv", "tax_ttv_toms", "tax_transfer_pricing",
                            "client_commision", "tax_client_commision", "client_rappel", "tax_client_rappel",
                            "cost_booking_currency", "tax_cost", "tax_cost_toms", "ttv_eur_currency", "tax_ttv_eur",
                            "tax_ttv_eur_toms", "tax_transfer_pricing_eur", "client_eur_commision",
                            "tax_client_eur_commision", "client_eur_rappel", "tax_client_eur_rappel",
                            "cost_eur_currency", "tax_cost_eur", "tax_cost_eur_toms", "application", "canrec_status",
                            "gsa_commision", "tax_gsa_commision", "gsa_eur_commision", "tax_gsa_eur_commision",
                            "agency_commision_hotel_payment", "tax_agency_commision_hotel_pay",
                            "fix_override_hotel_payment", "tax_fix_override_hotel_pay", "var_override_hotel_payment",
                            "tax_var_override_hotel_pay", "hotel_commision_hotel_payment",
                            "tax_hotel_commision_hotel_pay", "marketing_contribution", "tax_marketing_contribution",
                            "bank_expenses", "tax_bank_expenses", "platform_fee", "tax_platform_fee", "credit_card_fee",
                            "tax_credit_card_fee", "withholding", "tax_withholding", "local_levy", "tax_local_levy",
                            "partner_third_commision", "tax_partner_third_commision", "agency_comm_hotel_pay_eur",
                            "tax_agency_comm_hotel_pay_eur", "fix_override_hotel_pay_eur",
                            "tax_fix_overr_hotel_pay_eur", "var_override_hotel_pay_eur",
                            "tax_var_override_hotel_pay_eur", "hotel_commision_hotel_pay_eur",
                            "tax_hotel_comm_hotel_pay_eur", "marketing_contribution_eur",
                            "tax_marketing_contribution_eur", "bank_expenses_eur", "tax_bank_expenses_eur",
                            "platform_fee_eur", "tax_platform_fee_eur", "credit_card_fee_eur",
                            "tax_credit_card_fee_eur", "withholding_eur", "tax_withholding_eur", "local_levy_eur",
                            "tax_local_levy_eur", "partner_third_comm_eur", "tax_partner_third_comm_eur",
                            "number_active_acc_serv",
                            "tax_sales_transfer_pricing", "tax_cost_transfer_pricing",
                            "tax_sales_transfer_pricing_eur", "tax_cost_transfer_pricing_eur",
                            "cod_credit_type",
                            "cod_tag_nationality", "name_ref_age", "cod_agent")
