# -*- coding: utf-8 -*-
"""
Created on Mon Apr 13 15:28:49 2020

@author: aagils
"""
import numpy as np
import pyodbc
import pandas as pd
import datetime as dt
from datetime import date

class GetData:
    
    """
    Método para ejecutar consultas y obtener datos desde cloudera
    """
    
    @staticmethod
    def ImpalaConector():
        
        """
        Instancia de la clase para crear conexión con el servidor
        """
        
        impala_connection = pyodbc.connect('DSN=Impala', autocommit=True)
        cursor = impala_connection.cursor()
        return cursor


    @staticmethod
    def CursorExecute(texto):
        
        """
        texto: string de SQL
        """
        
        con = GetData.ImpalaConector()
        cursor = con.execute(texto)
        return None
    
    @staticmethod
    def Dates(n):

        today = date.today()
        today_t_5 = today - dt.timedelta(n)
        pr_fecha_desde = today_t_5.strftime('%Y%m%d')
        pr_fecha_hasta = today.strftime('%Y%m%d')
        return pr_fecha_desde, pr_fecha_hasta

    @staticmethod
    def DataFrame(texto):
        
        """
        texto: string de SQL
        df: DataFrame con el contenido de la query texto
        """
        
        con = GetData.ImpalaConector()
        cursor = con.execute(texto)
        names = [metadata[0] for metadata in cursor.description]
        df = pd.DataFrame([dict(zip(names, row)) for row in cursor], columns=names)
        return df
    
    @staticmethod
    def QueryIterator(df,query):
        
        """
        df: DataFrame con columnas a iterar
        query: string de SQL
        CrossSell: DataFrame con la consulta por item de lista
        """
        CrossSell = pd.DataFrame()

        for i in range(0,len(df)):
            
            cluster = df.iloc[i].to_numpy()[0]
            familia = df.iloc[i].to_numpy()[1]
            cross = GetData.DataFrame(query.format(cluster,familia))
            c = pd.Series([cluster for x in range(cross.shape[0])], 
                          name = 'cluster')
            f = pd.Series([familia for x in range(cross.shape[0])], 
              name = 'familia')
            ClusterFamilia = pd.concat([c,f], axis = 1)
            crossSelling = pd.concat([ClusterFamilia, cross], axis = 1)
            CrossSell = CrossSell.append(crossSelling)
            print(i)
        
        return CrossSell


GetData.CursorExecute("""
                DROP TABLE IF EXISTS sbxrtlsodarmktcom.tmp_perf;
                      """)
tmp_perf = """
CREATE TABLE sbxrtlsodarmktcom.tmp_perf AS 
SELECT regexp_replace(sales_return_dt,'-','') AS fecha_tk, 
		transaction_dttm,
       sales_return_document_num AS tk,
	   location_id AS tienda,
       regexp_replace(
            regexp_replace(
                regexp_replace(
                    regexp_replace(
                        regexp_replace(
                            regexp_replace(
                                regexp_replace(
                                    regexp_replace(
                                        regexp_replace(
                                        IF( length(
                                            cast(cast(
                                                regexp_replace(regexp_replace(customer_id,'[[:alpha:]]|_', ''), '[!-.-=^]', '') 
                                            AS BIGINT) AS string)) IN (7,8,11), 
                                                cast(cast(
                                                    regexp_replace(regexp_replace(customer_id,'[[:alpha:]]|_', ''), '[!-.-=^]', '') 
                                                AS BIGINT) AS string), '')
                                        ,'11111+','')
                                    ,'22222+','')
                                ,'33333+','')
                            ,'44444+','') 
                        ,'55555+','')
                    ,'666666+','')
                ,'77777+','')
            ,'88888+','') 
        ,'99999+','') 
        AS numero_documento,
		item_id AS sku,
        unit_cnt AS cantidad,
        transaction_unit_price_amt AS precio_venta,
		(unit_cnt*transaction_unit_price_amt) AS monto,
		main_payment_type_cd AS tipo_comprobante
FROM sod_onehouse_exp.vw_soar_sales_return_fact
WHERE regexp_replace(sales_return_dt,'-','') BETWEEN '{}' AND '{}'
      AND main_payment_type_cd <> ''
	  AND main_payment_type_cd is not NULL
	  AND transaction_unit_price_amt >= 2
	  AND unit_cnt >= 1;
            """
GetData.CursorExecuteI(tmp_perf.format())

GetData.CursorExcecute("""
        DROP TABLE IF EXISTS sbxrtlsodarmktcom.tmp_perf_1;
                       """)
                       
GetData.CursorExcecute("""
CREATE TABLE sbxrtlsodarmktcom.tmp_perf_1 AS 
SELECT t1.*,
t2.hier_department_name AS departamento,
t2.hier_family_name AS familia,
t2.hier_subfamily_name AS subfamilia,
t2.hier_group_name AS grupo,
t2.hier_set_name AS conjunto
FROM sbxrtlsodarmktcom.tmp_perf t1
INNER JOIN sod_onehouse_exp.vw_soar_item_dim t2
ON t1.sku = t2.item_id
WHERE length(numero_documento) IN (7,8,11)
AND tienda <>5001;
    """)

GetData.CursorExecute("""
    DROP TABLE IF EXISTS sbxrtlsodarmktcom.tmp_perf_2;
                      """)

GetData.CursorExcecute("""
CREATE TABLE sbxrtlsodarmktcom.tmp_perf_2 AS
SELECT t1.*,
		t2.segmento,
		t2.segmento_actividad
FROM sbxrtlsodarmktcom.tmp_perf_1 t1
INNER JOIN (SELECT * FROM 
			sbxrtlsodarmktcom.soar_perfilamiento_tablero_rfm
			WHERE periodo = (SELECT MAX(periodo)
								FROM sbxrtlsodarmktcom.soar_perfilamiento_tablero_rfm)
			AND segmento_actividad <> '0') t2
ON t1.numero_documento = t2.numdoc;
                       """)
                       
                     
#####################  PRO
GetData.CursorExecute("""
            DROP TABLE IF EXISTS sbxrtlsodarmktcom.perf_pro;
                      """)  
                      
GetData.CursorExcute("""
CREATE TABLE sbxrtlsodarmktcom.perf_pro AS
SELECT t1.*,
	   t2.descr AS cluster
FROM sbxrtlsodarmktcom.tmp_perf_2 t1
INNER JOIN sbxrtlsodarmktcom.soar_mapeo_pro t2
ON t1.segmento_actividad = t2.segmento_actividad
WHERE t1.segmento = 'PRO';
                     """)
                     
GetData.CursorExcecute("""
            DROP TABLE IF EXISTS sbxrtlsodarmktcom.fams_pro;
                       """)
                       
GetData.CursorExecute("""
CREATE TABLE sbxrtlsodarmktcom.fams_pro AS
SELECT * FROM 
(SELECT *, Row_Number()
			over (PARTITION BY cluster
					ORDER BY q DESC) AS rnk
FROM
(SELECT cluster, familia,
			COUNT(DISTINCT numero_documento) AS q,
			SUM(monto) AS monto,
			AVG(monto) AS monto_promedio
FROM sbxrtlsodarmktcom.perf_pro
GROUP BY cluster, familia) a) b
WHERE rnk < 6
ORDER BY cluster;
                    """)

search_familia = GetData.DataFrame("""
                               select cluster, familia
                               from sbxrtlsodarmktcom.fams_pro;
                               """)
ProSubfamilia = """
SELECT subfamilia,
			COUNT(DISTINCT numero_documento) AS q,
			SUM(monto) AS monto,
			AVG(monto) AS monto_promedio
FROM sbxrtlsodarmktcom.perf_pro
WHERE cluster = '{}'
AND familia = '{}'
GROUP BY subfamilia
limit 10
        """
pro_subfamilia = GetData.QueryIterator(search_familia,ProSubfamilia)

search_subfamilia = pd.concat([pro_subfamilia['cluster'],
                               pro_subfamilia['subfamilia']],
                              axis = 1).reset_index().drop('index', axis = 1)

ProSet = """
SELECT conjunto,
			COUNT(DISTINCT numero_documento) AS q,
			SUM(monto) AS monto,
			AVG(monto) AS monto_promedio
FROM sbxrtlsodarmktcom.perf_pro
WHERE cluster = '{}'
AND subfamilia = '{}'
GROUP BY conjunto
limit 10
        """
pro_set = GetData.QueryIterator(search_subfamilia,ProSet)

ProSku = """
select t2.item_name,
        t1.q,
        t1.monto,
        t1.monto_promedio
from
(SELECT sku,
			COUNT(DISTINCT numero_documento) AS q,
			SUM(monto) AS monto,
			AVG(monto) AS monto_promedio
FROM sbxrtlsodarmktcom.perf_pro
WHERE cluster = '{}'
AND conjunto = '{}'
GROUP BY sku
limit 10) t1
inner join sod_onehouse_exp.vw_soar_item_dim t2
on t1.sku = t2.item_id
            """
            
search_set = pd.concat([pro_set['cluster'],
                               pro_set['conjunto']],
                              axis = 1).reset_index().drop_duplicates().drop('index', axis = 1)

pro_sku = GetData.QueryIterator(search_set,ProSku)

name = 'PRO SKUS POR CONJUNTO.xlsx'
writer = pd.ExcelWriter(name, engine='xlsxwriter')
pro_sku.to_excel(writer, sheet_name='SKUS_CONJUNTO',index=False)
writer.save()


#####################  HOME

GetData.CursorExecute("""
            DROP TABLE IF EXISTS sbxrtlsodarmktcom.perf_home;
                      """)  
                      
GetData.CursorExcute("""
CREATE TABLE sbxrtlsodarmktcom.perf_home AS
SELECT t1.*,
	   t2.descr AS cluster
FROM sbxrtlsodarmktcom.tmp_perf_2 t1
INNER JOIN sbxrtlsodarmktcom.soar_mapeo_home t2
ON t1.segmento_actividad = t2.segmento_actividad
WHERE t1.segmento = 'HOME';
                     """)
                     
GetData.CursorExcecute("""
            DROP TABLE IF EXISTS sbxrtlsodarmktcom.fams_home;
                       """)
                       
GetData.CursorExecute("""
CREATE TABLE sbxrtlsodarmktcom.fams_home AS
SELECT * FROM 
(SELECT *, Row_Number()
			over (PARTITION BY cluster
					ORDER BY q DESC) AS rnk
FROM
(SELECT cluster, familia,
			COUNT(DISTINCT numero_documento) AS q,
			SUM(monto) AS monto,
			AVG(monto) AS monto_promedio
FROM sbxrtlsodarmktcom.perf_home
GROUP BY cluster, familia) a) b
WHERE rnk < 6
ORDER BY cluster;
                    """)

search_familia = GetData.DataFrame("""
                               select cluster, familia
                               from sbxrtlsodarmktcom.fams_home;
                               """)
HomeSubfamilia = """
SELECT subfamilia,
			COUNT(DISTINCT numero_documento) AS q,
			SUM(monto) AS monto,
			AVG(monto) AS monto_promedio
FROM sbxrtlsodarmktcom.perf_home
WHERE cluster = '{}'
AND familia = '{}'
GROUP BY subfamilia
limit 10
        """

home_subfamilia = GetData.QueryIterator(search_familia,HomeSubfamilia)

search_subfamilia = pd.concat([home_subfamilia['cluster'],
                               home_subfamilia['subfamilia']],
                              axis = 1).reset_index().drop_duplicates().drop('index', axis = 1)


HomeSet = """
SELECT conjunto,
			COUNT(DISTINCT numero_documento) AS q,
			SUM(monto) AS monto,
			AVG(monto) AS monto_promedio
FROM sbxrtlsodarmktcom.perf_home
WHERE cluster = '{}'
AND subfamilia = '{}'
GROUP BY conjunto
limit 10
        """
home_set = GetData.QueryIterator(search_subfamilia,HomeSet)


HomeSku = """
select t2.item_name,
        t1.q,
        t1.monto,
        t1.monto_promedio
from
(SELECT sku,
			COUNT(DISTINCT numero_documento) AS q,
			SUM(monto) AS monto,
			AVG(monto) AS monto_promedio
FROM sbxrtlsodarmktcom.perf_home
WHERE cluster = '{}'
AND conjunto = '{}'
GROUP BY sku
limit 10) t1
inner join sod_onehouse_exp.vw_soar_item_dim t2
on t1.sku = t2.item_id
            """
            
search_set = pd.concat([home_set['cluster'],
                               home_set['conjunto']],
                              axis = 1).reset_index().drop_duplicates().drop('index', axis = 1)

home_sku = GetData.QueryIterator(search_set,HomeSku)
home_sku['monto_promedio'] = pd.to_numeric(home_sku['monto_promedio'])


skus_ = GetData.DataFrame("""
                          SELECT DISTINCT item_id, item_name 
FROM sod_onehouse_exp.vw_soar_item_dim;
                          """)

name = 'HOME SKUS POR CONJUNTO.xlsx'
writer = pd.ExcelWriter(name, engine='xlsxwriter')
home_sku.to_excel(writer, sheet_name='SKUS_CONJUNTO',index=False)
skus_.to_excel(writer, sheet_name='SKUS_LISTA',index=False)
writer.save()


