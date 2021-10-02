# -*- coding: utf-8 -*-
"""
Created on Fri May  8 10:25:34 2020

@author: aagils
"""

import numpy as np
import pyodbc
import pandas as pd
import os


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
    def Distr(df):

    	df['porc'] = df['q'] / df.sum()['q']
    	return df
    
    @staticmethod
    def QueryIterator(lista,query):
        
        """
        lista: lista sobre la cual se itera
        query: string SQL
        CrossSell: DataFrame con la consulta por item de lista
        """
        CrossSell = pd.DataFrame()
        for i in lista:
                        
            cross = GetData.DataFrame(query.format(i))
            fam = [i for x in range(cross.shape[0])]
            crossSelling = pd.concat([pd.Series(fam, name = 'bucket'), cross], axis = 1)
            CrossSell = CrossSell.append(crossSelling)
            print(i)
        
        return CrossSell
    
    @staticmethod
    def QueryIterator1(lista,query,familia):
        
        """
        lista: lista sobre la cual se itera
        query: string SQL
        CrossSell: DataFrame con la consulta por item de lista
        """
        CrossSell = pd.DataFrame()
        for i in lista:
                        
            cross = GetData.DataFrame(query.format(i,familia))
            fam = [i for x in range(cross.shape[0])]
            crossSelling = pd.concat([pd.Series(fam, name = 'bucket'), cross], axis = 1)
            CrossSell = CrossSell.append(crossSelling)
            print(i)
        
        return CrossSell
    
    @staticmethod
    def QueryIterator2(df,query):
        
        """
        df: DataFrame con columnas a iterar
        query: string de SQL
        CrossSell: DataFrame con la consulta por item de lista
        """
        CrossSell = pd.DataFrame()

        for i in range(0,len(df)):
            
            bucket = df.iloc[i].to_numpy()[0]
            familia = df.iloc[i].to_numpy()[1]
            
            cross = GetData.DataFrame(query.format(bucket,familia))
            b = pd.Series([bucket for x in range(cross.shape[0])], 
                          name = 'bucket')
            f = pd.Series([familia for x in range(cross.shape[0])], 
              name = 'familia')
            BucketTiendaFamilia = pd.concat([b,f], axis = 1)
            crossSelling = pd.concat([BucketTiendaFamilia, cross], axis = 1)
            CrossSell = CrossSell.append(crossSelling)
            print(i,"de ",len(df)-1)
        
        return CrossSell

####### QUERYS

####### GetData.CursorExecute 

base = """
CREATE TABLE sbxrtlsodarmktcom.tmp_trx AS 
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
    AND 
	channel_sales_flg <> 'VE'
	AND
	sales_return_type_cd IN ('blta', 'fctr') 
    AND 
	st_extended_net_amt > 2 
	AND 
	unit_cnt >= 1
	ORDER BY sales_return_dt, customer_id;
        """

base_1 = """
        CREATE TABLE sbxrtlsodarmktcom.tmp_trx_1 AS 
            SELECT t1.*,
            t2.hier_department_name AS departamento,
            t2.hier_family_name AS familia,
            t2.hier_subfamily_name AS subfamilia,
            t2.hier_group_name AS grupo,
            t2.hier_set_name AS conjunto
            FROM sbxrtlsodarmktcom.tmp_trx t1
            INNER JOIN sod_onehouse_exp.vw_soar_item_dim t2
            ON t1.sku = t2.item_id;
        """

categoria = """
        CREATE TABLE sbxrtlsodarmktcom.tmp_cat AS
            SELECT * FROM sbxrtlsodarmktcom.tmp_trx_1
            WHERE numero_documento IN (SELECT DISTINCT numero_documento 
            							FROM sbxrtlsodarmktcom.tmp_trx_1
            							WHERE familia = '{}');
            """

check_cat = """
            CREATE TABLE sbxrtlsodarmktcom.fam_cat AS 
                SELECT *,
                		CASE WHEN FAMILIA = '{}'
                		THEN 'COMPRO {}'
                		ELSE 'NO COMPRO {}'
                		END AS check_categoria
                FROM sbxrtlsodarmktcom.tmp_trx_1;
            """
cat_compra = """
            CREATE TABLE sbxrtlsodarmktcom.fam_cat_1 AS 
                SELECT t1.*, 
                        t2.monto AS monto_cat
                FROM (SELECT numero_documento, familia, SUM(monto) AS monto
                FROM sbxrtlsodarmktcom.tmp_trx_1
                WHERE length(numero_documento) IN (7,8,11)
                GROUP BY numero_documento, familia
                ORDER BY numero_documento) t1
                LEFT JOIN (
                SELECT numero_documento, familia, SUM(monto) AS monto
                FROM sbxrtlsodarmktcom.tmp_trx_1
                WHERE familia = '{}'
                AND length(numero_documento) IN (7,8,11)
                GROUP BY numero_documento, familia
                ORDER BY numero_documento) t2
                ON t1.numero_documento = t2.numero_documento
                ORDER BY t1.numero_documento;
            """
            
ap_monto = """
CREATE TABLE sbxrtlsodarmktcom.ap_monto AS 
SELECT *,
	CASE 
		WHEN monto < 2500 THEN '< $2.500'
		WHEN (monto >= 2500 AND monto < 5000) THEN '$2.500 - $5.000'
		WHEN (monto >= 5000 AND monto < 10000) THEN '$5.000 - $10.000'
		ELSE '$10.000+'
		END AS bucket_consumo
FROM 	(SELECT numero_documento,
		SUM(monto) AS monto
FROM sbxrtlsodarmktcom.tmp_cat
GROUP BY numero_documento) a;
            """
 
####### GetData.DataFrame   
                                            
graf_1 = """
        SELECT t1.*,
		t2.compro_cat
FROM (SELECT familia, COUNT(DISTINCT numero_documento) AS q 
FROM sbxrtlsodarmktcom.fam_cat_1
GROUP BY familia
ORDER BY familia) t1
INNER JOIN (SELECT familia, COUNT(CASE WHEN monto_cat is not NULL
					  THEN numero_documento end) AS compro_cat
FROM sbxrtlsodarmktcom.fam_cat_1
GROUP BY familia
ORDER BY familia) t2
ON t1.familia = t2.familia
where t1.familia not in  ('{}','PROMOCIONES - SOPORTE TECNICO'); 
        """
       
cs_gral_familia = """
        SELECT familia, 
        		COUNT(DISTINCT numero_documento) AS q,
        		AVG(monto) AS monto_promedio
        FROM sbxrtlsodarmktcom.tmp_cat
        WHERE familia not IN ('{}','PROMOCIONES - SOPORTE TECNICO')
        GROUP BY familia
        ORDER BY COUNT(DISTINCT numero_documento) DESC
        limit 5;
                """

distr_sku = """
        select tramo, sum(q) as q
        from
        (select   CASE
		WHEN skus <= 2 THEN '1-2'
		WHEN (skus > 2 AND skus <=5) THEN '3-5'
        WHEN (skus > 5 AND skus <=10) THEN '6-10'
        WHEN (skus > 10 AND skus <=15) THEN '11-15'
        WHEN (skus > 15 AND skus <=25) THEN '16-25'
        WHEN (skus > 25 AND skus <=40) THEN '26-40'
        WHEN (skus > 40 AND skus <=60) THEN '41-60'
        WHEN (skus > 60 AND skus <=100) THEN '61-100'
		ELSE '>100'
		END AS tramo, q
        FROM
        (SELECT skus, 
        		COUNT(skus) AS q 
        FROM
        (SELECT numero_documento,
        		COUNT(DISTINCT sku) AS skus
        FROM sbxrtlsodarmktcom.tmp_cat
        WHERE familia = '{}'
        AND length(numero_documento) IN (7,8,11)
        GROUP BY numero_documento) a
        GROUP BY skus) b) c
        group by tramo
            """
####### Querys CrossSell
# General subfamilia. Itera por el top 5 de familias de cs_gral_familia
dinam_subfamilia = """
 select subfamilia,
        count(distinct numero_documento) as q,
        avg(monto) as monto_promedio
from sbxrtlsodarmktcom.tmp_cat
where familia = '{}'
and subfamilia <> 'INSUMOS'
group by subfamilia
order by count(distinct numero_documento) desc
limit 10; 
                     """           
            
# Familia por tienda y Nivel de Gasto
dinam_familia = """
SELECT * FROM (SELECT *, Row_Number()
		  OVER (PARTITION BY tienda
		  		ORDER BY q DESC) AS rnk
FROM (SELECT CASE tienda
		WHEN 5011 THEN 'HC MALVINAS'                                                                                        
		WHEN 5012 THEN 'HC SAN MARTIN'                                                                                      
		WHEN 5014 THEN 'HC SAN JUSTO'                                                                                       
		WHEN 5016 THEN 'HC VILLA TESEI'                                                                                     
		WHEN 5017 THEN 'HC VICENTE LOPEZ'                                                                                   
		WHEN 5019 THEN 'HC TORTUGUITA'                                                                                      
		WHEN 5021 THEN 'VENTA A DISTANCIA'                                                                                  
		WHEN 5022 THEN 'HC LA PLATA'                                                                                        
		WHEN 5023 THEN 'HC CORDOBA'                                                                                         
		WHEN 5018 THEN 'HC ADROGUE'   
		ELSE ''
		END AS tienda,familia,
		COUNT(DISTINCT numero_documento) AS q,
		AVG(monto) AS monto_promedio
FROM sbxrtlsodarmktcom.tmp_cat
WHERE numero_documento IN
					(SELECT numero_documento
						FROM sbxrtlsodarmktcom.ap_monto
						WHERE bucket_consumo = '{}')
AND familia not IN  ('{}','PROMOCIONES - SOPORTE TECNICO')
GROUP BY tienda, familia
ORDER BY tienda) a) b
WHERE rnk < 6
ORDER BY tienda;
                """
                
dinam_subfamilia_byfam = """
SELECT * FROM (SELECT *, Row_Number()
		  OVER (PARTITION BY tienda
		  		ORDER BY q DESC) AS rnk
FROM (SELECT CASE tienda
		WHEN 5011 THEN 'HC MALVINAS'                                                                                        
		WHEN 5012 THEN 'HC SAN MARTIN'                                                                                      
		WHEN 5014 THEN 'HC SAN JUSTO'                                                                                       
		WHEN 5016 THEN 'HC VILLA TESEI'                                                                                     
		WHEN 5017 THEN 'HC VICENTE LOPEZ'                                                                                   
		WHEN 5019 THEN 'HC TORTUGUITA'                                                                                      
		WHEN 5021 THEN 'VENTA A DISTANCIA'                                                                                  
		WHEN 5022 THEN 'HC LA PLATA'                                                                                        
		WHEN 5023 THEN 'HC CORDOBA'                                                                                         
		WHEN 5018 THEN 'HC ADROGUE'   
		ELSE ''
		END AS tienda,subfamilia,
		COUNT(DISTINCT numero_documento) AS q,
		AVG(monto) AS monto_promedio
FROM sbxrtlsodarmktcom.tmp_cat
WHERE numero_documento IN
					(SELECT numero_documento
						FROM sbxrtlsodarmktcom.ap_monto
						WHERE bucket_consumo = '{}')
AND familia = "{}"
GROUP BY tienda, subfamilia
ORDER BY tienda) a) b
WHERE rnk < 6
ORDER BY tienda;
                    """
                    
dinam_conjunto_bysubfam = """
SELECT * FROM (SELECT *, Row_Number()
		  OVER (PARTITION BY tienda
		  		ORDER BY q DESC) AS rnk
FROM (SELECT CASE tienda
		WHEN 5011 THEN 'HC MALVINAS'                                                                                        
		WHEN 5012 THEN 'HC SAN MARTIN'                                                                                      
		WHEN 5014 THEN 'HC SAN JUSTO'                                                                                       
		WHEN 5016 THEN 'HC VILLA TESEI'                                                                                     
		WHEN 5017 THEN 'HC VICENTE LOPEZ'                                                                                   
		WHEN 5019 THEN 'HC TORTUGUITA'                                                                                      
		WHEN 5021 THEN 'VENTA A DISTANCIA'                                                                                  
		WHEN 5022 THEN 'HC LA PLATA'                                                                                        
		WHEN 5023 THEN 'HC CORDOBA'                                                                                         
		WHEN 5018 THEN 'HC ADROGUE'   
		ELSE ''
		END AS tienda,conjunto,
		COUNT(DISTINCT numero_documento) AS q,
		AVG(monto) AS monto_promedio
FROM sbxrtlsodarmktcom.tmp_cat
WHERE numero_documento IN
					(SELECT numero_documento
						FROM sbxrtlsodarmktcom.ap_monto
						WHERE bucket_consumo = '{}')
AND subfamilia = "{}"
GROUP BY tienda, conjunto
ORDER BY tienda) a) b
WHERE rnk < 6
ORDER BY tienda;
                    """

query_sku = """
SELECT tienda,skus, COUNT(*) AS q FROM
(SELECT CASE tienda
		WHEN 5011 THEN 'HC MALVINAS'                                                                                        
		WHEN 5012 THEN 'HC SAN MARTIN'                                                                                      
		WHEN 5014 THEN 'HC SAN JUSTO'                                                                                       
		WHEN 5016 THEN 'HC VILLA TESEI'                                                                                     
		WHEN 5017 THEN 'HC VICENTE LOPEZ'                                                                                   
		WHEN 5019 THEN 'HC TORTUGUITA'                                                                                      
		WHEN 5021 THEN 'VENTA A DISTANCIA'                                                                                  
		WHEN 5022 THEN 'HC LA PLATA'                                                                                        
		WHEN 5023 THEN 'HC CORDOBA'                                                                                         
		WHEN 5018 THEN 'HC ADROGUE'   
		ELSE ''
		END AS tienda, numero_documento,
		COUNT(DISTINCT sku) AS skus
FROM sbxrtlsodarmktcom.tmp_cat
WHERE length(numero_documento) in (7,8,11)
AND numero_documento IN (SELECT numero_documento
						FROM sbxrtlsodarmktcom.ap_monto
						WHERE bucket_consumo = '{}')
AND familia = '{}'
GROUP BY tienda,numero_documento) a
GROUP BY tienda,skus;
            """
            
edad = """
SELECT tienda, bucket_edad, 
		COUNT(DISTINCT numero_documento) AS q
FROM
(SELECT CASE t2.tienda
		WHEN 5011 THEN 'HC MALVINAS'                                                                                        
		WHEN 5012 THEN 'HC SAN MARTIN'                                                                                      
		WHEN 5014 THEN 'HC SAN JUSTO'                                                                                       
		WHEN 5016 THEN 'HC VILLA TESEI'                                                                                     
		WHEN 5017 THEN 'HC VICENTE LOPEZ'                                                                                   
		WHEN 5019 THEN 'HC TORTUGUITA'                                                                                      
		WHEN 5021 THEN 'VENTA A DISTANCIA'                                                                                  
		WHEN 5022 THEN 'HC LA PLATA'                                                                                        
		WHEN 5023 THEN 'HC CORDOBA'                                                                                         
		WHEN 5018 THEN 'HC ADROGUE'   
		ELSE ''
		END AS tienda,
		t1.edad, 
		CASE WHEN (edad < 20)
		THEN '0-20'
		WHEN (edad >= 20 AND edad <40) 
		THEN '20-40'
		WHEN (edad >= 40 AND edad <60)
		THEN '40-60'
		WHEN (edad >= 60 AND edad <80)
		THEN '60-80'
		WHEN (edad >= 80)
		THEN '80+'
		END AS bucket_edad, 
		t1.numero_documento
FROM sbxrtlsodarmktcom.tmp_base_clientes t1
INNER JOIN sbxrtlsodarmktcom.tmp_cat t2
ON t1.numero_documento = t2.numero_documento
WHERE t1.numero_documento IN (SELECT DISTINCT numero_documento
							FROM sbxrtlsodarmktcom.ap_monto
							WHERE bucket_consumo = '{}'
							)) a
GROUP BY tienda, bucket_edad;
       """


sexo = """
SELECT tienda, sexo, 
		COUNT(DISTINCT numero_documento) AS q
FROM
(SELECT CASE t2.tienda
		WHEN 5011 THEN 'HC MALVINAS'                                                                                        
		WHEN 5012 THEN 'HC SAN MARTIN'                                                                                      
		WHEN 5014 THEN 'HC SAN JUSTO'                                                                                       
		WHEN 5016 THEN 'HC VILLA TESEI'                                                                                     
		WHEN 5017 THEN 'HC VICENTE LOPEZ'                                                                                   
		WHEN 5019 THEN 'HC TORTUGUITA'                                                                                      
		WHEN 5021 THEN 'VENTA A DISTANCIA'                                                                                  
		WHEN 5022 THEN 'HC LA PLATA'                                                                                        
		WHEN 5023 THEN 'HC CORDOBA'                                                                                         
		WHEN 5018 THEN 'HC ADROGUE'   
		ELSE ''
		END AS tienda,
		t1.sexo, 
		t1.numero_documento
FROM sbxrtlsodarmktcom.tmp_base_clientes t1
INNER JOIN sbxrtlsodarmktcom.tmp_cat t2
ON t1.numero_documento = t2.numero_documento
WHERE t1.numero_documento IN (SELECT DISTINCT numero_documento
							FROM sbxrtlsodarmktcom.ap_monto
							WHERE bucket_consumo = '{}'
							)) a
GROUP BY tienda, sexo;
       """

####### REPORTE 
familias = ['BANOS Y COCINAS',
            'PISOS',
            'PINTURA Y ACCESORIOS']

NivelDeGasto = ['< $2.500',
'$2.500 - $5.000',
'$5.000 - $10.000',
'$10.000+']

pr_fecha_desde = '20190601'
pr_fecha_hasta = '20190831'


for fam in familias:
    
    print("Generando Reporte ", fam)

    GetData.CursorExecute("DROP TABLE IF EXISTS sbxrtlsodarmktcom.tmp_trx;")
    GetData.CursorExecute(base.format(pr_fecha_desde,pr_fecha_hasta))
    GetData.CursorExecute("DROP TABLE IF EXISTS sbxrtlsodarmktcom.tmp_trx_1;")
    GetData.CursorExecute(base_1)
    GetData.CursorExecute("DROP TABLE IF EXISTS sbxrtlsodarmktcom.tmp_cat;")
    GetData.CursorExecute(categoria.format(fam))
    GetData.CursorExecute("DROP TABLE IF EXISTS sbxrtlsodarmktcom.fam_cat;")
    GetData.CursorExecute(check_cat.format(fam,fam,fam))
    GetData.CursorExecute("DROP TABLE IF EXISTS sbxrtlsodarmktcom.fam_cat_1;")
    GetData.CursorExecute(cat_compra.format(fam))
    GetData.CursorExecute("DROP TABLE IF EXISTS sbxrtlsodarmktcom.ap_monto;")
    GetData.CursorExecute(ap_monto)
    # Estaticos
    g1 = GetData.DataFrame(graf_1.format(fam)) # grafico de compras en la familia por otra (1 de excel)
    gral_fam = GetData.DataFrame(cs_gral_familia.format(fam))
    gral_fam['monto_promedio'] = pd.to_numeric(gral_fam['monto_promedio'])
    gral_skus = GetData.DataFrame(distr_sku.format(fam))
    
    # Dinamicos
    prpal_sub = GetData.QueryIterator(gral_fam['familia'].tolist(),dinam_subfamilia)
    prpal_sub['monto_promedio'] = pd.to_numeric(prpal_sub['monto_promedio'])
    
    fam_tienda_nivelgasto = GetData.QueryIterator1(NivelDeGasto,
                                                   dinam_familia,fam)
    fam_tienda_nivelgasto['monto_promedio'] = pd.to_numeric(fam_tienda_nivelgasto['monto_promedio'])
    
    search_familia = pd.concat([fam_tienda_nivelgasto['bucket'],
                        fam_tienda_nivelgasto['familia']],
                        axis = 1).reset_index().drop('index', 
                         axis = 1).reset_index().drop_duplicates().drop('index', axis = 1)
    subfam_by_fam = GetData.QueryIterator2(search_familia,dinam_subfamilia_byfam)
    subfam_by_fam['monto_promedio'] = pd.to_numeric(subfam_by_fam['monto_promedio'])
    
    search_subfamilia = pd.concat([subfam_by_fam['bucket'],
                       subfam_by_fam['subfamilia']],
                      axis = 1).reset_index().drop_duplicates().drop('index', axis = 1)
    conjun_by_subfam = GetData.QueryIterator2(search_subfamilia,dinam_conjunto_bysubfam) #quedaste aca
    conjun_by_subfam['monto_promedio'] = pd.to_numeric(conjun_by_subfam['monto_promedio'])
    
    dinam_edad = GetData.Distr(
        GetData.QueryIterator(NivelDeGasto,edad))
    dinam_sexo = GetData.Distr(
        GetData.QueryIterator(NivelDeGasto,sexo))
    
    
    #############  Preparacion de archivo
    # familia
    fam_tienda_nivelgasto['AUX'] = 'AUX1'
    fam_tienda_nivelgasto['CAT'] = 'FAMILIA'
    fam_tienda_nivelgasto = fam_tienda_nivelgasto[['bucket','AUX','tienda','familia','q','monto_promedio','CAT']]
    
    # subfamilia
    subfam_by_fam.rename(columns={'familia':'AUX','subfamilia':'familia'}, inplace=True)
    subfam_by_fam['CAT'] = 'SUBFAMILIA'
    subfam_by_fam = subfam_by_fam[['bucket','AUX','tienda','familia','q','monto_promedio','CAT']]
    
    #conjunto
    conjun_by_subfam.rename(columns={'familia':'AUX','conjunto':'familia'}, inplace=True)
    conjun_by_subfam['CAT'] = 'CONJUNTO'
    conjun_by_subfam = conjun_by_subfam[['bucket','AUX','tienda','familia','q','monto_promedio','CAT']]
    
    #edad
    dinam_edad['AUX'] = 'AUX2'
    dinam_edad.rename(columns={'porc':'monto_promedio','bucket_edad':'familia'}, inplace=True)
    dinam_edad['CAT'] = 'EDAD'
    dinam_edad = dinam_edad[['bucket','AUX','tienda','familia','q','monto_promedio','CAT']]
    
    #sexo
    dinam_sexo['AUX'] = 'AUX3'
    dinam_sexo.rename(columns={'porc':'monto_promedio','sexo':'familia'}, inplace=True)
    dinam_sexo['CAT'] = 'SEXO'
    dinam_sexo = dinam_sexo[['bucket','AUX','tienda','familia','q','monto_promedio','CAT']]
    
    #unificado
    fam_tienda_nivelgasto = fam_tienda_nivelgasto.append(subfam_by_fam)
    fam_tienda_nivelgasto = fam_tienda_nivelgasto.append(conjun_by_subfam)
    fam_tienda_nivelgasto = fam_tienda_nivelgasto.append(dinam_edad)
    fam_tienda_nivelgasto = fam_tienda_nivelgasto.append(dinam_sexo)
    
    
    ####### EXPORT EXCEL
    print("Generando archivo ", "REPORTE_"+fam.replace(" ", "_")+'.xlsx')
    name = "REPORTE_"+fam.replace(" ", "_")+'.xlsx'
    writer = pd.ExcelWriter(name, engine='xlsxwriter')
    g1.to_excel(writer, sheet_name='Compras por categorias',index=False)
    gral_fam.to_excel(writer, sheet_name='Cross Familia',index=False)
    gral_skus.to_excel(writer, sheet_name='PDF Skus',index=False)
    prpal_sub.to_excel(writer, sheet_name='Dinam_sub_byfam',index=False)
    fam_tienda_nivelgasto.to_excel(writer, sheet_name='Fam_gasto_tienda',index=False)
    writer.save() 
    print("El archivo ", name, " ha sido escrito en ",os.getcwd())
