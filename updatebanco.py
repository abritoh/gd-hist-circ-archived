# -*- coding: utf-8 -*-
"""
-- ============================================================================
-- Descripción....: Actualiza el campo <numero_de_transformador>
                    en [apcc.catalogo_equipos]
   
-- Params.........: ?division=DK&nombre_equipo_simoce=AAA4025
-- Service Result.: xML => <numero_de_transformador>1</numero_de_transformador>
-- Elabora........: ArBR (arcebrito@gmail.com)
-- Fecha..........: 2019-02-05
-- ============================================================================
"""

import re
import time
import utlpy
import pymysql
import requests

PR_NAME = "updatebanco"
CN = {"host": "10.4.22.84", "user":  "ClusterBR", "passwd": "XYZ...", "database": "apcc"}
SERVICE_URL = "http://10.14.3.8/calidad/SIMOCE-NACIONAL/Proyectos/SIAPCC/Catalogo_Equipos.jsp"

def fn_get_numero_de_transformador (clave_division, nombre_equipo_simoce):
    result = ""    
    params = {"division": clave_division, "nombre_equipo_simoce": nombre_equipo_simoce} 
    obj_request = requests.get(url = SERVICE_URL, params = params) 
    xml_raw = obj_request.text    
    regex = "<numero_de_transformador>(.*?)</numero_de_transformador>"
    match = re.search(regex, xml_raw)
    if match:
        numero_de_tr_str = (xml_raw.split('<numero_de_transformador>')[1]).split('</numero_de_transformador>')[0]
        result = "0" + numero_de_tr_str
        
    print("fn_get_numero_de_transformador: {} {} => {}\n".format(clave_division, nombre_equipo_simoce, result))
    return result


def fn_update_numero_de_transformador() :
    start_time = time.time()
    prid = utlpy.btc_gen_prid(PR_NAME, "*")

    connection = None
    cursor = None
    try:        
        connection = pymysql.connect(host=CN["host"], user=CN["user"], passwd=CN["passwd"], database=CN["database"], autocommit=True)        
        utlpy.btc_insert(connection, prid, PR_NAME, "INICIADO", "", "*")
        
        utlpy.println("fn_update_numero_de_transformador: started")
        sql_select = """ SELECT CLAVE_DIVISION, NOMBRE_EQUIPO_SIMOCE 
                          FROM APCC.CATALOGO_EQUIPOS CE 
                          WHERE TIPO_EQUIPO='TRANSFORMADOR';"""
                    
        sql_update = """ UPDATE APCC.CATALOGO_EQUIPOS SET NUMERO_DE_TRANSFORMADOR=%S
                            WHERE CLAVE_DIVISION=%S AND NOMBRE_EQUIPO_SIMOCE=%S """

        cursor = pymysql.cursors.Cursor(connection)
        cursor.execute(sql_select)
        for x in cursor.fetchall() :
            clave_division = x[0]
            nombre_equipo_simoce = x[1]            
            numero_de_transformador = fn_get_numero_de_transformador(clave_division, nombre_equipo_simoce)
            params_update = (numero_de_transformador, clave_division, nombre_equipo_simoce)
            cursor.execute(sql_update, params_update)
            connection.commit()
        
        elapsed_time = time.time() - start_time
        elapsed_time_fmt = time.strftime("%H:%M:%S", time.gmtime(elapsed_time))
        utlpy.println("fn_update_numero_de_transformador.completed. Total time: {}".format(elapsed_time_fmt))
        message = "DURACION:{}".format(str(elapsed_time_fmt))
        utlpy.btc_insert(connection, prid, PR_NAME, "COMPLETADO", message, "*")
        
    except Exception as e:
        utlpy.println('fn_update_numero_de_transformador.error >>> {}'.format(str(e)))
        if connection:
            utlpy.btc_insert(connection, prid, PR_NAME, "ERROR", str(e), "*")
    finally:        
        if cursor:
            cursor.close()
        if connection:
            connection.close()
    return


##########################
# main
##########################

if __name__ == '__main__' :
    fn_update_numero_de_transformador()
    
    