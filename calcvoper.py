# -*- coding: utf-8 -*-
"""
-- ============================================================================
-- Descripción....: 
	(1) Calculo de Voltaje Operativo (Threading Version):
    
-- 
-- run C:\pythonPATH\python.exe C:\scriptPATH\calcvoper.py
--

-- Elabora........: ArBR (arcebrito@gmail.com)
-- Fecha..........: 2019-01-30
---                 2019-02-27 ─ Se parametriza SP destino V1 o V2
--
-- (DEPRECATED => El cálculo de voltaje operativo se hace en [siapcc_multiprocessing.py])
--
-- ============================================================================
"""

import time
import utlpy
import pymysql
from threading import Thread

PR_NAME = "calcvoper"
CN = {"host": "10.4.22.84", "user": "ClusterBR", "passwd": "XYZ123...", "database": "apcc"}

SP_CALC_VOPER_AVG = {'V1':'sp_calcVOperAVG', 'V2': 'sp_calcVOperAVG_2'}

def execute_fn_calcVOperAVG(connection, prid, cveDivision, anio, mes, version) :
    params = (cveDivision, anio, mes)
    
    cursor = None
    try:
        cursor = pymysql.cursors.Cursor(connection)
        cursor.callproc(SP_CALC_VOPER_AVG[version], params)
        for result in cursor.fetchall() :
            print(str(result))
    except Exception as e:
        utlpy.println('execute_fn_calcVOperAVG.error {} >>> {}─{}─{}'.format(str(e), cveDivision, anio, mes))
        utlpy.btc_insert(connection, prid, PR_NAME, "ERROR", str(e), cveDivision, "*", "", "", anio, mes)
    finally:
        if cursor:
            cursor.close()
    return


def fn_calcVOperAVG_div_mes (cveDivision, anio, mes, version) :
    
    start_time = time.time()
    prid = utlpy.btc_gen_prid(PR_NAME, "{}{}{}".format(cveDivision, anio, mes))
    
    connection = None
    try:
        connection = pymysql.connect(host=CN["host"], user=CN["user"], passwd=CN["passwd"], database=CN["database"], autocommit=True)        
        utlpy.btc_insert(connection, prid, PR_NAME, "INICIADO", "", cveDivision, "*", "", "", anio, mes)
        
        str_cve = "{} {} {}".format(cveDivision, anio, mes)
        utlpy.println("fn_calcVOperAVG_div_mes: {} thread started & running (...)".format(str_cve))
        
        execute_fn_calcVOperAVG(connection, prid, cveDivision, anio, mes, version)        
        
        elapsed_time = time.time() - start_time
        elapsed_time_fmt = time.strftime("%H:%M:%S", time.gmtime(elapsed_time))
        
        message = "DURACION {}".format(str(elapsed_time_fmt))
        utlpy.println("fn_calcVOperAVG_div_mes: {} thread completed. Total time: {}".format(str_cve, elapsed_time_fmt))
        utlpy.btc_insert(connection, prid, PR_NAME, "COMPLETADO", message, cveDivision, "*", "", "", anio, mes)
        
    except Exception as e:
        utlpy.println('fn_calcVOperAVG_div_mes.error {} >>> {}'.format(str_cve, str(e)))
        if connection:
            utlpy.btc_insert(connection, prid, PR_NAME, "ERROR", str(e), cveDivision, "*", "", "", anio, mes)
    finally:
        if connection:
            connection.close()
    return


def fn_calcVOperAVG_div(cveDivision, anio, lst_months, version, useThread) :
    utlpy.println("fn_calcVOperAVG_div:started, useThread:{}".format(useThread))

    try:
        lst_threads = []                
        for mes in lst_months : 
            if useThread:
                t = Thread(target = fn_calcVOperAVG_div_mes, args = (cveDivision, anio, mes, version))
                lst_threads.append(t)
            else:
                fn_calcVOperAVG_div_mes(cveDivision, anio, mes, version)
            #end-if
        #end for mes
        
        if useThread:
            [t.start() for t in lst_threads]
            [t.join() for t in lst_threads]
        #end-if    
        
    except Exception as e:
        utlpy.println('fn_calcVOperAVG_div.error>>> {}'.format(str(e)))
        
    print("fn_calcVOperAVG_div:main thread completed. useThread:{}", useThread)
    return

def fn_calcVOperAVG_main (version, anio, lst_divisiones, lst_months, useThread):    
    start_time = utlpy.current_time()
    print("fn_calcVOperAVG_main block started at: {}".format(utlpy.current_time_fmt()))
    for division in lst_divisiones :
        fn_calcVOperAVG_div(division, anio, lst_months, version, useThread)

    elapsed_time_fmt = utlpy.elapsed_time_fmt(start_time)
    print("fn_calcVOperAVG_main block completed: {}, duration:{}".format(utlpy.current_time_fmt(), elapsed_time_fmt))
    return


##########################
# main
##########################

if __name__ == '__main__' :
    
    start_time = utlpy.current_time()
    print("__main__ started at: {}".format(utlpy.current_time_fmt()))    
    
    version = 'V2'
    anio = "2018"    
    lst_divisiones = ['DA','DB','DC','DD','DF','DG','DJ','DK','DL','DM','DN','DP','DU','DV','DW','DX']
    lst_months = ['ENE','FEB','MAR','ABR','MAY','JUN','JUL','AGO','SEP','OCT','NOV','DIC']
    [fn_calcVOperAVG_div(division, anio, lst_months, version, True) for division in lst_divisiones]    
    [fn_calcVOperAVG_div(division, anio, ['AGO'], version, True) for division in ['DJ'] ]
    [fn_calcVOperAVG_div(division, anio, ['FEB','JUL','AGO'], version, True) for division in ['DK'] ]
    [fn_calcVOperAVG_div(division, anio, ['AGO'], version, True) for division in ['DL'] ]
    [fn_calcVOperAVG_div(division, anio, ['DIC','MAR','AGO'], version, True) for division in ['DM'] ]
    [fn_calcVOperAVG_div(division, anio, ['DIC','MAR'], version, True) for division in ['DN'] ]
    [fn_calcVOperAVG_div(division, anio, ['MAY','SEP','NOV','OCT','DIC','JUL','AGO','ENE'], version, True) for division in ['DP'] ]
    [fn_calcVOperAVG_div(division, anio, ['MAR','AGO'], version, True) for division in ['DU'] ]
    [fn_calcVOperAVG_div(division, anio, ['ENE','AGO'], version, True) for division in ['DV'] ]
    [fn_calcVOperAVG_div(division, anio, ['AGO',''], version, True) for division in ['DW'] ]
    [fn_calcVOperAVG_div(division, anio, ['JUL','DIC'], version, True) for division in ['DX'] ]    
    [fn_calcVOperAVG_div(division, anio, ['NOV','OCT','JUL'], version, True) for division in ['DP'] ]
    [fn_calcVOperAVG_div(division, anio, ['MAY','SEP','ENE'], version, True) for division in ['DP'] ]

    elapsed_time_fmt = utlpy.elapsed_time_fmt(start_time)
    print("__main__ completed at {}, duration:{}".format(utlpy.current_time_fmt(), elapsed_time_fmt))
