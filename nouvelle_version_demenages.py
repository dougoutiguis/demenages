#!/usr/bin/python2.7
# coding: utf-8

from pyspark import SparkConf, SparkContext, SQLContext
from pyspark.sql import HiveContext
from datetime import datetime, timedelta
from pyspark.sql.types import *
from pyspark.sql.functions import *
from datetime import *
import time
from pyspark.sql import SparkSession
import pyspark
import unicodedata
import csv
import re
import os
import locale
import pandas as pand
import numpy as np
import sys
import urllib2
import urllib
import thread
import time
import threading
from pyspark.sql import functions as F
conf = SparkConf().setAppName("Aggregating histo for dem per day v2")
conf.set("spark.sql.broadcastTimeout",  36000000)
conf.set("spark.sql.autoBroadcastJoinThreshold",-1)
sc = SparkContext(conf=conf)
sqlContext = HiveContext(sc)
sqlCtx = SQLContext(sc) 
def get_year_month_day(day="2017-06-11"):
    dy=day.split("-")
    y=int(dy[0])
    m=int(dy[1])
    d=int(dy[2])
    return y,m,d
def getYearMonthDay(day="20170611"):
     y=int(day[:4])
     m=int(day[4:6])
     d=int(day[6:])
     return y,m,d
def entre_deux_date(y_debut,m_debut,d_debut,y_fin,m_fin,d_fin):
    import datetime
    listOfDay=[]
    listOfDays=[]
    date_deb = datetime.date(y_debut,m_debut,d_debut)
    date_fin = datetime.date(y_fin,m_fin,d_fin)
    diff = date_fin - date_deb
    for i in range(diff.days + 1):
        listOfDay.append( (date_deb + datetime.timedelta(i)).isoformat())
        listOfDays.append( (date_deb + datetime.timedelta(i)).strftime("%Y%m%d"))
    return listOfDay,listOfDays
def decoupe_table_par_jour(db="default",table="demenageur_4",dba="doug",a=".",outPrefixe="nb_hit_par_user_zipcode_tranche_horaire_"):
    date_debut=sqlContext.sql("select * from "+db+a+table).agg(min(col("date"))).collect()[0][0]
    #print date_debut
    date_fin=sqlContext.sql("select * from "+db+a+table).agg(max(col("date"))).collect()[0][0]
    #print date_fin
    y_d,m_d,d_d=get_year_month_day(day=date_debut)
    y_f,m_f,d_f=get_year_month_day(day=date_fin)
    avec_tiret,sans_tiret=entre_deux_date(y_d,m_d,d_d,y_f,m_f,d_f)
    print avec_tiret
    print sans_tiret
    for s_tiret,a_tiret in zip(sans_tiret, avec_tiret):
        print "traitement du jour "+a_tiret+ " ...."
        sqlContext.sql("""select * from """+db+a+table+""" """).filter(col("date")==a_tiret).registerTempTable("temp")
        #sqlContext.sql("drop  table if exists "+dba+a+outPrefixe+s_tiret)
        sqlContext.sql("create table if not exists "+dba+a+outPrefixe+s_tiret+" as select * from temp")
        print "table "+dba+a+outPrefixe+s_tiret+" creer avec succes"


def columnList(nombre_de_column=50):
    liste=[]
    for i in range(1, nombre_de_column):
        liste=liste+[i]  
    return liste
def columnListWithGap(nombre_de_column=50,gap=12):
    liste=[]
    for i in range(1+gap, nombre_de_column):
        liste=liste+[i]  
    return liste
def joining_multiple_dataframe(dfs=['20170216','20170125'],database='doug.',table_name_prefixe="user_nbhit_"):
    lis=[]
    if len(dfs) > 1:
        #print("dans if")
        df=sqlContext.sql(""" SELECT  * from """+database+table_name_prefixe+str(dfs[0])+""" """)
        df1=df.withColumn("day1",lit(str(dfs[0])))
        #df1.show()
        #print("Le nombre de user de la white liste du "+str(dfs[0])+" est " + str(df.count())+" users")
        return df1.unionAll(create_dataframe_liste(dfs[1:],database=database,table_name_prefixe=table_name_prefixe))
    else:
        #print("dans else")
        df=sqlContext.sql(""" SELECT  * from """+database+table_name_prefixe+str(dfs[0])+""" """)
        df1=df.withColumn("day1",lit(str(dfs[0])))
        #df1.show()
        #print("Le nombre de user de la white liste du "+str(dfs[0])+" est " + str(df.count())+" users")
        return  df1

#retourne sem si c'est un jour de semaine ou we si c'est un samedi ou dimanche
def weekDay(day="2017-06-11"):
    if "-" in day:
        year, month, day=get_year_month_day(day)
    else:
        year, month, day=getYearMonthDay(day)
    offset = [0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334]
    week   = ['Sunday', 
              'Monday', 
              'Tuesday', 
              'Wednesday', 
              'Thursday',  
              'Friday', 
              'Saturday']
    afterFeb = 1
    if month > 2: afterFeb = 0
    aux = year - 1700 - afterFeb
    # dayOfWeek for 1700/1/1 = 5, Friday
    dayOfWeek  = 5
    # partial sum of days betweem current date and 1700/1/1
    dayOfWeek += (aux + afterFeb) * 365                  
    # leap year correction    
    dayOfWeek += aux / 4 - aux / 100 + (aux + 100) / 400     
    # sum monthly and day offsets
    dayOfWeek += offset[month - 1] + (day - 1)               
    dayOfWeek %= 7
    if week[dayOfWeek] in ["Sunday",'Saturday']:
        return "we"
    else:
        return "sem"

#retourne le nom du jour de la semaine
def weekDayName(day="2017-06-11"):
    if "-" in day:
        year, month, day=get_year_month_day(day)
    else:
        year, month, day=getYearMonthDay(day)
    offset = [0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334]
    week   = ['Sunday', 
              'Monday', 
              'Tuesday', 
              'Wednesday', 
              'Thursday',  
              'Friday', 
              'Saturday']
    afterFeb = 1
    if month > 2: afterFeb = 0
    aux = year - 1700 - afterFeb
    # dayOfWeek for 1700/1/1 = 5, Friday
    dayOfWeek  = 5
    # partial sum of days betweem current date and 1700/1/1
    dayOfWeek += (aux + afterFeb) * 365                  
    # leap year correction    
    dayOfWeek += aux / 4 - aux / 100 + (aux + 100) / 400     
    # sum monthly and day offsets
    dayOfWeek += offset[month - 1] + (day - 1)               
    dayOfWeek %= 7
    #if week[dayOfWeek] in ["Sunday",'Saturday']:
    return week[dayOfWeek]

e = threading.Event()
def main(waitingTime=60):
    thread.start_new_thread(wait_for_input, tuple())
    thread.start_new_thread(do_something, (waitingTime,))

def wait_for_input():
    raw_input("Taper entrer pour continuer.\n")
    e.set()
#waitingTime en secondes
def do_something(waitingTime=60):
    T0 = time.time()
    i=0
    k=0
    while (time.time() - T0) < waitingTime and not e.isSet(): #as long as 60s haven't elapse #and the flag is not set
        #here do a bunch of stuff
        i=i+1
        if int(time.time() - T0)==0 and i==1 :
            print "Reprise du programme dans "+str(waitingTime/60.0)+ " minute...\n"
        if int(time.time() - T0) < waitingTime-1:
            k=0

        else :
            k=k+1
        if k==1 :
            print "Reprise dans 1 seconde\n"
    thread.interrupt_main() # kill the raw_input thread
    print "Reprise du programme"
def waitingOrder(waitingTime=60):
    try:
        thread.start_new_thread(main, (waitingTime,))
        while 1:
            time.sleep(0.1) 
    except KeyboardInterrupt:
        pass 
#test si une table existe dans une base de données 
#arg: 
def table_exist(db="sample_data",pref="historisation_",day='20170502'):
        stg_table_exists = sqlContext.sql("SHOW TABLES IN "+db).filter(col("tableName")==pref+str(day)).collect()  
        #print stg_table_exists
        if len(stg_table_exists) ==1:
            return True
        return False 
def columnList(nombre_de_column=50):
    liste=[]
    for i in range(1, nombre_de_column):
        liste=liste+[i]       
    return liste
#a utiliser sur la base de données sample_data pour obtenir une liste de jours dont les tables a 1% sont présente
#NB: on peut l"utiliser sur la base a 1 pour 1000
def dayWithGapAndChecking(nombre_de_jour=50,gap=12,db="sample_data",pref="historisation_",seuil=30):
    liste=[]
    liste1=[]
    for i in range(1+gap, nombre_de_jour):
        day1=(datetime.now() - timedelta(days=i)).strftime("%Y%m%d")
        if  table_exist(db=db,pref=pref,day=day1):
            if len(liste)<=seuil :
                liste=liste+[day1]
        else: 
            liste1.append(day1)
    return liste[::-1],liste1[::1]
def table_exists(database="doug",tableName="somme_"):
        sqlContext.sql("use "+database)
        if sqlContext.sql("show tables").filter(col("tableName").like("%"+tableName+"%")).count()!=0:
            return True
        else:
            return False
#transforme une date du format yyyymmdd en yyyy-mm-dd ou yyyy/mm/dd en fonction du séparateur
def ajout_tiret_date(day,sep):
    return day[:4]+sep+day[4:6]+sep+day[6:]
#retournr la nb_days jour avant day
def the_day_before(day,sep,nb_days):
    from datetime import datetime, timedelta
    start = ajout_tiret_date(day,sep)
    #start = datetime.strptime(start, "%m/%d/%Y") #string to date
    end = (datetime.strptime(start, "%Y-%m-%d") - timedelta(days=nb_days)).strftime("%Y%m%d") # date - days
    return end   
def getting_zipcode_maj_of_weekend_day(etat=1,dbsource="doug",database="doug.",today='20170213',in_prefixe="z",out_prefixe="user_mono_zipCode_") :
        tempTable =in_prefixe+str(today)
        dy=ajout_tiret_date(today,"-")
        #cookie             |nb_hits|tranche|zipcode|date 
        df=sqlContext.sql(""" SELECT  distinct  cast(cookie as bigint) as user_id, zipcode,nb_hits,cast(tranche as int) as tranche from """+dbsource+"""."""+tempTable+"""   """)
        df2=df.filter(length(df.zipcode)==5 )
        df4_we=df2.groupBy(df2.user_id,df2.zipcode).agg(sum(df2.nb_hits).alias("nb_page"))
        df5_we=df4_we.groupBy("user_id").agg(max('nb_page').alias('max_page_we'))
        df6_we=df4_we.join(df5_we,'user_id','left').filter(col('max_page_we')==col('nb_page'))
        df6_we.select("user_id","zipcode").registerTempTable("temp")
        sqlContext.sql("drop table  IF EXISTS "+database+out_prefixe+str(today))
        sqlContext.sql("create table "+database+out_prefixe+str(today)+" as select user_id,max(zipcode) as zipcode from temp group by user_id" )

def getting_zipcode_maj_of_working_day(etat=1,dbsource="sample_data",database="doug.",today='20170213',in_prefixe="Historisation_",out_prefixe="user_mono_zipCode_") :
        tempTable =in_prefixe+str(today) 
        #df=sqlContext.sql(""" SELECT  distinct 1 as nb_hit, cookie as user_id, CAST(timestamp as timestamp) AS date_hist,zipcode,date_format(cast(timestamp as timestamp),'u') as dayofweek,hour(cast(timestamp as timestamp)) as hour,date_format(cast(timestamp as timestamp),'"yyyy-MM-dd') as day from """+dbsource+"""."""+tempTable+"""   """)
        dy=ajout_tiret_date(today,"-")
        df=sqlContext.sql(""" SELECT  distinct  cast(cookie as bigint) as user_id, zipcode,nb_hits,cast(tranche as int) as tranche from """+dbsource+"""."""+tempTable+"""   """)
        df2=df.filter(length(df.zipcode)==5 )
        df3=df2.withColumn("horaire_double",lit(F.when((df2.tranche==2) | (df2.tranche== 3),"home")\
            .otherwise("middayOrWork")))
        df4_home=df3.filter( df3.horaire_double=='home')
        df4_home1=df4_home.groupBy([df4_home.user_id,df4_home.zipcode]).agg(sum(df4_home.nb_hits).alias("nb_page"))
        df5_home=df4_home1.groupBy("user_id").agg(max('nb_page').alias('max_page_home'))
        df6_home=df4_home1.join(df5_home,'user_id','left').filter(col('max_page_home')==col('nb_page'))
        df6_home.select("user_id","zipcode").registerTempTable("temp")
        sqlContext.sql("drop table  IF EXISTS "+database+out_prefixe+str(today))
        sqlContext.sql("create table IF NOT EXISTS "+database+out_prefixe+str(today)+" as select user_id,max(zipcode) as zipcode from temp group by user_id" )

def getting_major_zipcode_of_A_day(etat=1,dbsource="sample_data",database="doug.",today='20170213',in_prefixe="historisation_",out_prefixe="user_zipmajoritaire_") :
        if weekDay(day=today)=="sem":
            print "c'est un jour de semaine"
            getting_zipcode_maj_of_working_day(etat=etat,dbsource=dbsource,database=database,today=today,in_prefixe=in_prefixe,out_prefixe=out_prefixe)
        else:
            print "c'est un jour de weekend"
            getting_zipcode_maj_of_weekend_day(etat=etat,dbsource=dbsource,database=database,today=today,in_prefixe=in_prefixe,out_prefixe=out_prefixe)
def sontEgaux(zipcodeDuJour,zipcodeDeReference):
    if zipcodeDuJour==zipcodeDeReference:
        return 1
    else:
        return 0
colonne_egale=udf(sontEgaux, IntegerType())

#prends les données sur une journnée (user_zipmajoritaire_+"day"), et mets ceux qui ont déjà un zipcode de référence dans la table a_tester
#ceux qui n'en ont pas, sont placé dans la table ajouter_Reference
def traitement_donnees_du_jour(db="doug",a='.',in_jour="user_zipmajoritaire_",zipcodeDeReference="zipcode_de_reference",day="20170612"):
    don_jour=sqlContext.sql("""select * from doug"""+a+in_jour+str(day)+"""  """)
    zipRef=sqlContext.sql("""select distinct * from """+db+a+zipcodeDeReference+"""  """)
    if ref==True:
        ceux_qui_ont_un_code_postal_de_reference=don_jour.join(zipRef,"user_id","inner")\
        .withColumn("etatNouveauZipcode",colonne_egale(zipRef.zipcode_de_reference,don_jour.zipcode))\
        .select("user_id","zipcode","etatNouveauZipcode")
        df_writer = pyspark.sql.DataFrameWriter(ceux_qui_ont_un_code_postal_de_reference)
        df_writer.saveAsTable(db+a+"a_Tester", format='parquet', mode='overwrite')
        user_sans_code_postal_de_reference=don_jour.join(zipRef,"user_id","left").filter(zipRef.user_id.isNull())
        df_writer = pyspark.sql.DataFrameWriter(user_sans_code_postal_de_reference.select("user_id","zipcode"))
        #user_sans_code_postal_de_reference.select("user_id","zipcode").show()
        df_writer.saveAsTable(db+a+"ajouter_Reference", format='parquet', mode='overwrite')
        #print "affichage ajouter ref apres overwrite"
        #sqlContext.sql("""select * from """+db+a+"""ajouter_Reference  """).show()
    else:
        df_writer = pyspark.sql.DataFrameWriter(don_jour.select("user_id","zipcode"))
        df_writer.saveAsTable(db+a+"ajouter_Reference", format='parquet', mode='overwrite')
        #print "affichage ajouter ref apres overwrite"
        #sqlContext.sql("""select * from """+db+a+"""ajouter_Reference  """).show()
        #don_jour.select("user_id","zipcode").show()
#prends les table a_tester et donnees_demenageurs: si le user est dans les deux tables ou s'il n'est pas dans donnees_demenageurs mais 
#dont le zipcode de référence n'est pas égal au zipcode du jour, alors on le mets dans la table ajouter_demenageur
#sinon s'il n'est  dans donnees_demenageurs mais son zipcode de référence est égal au zipcode du jour alors on le mets dans ajouter_reference
def test_user_qui_ont_un_zipcode_reference(db="doug",a='.',atester="a_Tester",donneesDemenageurs="donnees_demenageurs",day="20170611"):
        if ref==True:
            user_atester = sqlContext.sql(""" select * from """+db+a+atester+"""  """)
            donDemenageur = sqlContext.sql(""" select distinct * from """+db+a+donneesDemenageurs+"""  """)
            ceux_qui_sont_dans_la_table_donnees_demenageurs = user_atester.join(donDemenageur,"user_id","left")\
            .filter(col("zipcode_de_reference").isNotNull())
            ceux_qui_ne_sont_pas_dans_la_table_donnees_demenageurs = user_atester.join(donDemenageur,"user_id","left")\
            .filter(col("zipcode_de_reference").isNull())
            ceux_dont_le_zipcode_du_jour_est_egal_au_zipcode_de_reference=ceux_qui_ne_sont_pas_dans_la_table_donnees_demenageurs\
            .filter(col("etatNouveauZipcode")==1)
            ceux_dont_le_zipcode_du_jour_nest_pas_egal_au_zipcode_de_reference=ceux_qui_ne_sont_pas_dans_la_table_donnees_demenageurs\
            .filter(col("etatNouveauZipcode")==0)
            df_writer = pyspark.sql.DataFrameWriter(ceux_dont_le_zipcode_du_jour_est_egal_au_zipcode_de_reference.select("user_id","zipcode"))
            df_writer.saveAsTable(db+a+"ajouter_Reference", format='parquet', mode='append')
            #print "affichage ajouter ref apres append"
            #sqlContext.sql("""select * from """+db+a+"""ajouter_Reference  """).show()
            ajouterdemenageurs=ceux_dont_le_zipcode_du_jour_nest_pas_egal_au_zipcode_de_reference\
            .unionAll(ceux_qui_sont_dans_la_table_donnees_demenageurs)
            df_writer = pyspark.sql.DataFrameWriter(ajouterdemenageurs.select("user_id","zipcode"))
            df_writer.saveAsTable(db+a+"ajouter_Demenageur", format='parquet', mode='overwrite')

#on prends les users présents dans la table ajouter_reference, on rajoute les champs somme_we et somme_sem et on rajoute a l'ancienne 
#table historisation_reference
"""
db="demenages1"
a='.'
ajouterReference="ajouter_Reference"
historisationReference="historisation_reference"
day="20170525"

"""
def ajouterHistorisationReference(db="doug",a='.',ajouterReference="ajouter_Reference",historisationReference="historisation_reference",day="20170611"):
        sqlContext.sql("drop table  IF EXISTS "+db+a+"temp1")
        sqlContext.sql(""" create table  """+db+a+"""temp1 as select distinct * from """+db+a+historisationReference+"""  """)
        if weekDayName(day=day) in ["Saturday","Sunday"]:
            sqlContext.sql(""" select *,1 as somme_we,0 as somme_sem from """+db+a+ajouterReference+"""  """).registerTempTable("temp2")
        else:
            sqlContext.sql(""" select *,0 as somme_we,1 as somme_sem from """+db+a+ajouterReference+"""  """).registerTempTable("temp2")
        sqlContext.sql("drop table  IF EXISTS "+db+a+historisationReference)
        sqlContext.sql("""
         create table   """+db+a+historisationReference+"""
         as select a.user_id,a.zipcode, sum(somme_we) as somme_we ,sum(somme_sem) as somme_sem 
         from (select user_id,zipcode,somme_we , somme_sem  from """+db+a+"""temp1 
         union all 
         select user_id,zipcode,somme_we , somme_sem  from temp2) a group by user_id,zipcode
         """) 
        sqlContext.sql("drop table  IF EXISTS "+db+a+"temp1")
#on prends le contenu de la table ajouter_reference, on rajoute la colonne
#date_entree: date du jour d'entree dans la table liste cookie 
#nombre_de_jours_ecoules_depuis_la_date_d_entree: date_du_jour-date_entre+1
#date_de_derniere_vue: dernière a laquelle le cookie est rencontré
#nombre_de_jours: date_du_jour-date_de_derniere_vue
#a chaque fois rajoute a l'ancienne liste_cookie, la nouvelle ainsi crée et on garde par cookie, 
#date_de_derniere_vue max
#date_entree  min
#nombre_de_jours_ecoules_depuis_la_date_d_entree max
#nombre_de_jours max
def misAJourListeCookie_a_Partir_de_la_table_ajouterReference(db="doug",a='.',ajouterReference="ajouter_reference",listeCookie="liste_cookie",day="20170611"):
    dt=ajout_tiret_date(day,"-")
    anc_liste_cookie=sqlContext.sql(""" select distinct cast(user_id as bigint) as user_id, date_de_derniere_vue,date_entree,
        nombre_de_jours,nombre_de_jours_ecoules_depuis_la_date_d_entree from """+db+a+listeCookie+"""  """)
    anc_liste_cookie1=anc_liste_cookie.withColumn("dt",lit(dt))\
    .withColumn("dj",lit(datediff(col("dt"),col("date_entree"))+1)).drop("nombre_de_jours_ecoules_depuis_la_date_d_entree")\
    .withColumnRenamed("dj","nombre_de_jours_ecoules_depuis_la_date_d_entree")\
    .withColumn("jd",lit(datediff(col("dt"),col("date_de_derniere_vue"))+1)).drop("nombre_de_jours")\
    .withColumnRenamed("jd","nombre_de_jours")\
    .registerTempTable("temp1")
    sqlContext.sql(""" drop table if exists """+db+a+"""temp1""")
    sqlContext.sql(""" create table  """+db+a+"""temp1 as select  user_id, date_de_derniere_vue,date_entree,nombre_de_jours,
        nombre_de_jours_ecoules_depuis_la_date_d_entree from temp1  """)
    df = sqlContext.sql(""" select user_id,1 as nombre_de_jours , 1 as nombre_de_jours_ecoules_depuis_la_date_d_entree 
        from """+db+a+ajouterReference+"""  """)
    dansAjouterReference=df.withColumn("date_de_derniere_vue",lit(dt)).withColumn("date_entree",lit(dt))
    dansAjouterReference.registerTempTable("temp2")
    sqlContext.sql("drop table  IF EXISTS "+db+a+listeCookie)
    sqlContext.sql("""create table   """+db+a+listeCookie+"""
     as select user_id,max(date_de_derniere_vue ) as date_de_derniere_vue ,
     min(date_entree) as date_entree,max(nombre_de_jours_ecoules_depuis_la_date_d_entree)
      as nombre_de_jours_ecoules_depuis_la_date_d_entree, max(nombre_de_jours) as nombre_de_jours 
      from (select user_id,date_de_derniere_vue,date_entree,nombre_de_jours,nombre_de_jours_ecoules_depuis_la_date_d_entree
       from """+db+a+"""temp1 
       union 
       select user_id,date_de_derniere_vue,date_entree,nombre_de_jours,nombre_de_jours_ecoules_depuis_la_date_d_entree 
       from temp2) abd 
       group by user_id
       """)

#seuilNBJour: nombre minimum de jour ecoulé depuis l'entree du cookie dans la table liste_cookie (au mmoins 30 jours
#de présence du user pour déterminer son zipcode de référence)
#on prend la table historisation_reference et on calcul et on rajoute les champs
#total_somme_sem: somme du nombre de jours de semaine ou un user est sur un zipcode au horaires de maison
#total_somme_we: somme du nombre de jours de weekend ou un user est sur un zipcode le weekend
#percent_sem: la probabilité du user a être sur un zipcode au horaires de maison les jours de semaine
#percent_we: la probabilité du user a être sur un zipcode le weekend
#ensuite on joint cette nouvelle table avec la table liste_cookie pour avoir le nombre de jour écoulé depuis l'entrée de ce cookie
#à aujourd'hui, ensuite on filtre selon les critères suivants:
#-seuilNBJour=30
#SeuilTotalSemaine=15
#seuilPercentSem=0.6
#seuilPercentWe=0.5
#et tous les users respectant ces conditions sont considérés comme ayant un zipcode de référence
def calcul_zipcode_reference(db="doug",a='.',historisationReference="historisation_reference",\
    listeCookie="liste_cookie",day="20170611",seuilNBJour=1,SeuilTotalSemaine=18,seuilPercentSem=0.6\
    ,seuilPercentWe=0,seuilMinimumDepresence=7):
        anc_historef=sqlContext.sql(""" select distinct * from """+db+a+historisationReference+"""  """)
        anc_liste_cookie=sqlContext.sql(""" select distinct * from """+db+a+listeCookie+"""  """)
        historef=anc_historef.groupBy("user_id").agg(sum("somme_sem").alias("total_somme_sem"),sum("somme_we").alias("total_somme_we"))\
        .withColumn("total_jour",lit(col("total_somme_sem")+col("total_somme_we")))
        historef1=anc_historef.select("user_id","zipcode","somme_we" , "somme_sem" ,(col("somme_we")+col("somme_sem")).alias("total_day_zip"))
        histo_join=historef1.join(historef,"user_id")
        hj=histo_join.join(anc_liste_cookie,"user_id")\
        .filter((col("total_day_zip")>=SeuilTotalSemaine)&((col("total_day_zip")/(col("nombre_de_jours_ecoules_depuis_la_date_d_entree")*1.0))>=seuilPercentSem))
        eta_zipcode11=hj.select("user_id", col("zipcode").alias("zipcode_de_reference"))
        df_writer = pyspark.sql.DataFrameWriter(eta_zipcode11)
        df_writer.insertInto(db+a+"zipcode_de_reference", overwrite=False)
#on prends la table ajouter_demenageur et on procède ainsi
#les users qui ne sont pas dans ajouter_demenageur et historisation_demenageur on rajoute les colonnes
#date_debut_demenagement=0 et semaine=0 et si c'est le weekend alors somme_we=1 et somme_sem=0 sinon somme_we=0 et somme_sem=1
#pour les users qui sont  dans ajouter_demenageur et historisation_demenageur et qui on un zipcode pour lequel il sont entrés 
#dans la table historisation_demenageur et qui est différent du zipcode qu'ils ont ajourd'hui dans la table ajouter_demenageur
#  on rajoute les colonnes date_debut_demenagement=0 et semaine=0 et si c'est le weekend alors somme_we=1 et somme_sem=0 
#sinon somme_we=0 et somme_sem=1
#pour les users qui sont  dans ajouter_demenageur et historisation_demenageur et qui on un zipcode pour lequel il sont entrés 
#dans la table historisation_demenageur et qui est égal au zipcode qu'ils ont ajourd'hui dans la table ajouter_demenageur
#les valeurs de la colonne date_debut_demenagement ne change pas
# et semaine=(date_du_jour-date_debut_demenagement +1)/7
# et si c'est le weekend alors somme_we=somme_we+1  
#sinon somme_sem=somme_sem+1
#une fois ces modifications terminées, on reconstitue à partir de ces trois sources et de l'ancien contenu de la table historisation_demenageur
#la nouvelle table historisation_demenageur en gardant par user_id, zipcode le
#date_debut_demenagement min 
#somme_we sum
#somme_sem sum
#semaine max
"""
db="demenages2"
a='.'
historisationDemenageur="historisation_demenageur"
ajouterDemenageur="ajouter_Demenageur"
day="20170625"
"""
#anc_histodem=sqlContext.sql(""" select * from """+db+a+historisationDemenageur+"""  """).distinct()
#dansAjouterdem = sqlContext.sql(""" select user_id,zipcode as zipcode_anc  from """+db+a+ajouterDemenageur+"""  """).distinct()
#dansAjouterdem1 = sqlContext.sql(""" select user_id,zipcode   from """+db+a+ajouterDemenageur+"""  """).distinct()
"""
dt=ajout_tiret_date(day,"-")
table_jointe=dansAjouterdem.join(anc_histodem,["user_id"],"left").filter(anc_histodem.user_id.isNull())\
.select("user_id","zipcode_anc").withColumnRenamed("zipcode_anc","zipcode")
table_jointe11=dansAjouterdem1.join(anc_histodem,["user_id","zipcode"],'left').filter(col("date_debut_demenagement").isNotNull())\
.withColumn("date_du_jour",lit(dt))
table_jointe22=dansAjouterdem1.join(anc_histodem,["user_id","zipcode"],'left').filter(col("date_debut_demenagement").isNull())\
.select("user_id","zipcode").withColumn("date_debut_demenagement",lit(dt)).withColumn("semaine",lit(0))
table_jointe1=dansAjouterdem.join(anc_histodem,["user_id"],"left").filter((anc_histodem.user_id.isNotNull())&(anc_histodem.zipcode==dansAjouterdem.zipcode_anc))\
.withColumn("date_du_jour",lit(dt))
table_jointe2=dansAjouterdem.join(anc_histodem,["user_id"],"left").filter((anc_histodem.user_id.isNotNull())&(anc_histodem.zipcode!=dansAjouterdem.zipcode_anc))\
.select("user_id","zipcode_anc").withColumnRenamed("zipcode_anc","zipcode").withColumn("date_debut_demenagement",lit(dt)).withColumn("semaine",lit(0))
table_jointe3=anc_histodem.join(dansAjouterdem,["user_id"],"left").filter((dansAjouterdem.user_id.isNotNull())&(anc_histodem.zipcode!=dansAjouterdem.zipcode_anc))
table_jointe4=anc_histodem.join(dansAjouterdem,["user_id"],"left").filter(dansAjouterdem.user_id.isNull())

"""
def ajouter_demenageurs(db="doug",a='.',historisationDemenageur="historisation_demenageur",ajouterDemenageur="ajouter_Demenageur",day="20170611"):
    sqlContext.sql("drop table  IF EXISTS "+db+a+"temp1")
    sqlContext.sql("drop table  IF EXISTS "+db+a+"temp11")
    sqlContext.sql("drop table  IF EXISTS "+db+a+"temp12")
    sqlContext.sql("drop table  IF EXISTS "+db+a+"temp13")
    sqlContext.sql("drop table  IF EXISTS "+db+a+"temp2")
    sqlContext.sql("drop table  IF EXISTS "+db+a+"temp21")
    sqlContext.sql("drop table  IF EXISTS "+db+a+"temp22")
    sqlContext.sql("drop table  IF EXISTS "+db+a+"temp23")
    anc_histodem=sqlContext.sql(""" select * from """+db+a+historisationDemenageur+"""  """).distinct()
    dansAjouterdem = sqlContext.sql(""" select user_id,zipcode as zipcode_anc  from """+db+a+ajouterDemenageur+"""  """).distinct()
    dansAjouterdem1 = sqlContext.sql(""" select user_id,zipcode   from """+db+a+ajouterDemenageur+"""  """).distinct()
    dt=ajout_tiret_date(day,"-")
    #cookie qui sont dans ajouter demenageur et pas dans  historisation demenageur
    tad=dansAjouterdem.count()
    thd=anc_histodem.count()
    if tad!=0 and thd!=0:
        table_jointe=dansAjouterdem.join(anc_histodem,["user_id"],"left").filter(anc_histodem.user_id.isNull())\
        .select("user_id","zipcode_anc").withColumnRenamed("zipcode_anc","zipcode")
        #cookie qui sont dans ajouter demenageur et historisation demenageur et qui ont le même zipcode dans les 2 tables
        table_jointe1=dansAjouterdem.join(anc_histodem,["user_id"],"left")\
        .filter((anc_histodem.user_id.isNotNull())&(anc_histodem.zipcode==dansAjouterdem.zipcode_anc))\
        .withColumn("date_du_jour",lit(dt))
        #cookie qui sont dans ajouter demenageur et historisation demenageur et qui ont un zipcode différent dans les 2 tables
        table_jointe2=dansAjouterdem.join(anc_histodem,["user_id"],"left")\
        .filter((anc_histodem.user_id.isNotNull())&(anc_histodem.zipcode!=dansAjouterdem.zipcode_anc)).drop("zipcode")\
        .select("user_id","zipcode_anc").withColumnRenamed("zipcode_anc","zipcode")\
        .withColumn("date_debut_demenagement",lit(dt)).withColumn("semaine",lit(0))
        #cookie qui sont dans historisation demenageur et pas dans ajouter_demenageur
        table_jointe4=anc_histodem.join(dansAjouterdem,["user_id"],"left").filter(dansAjouterdem.user_id.isNull())
        df_writer = pyspark.sql.DataFrameWriter(table_jointe4)
        df_writer.saveAsTable(db+a+"table_jointe", format='parquet', mode='overwrite')
        tb=table_jointe.withColumn("date_debut_demenagement",lit(dt)).withColumn("semaine",lit(0))
        bt=table_jointe
        if weekDayName(day=day) in ["Saturday","Sunday"]:
            bt=tb.withColumn("somme_we",lit(1)).withColumn("somme_sem",lit(0))
        else:
            bt=tb.withColumn("somme_sem",lit(1)).withColumn("somme_we",lit(0))
        sqlContext.sql("drop table  IF EXISTS temp11")
        bt.select("user_id" ,"zipcode","date_debut_demenagement" ,"somme_we","somme_sem","semaine").registerTempTable("temp11")
        sqlContext.sql("drop table  IF EXISTS "+db+a+"temp11")
        sqlContext.sql("""create table   """+db+a+"""temp11 as select distinct * from temp11""" )
        sqlContext.sql("drop table  IF EXISTS "+db+a+"temp2")
        sqlContext.sql("""create table  """+db+a+"""temp2 as  select user_id, zipcode_de_reference, etat_demenageur,date from """+db+a+"""donnees_demenageurs  """)
        table_jointe.withColumn("etat_demenageur",lit(0))\
        .withColumn("date",lit(dt))\
        .select("user_id",col("zipcode")\
        .alias("zipcode_de_reference"),"etat_demenageur","date")\
        .registerTempTable("temp21")
        sqlContext.sql("drop table  IF EXISTS "+db+a+"temp21")
        sqlContext.sql("""create table  """+db+a+"""temp21 as  select user_id, zipcode_de_reference, etat_demenageur,date from temp21  """)
        if weekDayName(day=day) in ["Saturday","Sunday"]:
            bt=table_jointe2.withColumn("somme_we",lit(1)).withColumn("somme_sem",lit(0))
        else:
            bt=table_jointe2.withColumn("somme_sem",lit(1)).withColumn("somme_we",lit(0))
        sqlContext.sql("drop table  IF EXISTS temp12")
        #bt.show()
        df_writer = pyspark.sql.DataFrameWriter(bt)
        df_writer.saveAsTable(db+a+"temp12", format='parquet', mode='overwrite')
        table_jointe2.select("user_id","zipcode").withColumn("etat_demenageur",lit(0)).withColumn("date",lit(dt))\
        .select("user_id",col("zipcode")\
        .alias("zipcode_de_reference"),"etat_demenageur","date")\
        .registerTempTable("temp22")
        sqlContext.sql("drop table  IF EXISTS "+db+a+"temp22")
        sqlContext.sql("""create table  """+db+a+"""temp22 as  select user_id, zipcode_de_reference, etat_demenageur,date from temp22  """)
        sqlContext.sql("drop table  IF EXISTS "+db+a+"""donnees_demenageurs """)
        sqlContext.sql("""create table  """+db+a+"""donnees_demenageurs  as select  distinct tmp.user_id, tmp.zipcode_de_reference,
         tmp.etat_demenageur,min(tmp.date) as date from
         (select user_id, zipcode_de_reference, etat_demenageur,date
         from """+db+a+"""temp2 union all select user_id, zipcode_de_reference, etat_demenageur,date from """+db+a+"""temp21 
         union all select user_id, zipcode_de_reference, etat_demenageur,date from """+db+a+"""temp22) tmp 
         group by tmp.user_id, tmp.zipcode_de_reference,tmp.etat_demenageur""")
        ttb=table_jointe1.withColumn("semaine1",(datediff(table_jointe1.date_du_jour,col("date_debut_demenagement"))+1)/7).drop("semaine")\
        .select( "user_id",col("zipcode"),"date_debut_demenagement",col("semaine1").cast("int").alias("semaine"),\
            "somme_we","somme_sem")
        if weekDayName(day=day) in ["Saturday","Sunday"]:
            bt=ttb.withColumn("somme_we1",lit(col("somme_we")+1)).drop("somme_we").withColumnRenamed("somme_we1","somme_we")
        else:
            bt=ttb.withColumn("somme_sem1",lit(col("somme_sem")+1)).drop("somme_sem").withColumnRenamed("somme_sem1","somme_sem")
        sqlContext.sql("drop table  IF EXISTS temp13")
        bt.select("user_id" ,"zipcode","date_debut_demenagement" ,"somme_we","somme_sem","semaine").registerTempTable("temp13") 
        sqlContext.sql("drop table  IF EXISTS "+db+a+"temp13")
        sqlContext.sql("""create table  """+db+a+"""temp13 as select distinct * from temp13""" )
        sqlContext.sql("drop table  IF EXISTS "+db+a+historisationDemenageur)
        sqlContext.sql("""create table  """+db+a+historisationDemenageur+""" 
            as select distinct * from ( select  tmp.user_id ,tmp.zipcode,min(tmp.date_debut_demenagement) as date_debut_demenagement ,
            sum(tmp.somme_we) as somme_we,sum(tmp.somme_sem) as somme_sem,
            max(tmp.semaine) as semaine from ( select user_id ,zipcode, date_debut_demenagement ,somme_we, somme_sem,semaine
            from """+db+a+"""temp11 union all select user_id ,zipcode, date_debut_demenagement ,somme_we, somme_sem,semaine
             from """+db+a+"""temp12 union all select user_id ,zipcode, date_debut_demenagement ,somme_we, somme_sem,semaine
              from """+db+a+"""temp13
            union all select user_id ,zipcode, date_debut_demenagement ,somme_we, somme_sem,semaine from """+db+a+"""table_jointe) tmp
            group by  tmp.user_id ,tmp.zipcode) ader""" )
        sqlContext.sql("drop table  IF EXISTS "+db+a+"temp1")
        sqlContext.sql("drop table  IF EXISTS "+db+a+"temp11")
        sqlContext.sql("drop table  IF EXISTS "+db+a+"temp12")
        sqlContext.sql("drop table  IF EXISTS "+db+a+"temp13")
        sqlContext.sql("drop table  IF EXISTS "+db+a+"temp2")
        sqlContext.sql("drop table  IF EXISTS "+db+a+"temp21")
        sqlContext.sql("drop table  IF EXISTS "+db+a+"temp22")
        sqlContext.sql("drop table  IF EXISTS "+db+a+"temp23")
    elif tad!=0 and thd==0:
        table_jointe=dansAjouterdem\
        .select("user_id","zipcode_anc").withColumnRenamed("zipcode_anc","zipcode")
        tb=table_jointe.withColumn("date_debut_demenagement",lit(dt)).withColumn("semaine",lit(0))
        bt=table_jointe
        if weekDayName(day=day) in ["Saturday","Sunday"]:
            bt=tb.withColumn("somme_we",lit(1)).withColumn("somme_sem",lit(0))
        else:
            bt=tb.withColumn("somme_sem",lit(1)).withColumn("somme_we",lit(0))
        sqlContext.sql("drop table  IF EXISTS temp11")
        bt.select("user_id" ,"zipcode","date_debut_demenagement" ,"somme_we","somme_sem","semaine").registerTempTable("temp11")
        sqlContext.sql("drop table  IF EXISTS "+db+a+"temp11")
        sqlContext.sql("""create table   """+db+a+"""temp11 as select distinct * from temp11""" )
        table_jointe.withColumn("etat_demenageur",lit(0))\
        .withColumn("date",lit(dt))\
        .select("user_id",col("zipcode")\
        .alias("zipcode_de_reference"),"etat_demenageur","date")\
        .registerTempTable("temp21")
        sqlContext.sql("drop table  IF EXISTS "+db+a+"""donnees_demenageurs """)
        sqlContext.sql("""create table  """+db+a+"""donnees_demenageurs as  select user_id, zipcode_de_reference, etat_demenageur,date from temp21  """)
        sqlContext.sql("drop table  IF EXISTS "+db+a+historisationDemenageur)
        sqlContext.sql("""create table  """+db+a+historisationDemenageur+""" 
            as select distinct  user_id ,zipcode, date_debut_demenagement ,somme_we, somme_sem,semaine
            from """+db+a+"""temp11 """ )
        sqlContext.sql("drop table  IF EXISTS "+db+a+"temp11")
def calcul_zipcode_de_reference_semaine1(db="doug",a='.',historisationDemenageur="historisation_demenageur",\
    day="20170611",seuilTotalsem=3,seuilTotalwe=1,seuilPercentSem=0.6,seuilPercentWe=0.5,nb_jours_depuis_dem_j1=7,semaime=1,\
    nombreDeJourDePresenceSeuil1=28,nombreDeJourDePresenceSeuil2=148):
    dt=ajout_tiret_date(day,"-")
    anc_histodem=sqlContext.sql("select distinct * from "+db+a+historisationDemenageur)\
    .withColumn("dt_jour",lit(dt))\
    .withColumn("nb_jours_depuis_dem_j1",lit((datediff(col("dt_jour"),col("date_debut_demenagement"))+1)))
    nb_jour_max=anc_histodem.agg(max("nb_jours_depuis_dem_j1").alias("max_nb_jour_dem")).collect()[0][0]
    ok=False
    if nb_jour_max!=None:
        nb_iteration=nb_jour_max/7
        if nb_iteration>=1:
            ok=True
            print "Nombre d'iterations à faire "+str(nb_iteration)
    else:
         nb_iteration=0
    somme_tot=sqlContext.sql("select distinct * from "+db+a+historisationDemenageur)\
    .groupBy(["user_id","semaine"]).agg(sum("somme_sem").alias("total_somme_sem"),sum("somme_we").alias("total_somme_we"))
    join_2=anc_histodem.join(somme_tot,["user_id","semaine"])
    if ok==True:
        for i in range(1,nb_iteration+1):
            semaine=i
            print "Traitement semaine "+str(i)
            nb_jours_depuis_dem_j1=i*7
            print "et nombre de jours depuis jour 1 demenagement "+str(nb_jours_depuis_dem_j1)
            histo_percent_sem1=join_2.filter((col("nb_jours_depuis_dem_j1")==nb_jours_depuis_dem_j1) & (col("semaine")==semaine)).withColumn("percent_sem",lit(F.when(col("total_somme_sem")==0,0.0).otherwise(col("somme_sem")/(col("total_somme_sem")*1.0) )))\
            .withColumn("percent_we",lit(F.when((col("total_somme_we")==0),0.0).otherwise(col("somme_we")/(col("total_somme_we")*1.0) )))
            eta_zipcode1=histo_percent_sem1\
            .filter((col("total_somme_we")>=seuilTotalwe)&(col("total_somme_sem")>=seuilTotalsem)&(col("percent_we")>=seuilPercentWe)&(col("percent_sem")>=seuilPercentSem))
            eta_zipcode11=eta_zipcode1.groupBy("user_id").agg(max("zipcode").alias("zipcode_de_reference")).select("user_id","zipcode_de_reference")
            zipRef=sqlContext.sql("""select  user_id,zipcode_de_reference  from """+db+a+"""zipcode_de_reference """)\
            .select("user_id",col("zipcode_de_reference").alias("zipcode_de_reference_init")).distinct()
            a_supprimer_don_dem_et_histo_dem=eta_zipcode11.join(zipRef,"user_id")\
            .filter(col("zipcode_de_reference")==col("zipcode_de_reference_init"))\
            .select("user_id" ).distinct()
            dd=sqlContext.sql("   select user_id,zipcode_de_reference, etat_demenageur ,date from "+db+a+"donnees_demenageurs ")
            hd=sqlContext.sql("select * from "+db+a+historisationDemenageur)
            #2- ceux qui ont un zipcode de référence sur la semaine1 et qui ne sont pas a supprimer
            #3- ceux qui ont un zipcode de reférence de la semaine1 différent du zipcode de référence sont a un état déménageur 1
            a_ajouter_don_dem=eta_zipcode11.join(zipRef,"user_id")\
            .filter(col("zipcode_de_reference")!=col("zipcode_de_reference_init"))\
            .withColumn("etat_demenageur",lit(semaine))\
            .withColumn("date",lit(dt))\
            .select("user_id","zipcode_de_reference","etat_demenageur","date")
            #4- ceux qui sont dans 3 et 2 forment la nouvelle table données déménageurs et on garde pour chaque user état déménageur max
            """
            ndd=dd.unionAll(a_ajouter_don_dem)\
            .join(a_supprimer_don_dem_et_histo_dem,"user_id","left")\
            .filter(a_supprimer_don_dem_et_histo_dem.user_id.isNull())\
            .select("user_id","zipcode_de_reference","etat_demenageur","date")
            ndd.registerTempTable("temp1")
            sqlContext.sql("drop table  IF EXISTS "+db+a+"temp13")
            """
            #sqlContext.sql("""create table  """+db+a+"""temp13 as select distinct * from temp1""" )
            #sqlContext.sql("drop table  IF EXISTS "+db+a+"donnees_Demenageurs")
            #sqlContext.sql("""create table  """+db+a+"""donnees_Demenageurs as select  user_id,zipcode_de_reference, 
            #   etat_demenageur ,min(date) as date from """+db+a+"""temp13  
            #   group by user_id,zipcode_de_reference,etat_demenageur""" )
            """
            sqlContext.sql("drop table  IF EXISTS "+db+a+"temp13")
            nhd=hd.join(a_supprimer_don_dem_et_histo_dem,"user_id","left")\
            .filter(a_supprimer_don_dem_et_histo_dem.user_id.isNull())\
            .select("user_id" ,"zipcode","date_debut_demenagement" ,"somme_we","somme_sem","semaine")
            nhd.registerTempTable("temp")
            sqlContext.sql("drop table  IF EXISTS "+db+a+"temp13")
            """
            #sqlContext.sql("""create table  """+db+a+"""temp13 as select distinct * from temp""" )
            #sqlContext.sql("drop table  IF EXISTS "+db+a+historisationDemenageur)
            #sqlContext.sql("""create table  """+db+a+historisationDemenageur+""" as select distinct * from """+db+a+"""temp13""" )


            #4- ceux qui sont dans 3 et 2 forment la nouvelle table données déménageurs et on garde pour chaque user état déménageur max
            #*********** suppression des user pas présent assez longtemps pour être considéré comme déménageur *********
            date_entree_dd=dd.groupBy("user_id").agg(min("date").alias("date_min"),max("date").alias("date_max"))
            #nb_etat_distinct=dd.groupBy("user_id").agg(countDistinct("etat_demenageur").alias("nb_etat_demenageur"))
            #nb_date=dd.groupBy("user_id").agg(countDistinct("date").alias("nb_date"))
            #nb_zip=dd.filter(dd.etat_demenageur>=1).groupBy("user_id").agg(count("zipcode_de_reference").alias("nb_zip"))
            a_supprimer_don_dem_et_histo_dem_new=a_supprimer_don_dem_et_histo_dem\
            .join(date_entree_dd,"user_id")\
            .withColumn("date_du_jour",lit(dt))\
            .withColumn("nombreDeJourDePresence",lit(datediff(col("date_du_jour"),col("date_min"))+1))\
            .filter(col("nombreDeJourDePresence")<=nombreDeJourDePresenceSeuil1)
            #ceux qui sont revenu au zipcode de référence initial avant la période de surété de 28 jours
            df_writer = pyspark.sql.DataFrameWriter(a_supprimer_don_dem_et_histo_dem_new)
            df_writer.saveAsTable(db+a+"a_supprimer_revenu_auzipcode_de_reference_initial_"+str(semaine)+"_"+day, format='parquet', mode='overwrite')
            a_supprimer_don_dem_et_histo_dem_new1=dd.join(date_entree_dd,"user_id")\
            .withColumn("date_du_jour",lit(dt))\
            .withColumn("nombreDeJourDePresence",lit(datediff(col("date_du_jour"),col("date_min"))+1))\
            .filter(col("nombreDeJourDePresence")>nombreDeJourDePresenceSeuil2)
            df_writer = pyspark.sql.DataFrameWriter(a_supprimer_don_dem_et_histo_dem_new1)
            #ootentiels déménagés trop vieux  car ayant passé plus de 120 jours dans le pool après les 28 
            #jours de surété (en tout donc 148 jours dans le pool)
            df_writer.saveAsTable(db+a+"a_supprimer_car_cookie_trop_vieux_"+str(semaine)+"_"+day, format='parquet', mode='overwrite')
            a_supprimer_don_dem_et_histo_dem_new1=sqlContext.sql("select distinct user_id from "+db+a+"a_supprimer_car_cookie_trop_vieux_"+str(semaine)+"_"+day)
            a_supprimer_don_dem_et_histo_dem_new=sqlContext.sql("select distinct user_id from "+db+a+"a_supprimer_revenu_auzipcode_de_reference_initial_"+str(semaine)+"_"+day)
            ndd=dd.unionAll(a_ajouter_don_dem)\
            .join(a_supprimer_don_dem_et_histo_dem_new,"user_id","left")\
            .filter(a_supprimer_don_dem_et_histo_dem_new.user_id.isNull())\
            .join(a_supprimer_don_dem_et_histo_dem_new1,"user_id","left")\
            .filter(a_supprimer_don_dem_et_histo_dem_new1.user_id.isNull())\
            .select("user_id","zipcode_de_reference","etat_demenageur","date")
            ndd.registerTempTable("temp1")
            sqlContext.sql("drop table  IF EXISTS "+db+a+"temp13")
            sqlContext.sql("""create table  """+db+a+"""temp13 as select distinct * from temp1""" )
            sqlContext.sql("drop table  IF EXISTS "+db+a+"donnees_Demenageurs")
            sqlContext.sql("""create table   """+db+a+"""donnees_Demenageurs as select  user_id,zipcode_de_reference, 
                etat_demenageur ,min(date) as date from """+db+a+"""temp13  
                group by user_id,zipcode_de_reference,etat_demenageur""" )
            sqlContext.sql("drop table  IF EXISTS "+db+a+"temp13")
            nhd=hd.join(a_supprimer_don_dem_et_histo_dem_new,"user_id","left")\
            .filter(a_supprimer_don_dem_et_histo_dem_new.user_id.isNull())\
            .join(a_supprimer_don_dem_et_histo_dem_new1,"user_id","left")\
            .filter(a_supprimer_don_dem_et_histo_dem_new1.user_id.isNull())\
            .select("user_id" ,"zipcode","date_debut_demenagement" ,"somme_we","somme_sem","semaine")
            nhd.registerTempTable("temp")
            sqlContext.sql("drop table  IF EXISTS "+db+a+"temp13")
            sqlContext.sql("""create table  """+db+a+"""temp13 as select distinct * from temp""" )
            sqlContext.sql("drop table  IF EXISTS "+db+a+historisationDemenageur)
            sqlContext.sql("""create table  """+db+a+historisationDemenageur+""" as select distinct * from """+db+a+"""temp13""" )
            
def filtrage_donnees_demenageurs_pour_garder_potentiel_demenageur():
        zr=spark.table("demenages4.zipcode_de_reference")
        dd=spark.table("demenages4.donnees_demenageurs")
        zr=spark.table("demenages4.zipcode_de_reference").distinct().select("user_id",col("zipcode_de_reference").alias("zipcode_init"))
        print "Users ayant dans donnees demenageurs une fois leur zipcode de reference"
        print dd.join(zr,"user_id").filter(zr.zipcode_init==dd.zipcode_de_reference)\
        .select("user_id").distinct().count()
        df_writer = pyspark.sql.DataFrameWriter(dd.join(zr,"user_id").filter(zr.zipcode_init==dd.zipcode_de_reference)\
        .select("user_id").distinct())
        df_writer.saveAsTable('demenages4.user_zipcode_init_in_dd', format='parquet', mode='overwrite')
        zr_dd=spark.table("demenages4.user_zipcode_init_in_dd")
        print "Affichage potentiel demenages"
        dd.join(zr_dd,"user_id").filter(zr_dd.user_id.isNull()).show()
        df_writer = pyspark.sql.DataFrameWriter(dd.join(zr_dd,"user_id").filter(zr_dd.user_id.isNull()))
        df_writer.saveAsTable('demenages4.potentiel_demenages1', format='parquet', mode='overwrite')
        print "Potentiels demenages"
        pd=spark.table("demenages4.potentiel_demenages1")
        print pd.count()
db="demenages1"
a="."
"""
sqlContext.sql("drop table  IF EXISTS temp1")
sqlContext.sql("drop table  IF EXISTS temp2")
sqlContext.sql("drop table  IF EXISTS "+db+a+"historisation_reference")
sqlContext.sql("drop table  IF EXISTS "+db+a+"a_tester")
sqlContext.sql("drop table  IF EXISTS "+db+a+"ajouter_reference")
sqlContext.sql("drop table  IF EXISTS "+db+a+"ajouter_demenageur")
sqlContext.sql("drop table  IF EXISTS "+db+a+"zipcode_de_reference")
sqlContext.sql("drop table  IF EXISTS "+db+a+"historisation_demenageur")
sqlContext.sql("drop table  IF EXISTS "+db+a+"demenages")
sqlContext.sql("drop table  IF EXISTS "+db+a+"liste_cookie")
sqlContext.sql("drop table  IF EXISTS "+db+a+"donnees_demenageurs")
sqlContext.sql("drop table  IF EXISTS "+db+a+"a_tester")
sqlContext.sql("drop table  IF EXISTS "+db+a+"historef")
sqlContext.sql("drop table  IF EXISTS "+db+a+"histo_join")
sqlContext.sql("drop table  IF EXISTS "+db+a+"histo_percent")
sqlContext.sql("drop table  IF EXISTS "+db+a+"histo_join_liste_cookie")
sqlContext.sql("drop table  IF EXISTS "+db+a+"eta_zipcode11")
sqlContext.sql("drop table  IF EXISTS "+db+a+"temp1")
sqlContext.sql("drop table  IF EXISTS "+db+a+"temp11")
sqlContext.sql("drop table  IF EXISTS "+db+a+"temp12")
sqlContext.sql("drop table  IF EXISTS "+db+a+"temp13")
sqlContext.sql("drop table  IF EXISTS "+db+a+"temp2")
sqlContext.sql("drop table  IF EXISTS "+db+a+"temp21")
sqlContext.sql("drop table  IF EXISTS "+db+a+"temp22")
sqlContext.sql("drop table  IF EXISTS "+db+a+"temp23")
"""
sqlContext.sql("""CREATE TABLE IF NOT EXISTS """+db+a+"""a_tester (  user_id BIGINT,
                        zipcode string,etatNouveauZipcode INT)
                        COMMENT 'table contenant les donnees relatives aux potentiels demenageurs'
                        STORED AS PARQUET """)
sqlContext.sql("""CREATE TABLE IF NOT EXISTS """+db+a+"""historisation_reference (  user_id BIGINT,
                        zipcode string,somme_we int,somme_sem int)
                        COMMENT 'table contenant les donnees relatives aux potentiels demenageurs'
                        STORED AS PARQUET """)
sqlContext.sql("""CREATE TABLE IF NOT EXISTS """+db+a+"""ajouter_Reference(  user_id BIGINT,
                        zipcode string)
                        COMMENT 'table contenant les donnees relatives aux potentiels usrs avec un zipcode de référence'
                        STORED AS PARQUET """)
sqlContext.sql("""CREATE TABLE IF NOT EXISTS """+db+a+"""ajouter_Demenageur(  user_id BIGINT,
                        zipcode string)
                        COMMENT 'table contenant les donnees relatives aux potentiels usrs avec un zipcode de référence'
                        STORED AS PARQUET """)
sqlContext.sql("""CREATE TABLE IF NOT EXISTS """+db+a+"""zipcode_de_reference (  user_id Bigint,
                        zipcode_de_reference string)
                        COMMENT 'table contenant le zipcode de référence'
                        STORED AS PARQUET  """)
sqlContext.sql("""CREATE TABLE IF NOT EXISTS """+db+a+"""historisation_demenageur ( 
     user_id BIGINT,zipcode string,date_debut_demenagement String,somme_we int,somme_sem int,semaine int)
                        COMMENT 'table contenant les donnees relatives aux potentiels demenageurs'
                        STORED AS PARQUET  """)
sqlContext.sql("""CREATE TABLE IF NOT EXISTS """+db+a+"""demenages ( 
     user_id BIGINT,zipcode_de_reference_init string,zipcode_de_reference string,date_debut_demenagement String)
                        COMMENT 'table contenant les donnees relatives aux potentiels demenageurs'
                        STORED AS PARQUET  """)
sqlContext.sql("""CREATE TABLE IF NOT EXISTS """+db+a+"""liste_cookie (  user_id BIGINT,
                        date_de_derniere_vue String,date_entree String,
                        nombre_de_jours int, nombre_de_jours_ecoules_depuis_la_date_d_entree int)
                        COMMENT 'table contenant les donnees relatives aux potentiels demenageurs'
                        STORED AS PARQUET  """)
sqlContext.sql("""CREATE TABLE IF NOT EXISTS """+db+a+"""donnees_demenageurs (  user_id BIGINT,
                        zipcode_de_reference string,etat_demenageur int,date string)
                        COMMENT 'table contenant les donnees relatives aux potentiels demenageurs'
                        STORED AS PARQUET """)
#liste,l=dayWithGapAndChecking(nombre_de_jour=100,gap=0,db="sample_data",pref="historisation_",seuil=60)
#liste1=['2017-04-19', '2017-04-20', '2017-04-21', '2017-04-22', '2017-04-23', '2017-04-24', '2017-04-25', '2017-04-26', '2017-04-27', '2017-04-28', '2017-04-29', '2017-04-30', '2017-05-01', '2017-05-02', '2017-05-03', '2017-05-04', '2017-05-05', '2017-05-06', '2017-05-07', '2017-05-08', '2017-05-09', '2017-05-10', '2017-05-11', '2017-05-12', '2017-05-13', '2017-05-14', '2017-05-15', '2017-05-16', '2017-05-17', '2017-05-18', '2017-05-19', '2017-05-20', '2017-05-21', '2017-05-22', '2017-05-23', '2017-05-24', '2017-05-25', '2017-05-26', '2017-05-27', '2017-05-28', '2017-05-29', '2017-05-30', '2017-05-31', '2017-06-01', '2017-06-02', '2017-06-03', '2017-06-04', '2017-06-05', '2017-06-06', '2017-06-07', '2017-06-08', '2017-06-09', '2017-06-10', '2017-06-11', '2017-06-12', '2017-06-13', '2017-06-14', '2017-06-15', '2017-06-16', '2017-06-17', '2017-06-18', '2017-06-19', '2017-06-20', '2017-06-21', '2017-06-22', '2017-06-23', '2017-06-24', '2017-06-25']
#liste=['20170419', '20170420', '20170421', '20170426', '20170427', '20170428', '20170429', '20170430', '20170501','20170502', '20170503', '20170504', '20170505', '20170506', '20170507', '20170508', '20170509', '20170510', '20170511', '20170512', '20170513', '20170514', '20170515', '20170516', '20170517', '20170518', '20170519', '20170520', '20170521', '20170522', '20170523', '20170524', '20170525', '20170526', '20170527', '20170528', '20170529', '20170530', '20170531', '20170601', '20170602', '20170603', '20170604', '20170605', '20170606', '20170607', '20170608', '20170609', '20170610', '20170611', '20170612', '20170613', '20170614', '20170615', '20170616', '20170617', '20170618', '20170619', '20170620', '20170621', '20170622', '20170623', '20170624', '20170625',  '20170627', '20170628', '20170629','20170701', '20170702', '20170703', '20170704',  '20170707', '20170708', '20170709', '20170710', '20170711', '20170712']
#liste=['20170419', '20170420', '20170421', '20170426', '20170427', '20170428', '20170429', '20170430', '20170501','20170502', '20170503', '20170504', '20170505', '20170506', '20170507', '20170508', '20170509', '20170510', '20170511', '20170512', '20170513', '20170514', '20170515', '20170516', '20170517', '20170518', '20170519', '20170520', '20170521', '20170522', '20170523', '20170524', '20170525', '20170526', '20170527', '20170528', '20170529', '20170530', '20170531', '20170601', '20170602', '20170603', '20170604', '20170605', '20170606', '20170607', '20170608', '20170609', '20170610', '20170611', '20170612', '20170613', '20170614', '20170615', '20170616', '20170617', '20170618', '20170619', '20170620', '20170621', '20170622', '20170623', '20170624', '20170625', '20170626', '20170627', '20170628', '20170629','20170630','20170701', '20170702', '20170703', '20170704', '20170705','20170706', '20170707', '20170708', '20170709', '20170710', '20170711', '20170712','20170713','20170714','20170715','20170716']
liste=['20170717','20170718','20170719','20170720','20170721', '20170722']
#liste=[ '20170520', '20170521', '20170522', '20170523', '20170524', '20170525', '20170526', '20170527', '20170528', '20170529', '20170530', '20170531', '20170601', '20170602', '20170603', '20170604', '20170605', '20170606', '20170607', '20170608', '20170609', '20170610', '20170611', '20170612', '20170613', '20170614', '20170615', '20170616', '20170617', '20170618', '20170619', '20170620', '20170621', '20170622', '20170623', '20170624', '20170625', '20170626', '20170627', '20170628', '20170629']
#liste=[ '20170525', '20170526', '20170527', '20170528', '20170529', '20170530', '20170531', '20170601', '20170602', '20170603', '20170604', '20170605', '20170606', '20170607', '20170608', '20170609', '20170610', '20170611', '20170612', '20170613', '20170614', '20170615', '20170616', '20170617', '20170618', '20170619', '20170620', '20170621', '20170622', '20170623', '20170624', '20170625', '20170626', '20170627', '20170628', '20170629']
#dbsource="sample_data",database="doug.",today='20170213',in_prefixe="Historisation_
#print liste[:20]
#liste=[  '20170601', '20170602', '20170603', '20170604', '20170605', '20170606', '20170607', '20170608', '20170609', '20170610', '20170611', '20170612', '20170613', '20170614', '20170615', '20170616', '20170617', '20170618', '20170619', '20170620', '20170621', '20170622', '20170623', '20170624', '20170625', '20170626', '20170627', '20170628', '20170629', '20170630', '20170701', '20170702', '20170703', '20170704', '20170707', '20170708', '20170709', '20170710']
#liste=['20170630', '20170701', '20170702', '20170703', '20170704', '20170705', '20170706', '20170707', '20170708', '20170709', '20170710', '20170711', '20170712', '20170713', '20170714', '20170715']
#liste=['20170601', '20170602', '20170603', '20170604', '20170605', '20170606', '20170607', '20170608', '20170609', '20170610', '20170611', '20170612', '20170613', '20170614', '20170615', '20170616', '20170617', '20170618', '20170619', '20170620', '20170621', '20170622', '20170623', '20170624', '20170625', '20170626', '20170627']
#liste=['20170522', '20170523', '20170524', '20170525', '20170526', '20170527', '20170528', '20170529', '20170530', '20170531', '20170601', '20170602', '20170603', '20170604', '20170605', '20170606', '20170607', '20170608', '20170609', '20170610', '20170611', '20170612', '20170613', '20170614', '20170615', '20170616', '20170617', '20170618', '20170619', '20170620', '20170621', '20170622', '20170623', '20170624', '20170625', '20170626', '20170627', '20170628', '20170629']
#liste=[ '20170612', '20170613', '20170614', '20170615', '20170616', '20170617', '20170618', '20170619', '20170620', '20170621', '20170622', '20170623', '20170624', '20170625', '20170626', '20170627', '20170628', '20170629']
#print liste1 [ '20170501','20170502', '20170503', '20170504', '20170505', '20170506']
#liste= ['20170507', '20170508', '20170509', '20170510', '20170511', '20170512', '20170513', '20170514', '20170515', '20170516', '20170517', '20170518', '20170519', '20170520', '20170521', '20170522', '20170523', '20170524', '20170525', '20170526', '20170527', '20170528', '20170529', '20170530', '20170531', '20170601', '20170602', '20170603', '20170604', '20170605', '20170606', '20170607', '20170608', '20170609', '20170610', '20170611', '20170612', '20170613', '20170614', '20170615', '20170616', '20170617', '20170618', '20170619', '20170620', '20170621', '20170622', '20170623', '20170624', '20170625', '20170626', '20170627', '20170628', '20170629', '20170630', '20170701', '20170702', '20170703', '20170704', '20170707', '20170708', '20170709', '20170710']
#print l
ref=True
nb_day=0
for date_day in liste:
        dt=time.time()
        nb_day=nb_day+1
        #getting_major_zipcode_of_A_day(dbsource="doug",database="doug.",today=date_day,in_prefixe="nb_hit_par_user_zipcode_tranche_horaire_",out_prefixe="user_zipmajoritaire_")
        #getting_major_zipcode_of_A_day(dbsource="sample_data",database=db+".",today=date_day,in_prefixe="Historisation_",out_prefixe="user_zipmajoritaire_")
        #if sqlContext.sql("select * from doug.user_zipmajoritaire_"+date_day).count()!=0:
        if date_day=="20170710":
            #ajouter_demenageurs(db=db,a='.',historisationDemenageur="historisation_demenageur",ajouterDemenageur="ajouter_Demenageur",day=date_day)
            #print "apres  ajouter_demenageurs "+str(date_day)
            #ajouterHistorisationReference(db=db,a='.',ajouterReference="ajouter_Reference",historisationReference="historisation_reference",day=date_day)
            #print "apres ajouterHistorisationReference "+str(date_day)
            #misAJourListeCookie_a_Partir_de_la_table_ajouterReference(db=db,a='.',ajouterReference="ajouter_reference",listeCookie="liste_cookie",day=date_day)
            #print "apres misAJourListeCookie_a_Partir_de_la_table_ajouterReference "+str(date_day)
            #if nb_day>=18:
            #calcul_zipcode_reference(db=db,a='.',historisationReference="historisation_reference",listeCookie="liste_cookie",\
            #day=date_day,seuilNBJour=30,SeuilTotalSemaine=18,seuilPercentSem=0.6,seuilPercentWe=0.5) 
            #print "apres calcul_zipcode_reference "+str(date_day)
            #if ref==False: 
            #    if sqlContext.sql("select * from "+db+a+"zipcode_de_reference").count()!=0:
            #            ref=True
            #if ref==True:
            #ajouter_demenageurs(db=db,a='.',historisationDemenageur="historisation_demenageur",ajouterDemenageur="ajouter_Demenageur",day=date_day)
            #print "apres  ajouter_demenageurs "+str(date_day)
            calcul_zipcode_de_reference_semaine1(db=db,a='.',historisationDemenageur="historisation_demenageur",\
                day=date_day,seuilTotalsem=3,seuilTotalwe=1,seuilPercentSem=0.6,seuilPercentWe=0.5)
            print "apres calcul_zipcode_de_reference_semaine1 "+str(date_day)
        else:
            traitement_donnees_du_jour(db=db,a='.',in_jour="user_zipmajoritaire_",zipcodeDeReference="zipcode_de_reference",day=date_day)
            print "apres traitement_donnees_du_jour "+str(date_day)
            test_user_qui_ont_un_zipcode_reference(db=db,a='.',atester="a_Tester",donneesDemenageurs="donnees_demenageurs",day=date_day)
            print "apres test_user_qui_ont_un_zipcode_reference "+str(date_day)
            ajouterHistorisationReference(db=db,a='.',ajouterReference="ajouter_Reference",historisationReference="historisation_reference",day=date_day)
            print "apres ajouterHistorisationReference "+str(date_day)
            misAJourListeCookie_a_Partir_de_la_table_ajouterReference(db=db,a='.',ajouterReference="ajouter_reference",listeCookie="liste_cookie",day=date_day)
            print "apres misAJourListeCookie_a_Partir_de_la_table_ajouterReference "+str(date_day)
            #if nb_day>=18:
            calcul_zipcode_reference(db=db,a='.',historisationReference="historisation_reference",listeCookie="liste_cookie",\
            day=date_day,seuilNBJour=30,SeuilTotalSemaine=18,seuilPercentSem=0.6,seuilPercentWe=0.5) 
            print "apres calcul_zipcode_reference "+str(date_day)
            #if ref==False: 
            #    if sqlContext.sql("select * from "+db+a+"zipcode_de_reference").count()!=0:
            #            ref=True
            #if ref==True:
            ajouter_demenageurs(db=db,a='.',historisationDemenageur="historisation_demenageur",ajouterDemenageur="ajouter_Demenageur",day=date_day)
            print "apres  ajouter_demenageurs "+str(date_day)
            calcul_zipcode_de_reference_semaine1(db=db,a='.',historisationDemenageur="historisation_demenageur",\
                day=date_day,seuilTotalsem=3,seuilTotalwe=1,seuilPercentSem=0.6,seuilPercentWe=0.5)
            print "apres calcul_zipcode_de_reference_semaine1 "+str(date_day)
            waitingOrder(waitingTime=30)
        print "Finish day "+str(date_day)+" en "+str((time.time()-dt)/60)+' minutes'
        
#filtrage_donnees_demenageurs_pour_garder_potentiel_demenageur()
sc.stop()
    