package main

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

const dir = "c://Media/sensors_messages/csv"
const dataSourceName = "root:Police04@tcp(127.0.0.1:3306)/sm"
const driverName = "mysql"

func pmain() {
	reporting()
	//mail()
	/*
		tbl := table.Table{
			ID:    "simpletable",
			Style: table.StyleSimple,
			Rows: [][]string{
				{"foo", "bar"},
				{"1", "2"}}}

		output := table.SimpleTable(tbl)
		fmt.Println(output)

		err := tbl.WriteCSV("filename.csv")
		if err != nil {
			log.Fatal(err)
		}
		err = tbl.WriteXLSX("filename.xlsx", "test")
		if err != nil {
			log.Fatal(err)
		}
	*/
}

func extractSql2Excel(db *sql.DB, sql string, titre string) {
	log.Println("Debut ", titre)
	row, err := db.Query(sql)
	if err != nil {
		log.Fatal(err)
	}

	extractFilenameFormat := fmt.Sprintf("c:\\Media\\dev\\messageWX\\dump\\%v - %s.xlsx", time.Now().Format("2006-01-02"), titre)
	sql2excel(row, extractFilenameFormat)
	row.Close()

	log.Println("Fin   ", titre)
}

func reporting() {
	db, err := sql.Open(driverName, dataSourceName)

	if err != nil {
		log.Fatal(err)
	}

	extractSql2Excel(db,
		"select bat, etg, jour as date, numerojoursemaine, month(jour) as mois, annee, numerosemaine, avg( COALESCE(t08, 0)+ COALESCE(t09, 0)+ COALESCE(t10, 0)+ COALESCE(t11, 0)+ COALESCE(t12, 0)+ COALESCE(t13, 0)+ COALESCE(t14, 0)+ COALESCE(t15, 0)+ COALESCE(t16, 0)+ COALESCE(t17, 0)+ COALESCE(t18, 0) ) as moyenne_jour, avg( COALESCE(t08, 0) ) as moyenne_t08, avg( COALESCE(t09, 0) ) as moyenne_t09, avg( COALESCE(t10, 0) ) as moyenne_t10, avg( COALESCE(t11, 0) ) as moyenne_t11, avg( COALESCE(t12, 0) ) as moyenne_t12, avg( COALESCE(t13, 0) ) as moyenne_t13, avg( COALESCE(t14, 0) ) as moyenne_t14, avg( COALESCE(t15, 0) ) as moyenne_t15, avg( COALESCE(t16, 0) ) as moyenne_t16, avg( COALESCE(t17, 0) ) as moyenne_t17, avg( COALESCE(t18, 0) ) as moyenne_t18, count(1) AS NB from sm.utilallglobal WHERE DATEDIFF(now(), jour) < 42 and  COALESCE(t08, 0)+ COALESCE(t09, 0)+ COALESCE(t10, 0)+ COALESCE(t11, 0)+ COALESCE(t12, 0)+ COALESCE(t13, 0)+ COALESCE(t14, 0)+ COALESCE(t15, 0)+ COALESCE(t16, 0)+ COALESCE(t17, 0)+ COALESCE(t18, 0) > 0 group by bat, etg, jour, numerojoursemaine, mois, annee, numerosemaine order by bat, etg, jour, numerojoursemaine, mois, annee, numerosemaine",
		"Durée moyenne d'occupation des postes occupés en seconde")

	extractSql2Excel(db,
		"select bat, etg, jour as date, numerojoursemaine, month(jour) as mois, annee, numerosemaine, round( avg( COALESCE(t08, 0)+ COALESCE(t09, 0)+ COALESCE(t10, 0)+ COALESCE(t11, 0)+ COALESCE(t12, 0)+ COALESCE(t13, 0)+ COALESCE(t14, 0)+ COALESCE(t15, 0)+ COALESCE(t16, 0)+ COALESCE(t17, 0)+ COALESCE(t18, 0) ) / 60) as moyenne_jour, round( avg( COALESCE(t08, 0) ) / 60) as moyenne_t08, round( avg( COALESCE(t09, 0) ) / 60) as moyenne_t09, round( avg( COALESCE(t10, 0) ) / 60) as moyenne_t10, round( avg( COALESCE(t11, 0) ) / 60) as moyenne_t11, round( avg( COALESCE(t12, 0) ) / 60) as moyenne_t12, round( avg( COALESCE(t13, 0) ) / 60) as moyenne_t13, round( avg( COALESCE(t14, 0) ) / 60) as moyenne_t14, round( avg( COALESCE(t15, 0) ) / 60) as moyenne_t15, round( avg( COALESCE(t16, 0) ) / 60) as moyenne_t16, round( avg( COALESCE(t17, 0) ) / 60) as moyenne_t17, round( avg( COALESCE(t18, 0) ) / 60) as moyenne_t18, count(1) AS NB from sm.utilallglobal WHERE DATEDIFF(now(), jour) < 42 and  COALESCE(t08, 0)+ COALESCE(t09, 0)+ COALESCE(t10, 0)+ COALESCE(t11, 0)+ COALESCE(t12, 0)+ COALESCE(t13, 0)+ COALESCE(t14, 0)+ COALESCE(t15, 0)+ COALESCE(t16, 0)+ COALESCE(t17, 0)+ COALESCE(t18, 0) > 0 group by bat, etg, jour, numerojoursemaine, mois, annee, numerosemaine order by bat, etg, jour, numerojoursemaine, mois, annee, numerosemaine",
		"Durée moyenne d'occupation des postes occupés en minute")

	extractSql2Excel(db,
		"select bat, etg, jour as date, numerojoursemaine, month(jour) as mois, annee, numerosemaine, avg( COALESCE(t08, 0)+ COALESCE(t09, 0)+ COALESCE(t10, 0)+ COALESCE(t11, 0)+ COALESCE(t12, 0)+ COALESCE(t13, 0)+ COALESCE(t14, 0)+ COALESCE(t15, 0)+ COALESCE(t16, 0)+ COALESCE(t17, 0)+ COALESCE(t18, 0) ) as moyenne_jour, avg( COALESCE(t08, 0) ) as moyenne_t08, avg( COALESCE(t09, 0) ) as moyenne_t09, avg( COALESCE(t10, 0) ) as moyenne_t10, avg( COALESCE(t11, 0) ) as moyenne_t11, avg( COALESCE(t12, 0) ) as moyenne_t12, avg( COALESCE(t13, 0) ) as moyenne_t13, avg( COALESCE(t14, 0) ) as moyenne_t14, avg( COALESCE(t15, 0) ) as moyenne_t15, avg( COALESCE(t16, 0) ) as moyenne_t16, avg( COALESCE(t17, 0) ) as moyenne_t17, avg( COALESCE(t18, 0) ) as moyenne_t18, count(1) AS NB from sm.utilallglobal WHERE DATEDIFF(now(), jour) < 42  group by bat, etg, jour, numerojoursemaine, mois, annee, numerosemaine order by bat, etg, jour, numerojoursemaine, mois, annee, numerosemaine",
		"Durée moyenne d'occupation des postes occupés versus l'intégralité des postes")

	extractSql2Excel(db,
		"select bat, etg, jour as date, numerojoursemaine, month(jour) as mois, annee, numerosemaine, round( avg( COALESCE(t08, 0)+ COALESCE(t09, 0)+ COALESCE(t10, 0)+ COALESCE(t11, 0)+ COALESCE(t12, 0)+ COALESCE(t13, 0)+ COALESCE(t14, 0)+ COALESCE(t15, 0)+ COALESCE(t16, 0)+ COALESCE(t17, 0)+ COALESCE(t18, 0) ) / 60) as moyenne_jour, round( avg( COALESCE(t08, 0) ) / 60) as moyenne_t08, round( avg( COALESCE(t09, 0) ) / 60) as moyenne_t09, round( avg( COALESCE(t10, 0) ) / 60) as moyenne_t10, round( avg( COALESCE(t11, 0) ) / 60) as moyenne_t11, round( avg( COALESCE(t12, 0) ) / 60) as moyenne_t12, round( avg( COALESCE(t13, 0) ) / 60) as moyenne_t13, round( avg( COALESCE(t14, 0) ) / 60) as moyenne_t14, round( avg( COALESCE(t15, 0) ) / 60) as moyenne_t15, round( avg( COALESCE(t16, 0) ) / 60) as moyenne_t16, round( avg( COALESCE(t17, 0) ) / 60) as moyenne_t17, round( avg( COALESCE(t18, 0) ) / 60) as moyenne_t18, count(1) AS NB from sm.utilallglobal WHERE DATEDIFF(now(), jour) < 42  group by bat, etg, jour, numerojoursemaine, mois, annee, numerosemaine order by bat, etg, jour, numerojoursemaine, mois, annee, numerosemaine",
		"Durée moyenne d'occupation des postes occupés versus l'intégralité des postes en minute")

	extractSql2Excel(db,
		"SELECT bat, etg, date(datedebut) as date, numerojoursemaine, mois, annee, numerosemaine, sum( CASE WHEN TIMEDIFF( TIME(dateFin), TIME(DateDebut) ) >= 15 * 60 THEN 1 END ) AS 't015', sum( CASE WHEN TIMEDIFF( TIME(dateFin), TIME(DateDebut) ) >= 30 * 60 THEN 1 END ) AS 't030', sum( CASE WHEN TIMEDIFF( TIME(dateFin), TIME(DateDebut) ) >= 60 * 60 THEN 1 END ) AS 't060', sum( CASE WHEN TIMEDIFF( TIME(dateFin), TIME(DateDebut) ) >= 120 * 60 THEN 1 END ) AS 't120', sum( CASE WHEN TIMEDIFF( TIME(dateFin), TIME(DateDebut) ) >= 240 * 60 THEN 1 END ) AS 't240' FROM sm.utilisationall WHERE DATEDIFF(now(), date(datedebut)) < 42 and  TIMEDIFF > 12 * 60 group by bat, etg, date(datedebut), numerojoursemaine, mois, annee, numerosemaine order by bat, etg, date(datedebut), numerojoursemaine, mois, annee, numerosemaine",
		"Durée moyenne d'utilisation d'un poste sur postes occupés")

	extractSql2Excel(db,
		"select bat, etg, date, numerojoursemaine, mois, annee, numerosemaine, sum(t_matin_25) as t_matin_25, sum(t_matin_50) as t_matin_50, sum(t_matin_75) as t_matin_75, sum(t_matin)/ 3 / 60 / 60 as ratio_matin, sum(t_apresmidi_25) as t_apresmidi_25, sum(t_apresmidi_50) as t_apresmidi_50, sum(t_apresmidi_75) as t_apresmidi_75, sum(t_apresmidi)/ 3 / 60 / 60 as ratio_apresmidi, count(distinct sygfox) as nb_sygfox from ( SELECT sygfox, path, date(dateDebut) as date, numerojoursemaine, mois, annee, numerosemaine, bat, etg, CASE WHEN SUM(t_matin) > 25 / 100 * 3 * 60 * 60 then 1 else 0 end as t_matin_25, CASE WHEN SUM(t_matin) > 50 / 100 * 3 * 60 * 60 then 1 else 0 end as t_matin_50, CASE WHEN SUM(t_matin) > 75 / 100 * 3 * 60 * 60 then 1 else 0 end as t_matin_75, SUM(t_matin) as t_matin, CASE WHEN SUM(t_apresmidi) > 25 / 100 * 3 * 60 * 60 then 1 else 0 end as t_apresmidi_25, CASE WHEN SUM(t_apresmidi) > 50 / 100 * 3 * 60 * 60 then 1 else 0 end as t_apresmidi_50, CASE WHEN SUM(t_apresmidi) > 75 / 100 * 3 * 60 * 60 then 1 else 0 end as t_apresmidi_75, sum(t_apresmidi) as t_apresmidi from ( select sygfox, path, dateDebut, dateFin, time_to_sec(TIMEDIFF(dateFin, dateDebut)) as timediff, DAYOFWEEK(dateDebut) as numerojoursemaine, MONTH(dateDebut) as mois, year(dateDebut) as annee, (datediff(datedebut, concat('' + year(datedebut), '-01-01'))+ 7 - DAYOFWEEK(dateDebut)) div 7 as numerosemaine, case when (SUBSTRING_INDEX(SUBSTRING_INDEX(path, '/', 4), '/', -1) in ('F01', 'F02', 'F03', 'F04', 'F05')) then 'Opéra' else SUBSTRING_INDEX(SUBSTRING_INDEX(path, '/', 4), '/', -1) end AS bat, case when (SUBSTRING_INDEX(SUBSTRING_INDEX(path, '/', 4), '/', -1) in ('F01', 'F02', 'F03', 'F04', 'F05')) then SUBSTRING_INDEX(SUBSTRING_INDEX(path, '/', 4), '/', -1) else SUBSTRING_INDEX(SUBSTRING_INDEX(path, '/', 5), '/', -1) end AS etg, CASE WHEN (TIMEDIFF(TIME(dateFin), '12:29:59') >= 0) AND (TIMEDIFF('09:30:00', TIME(datedebut)) >= 0) THEN time_to_sec(TIMEDIFF('12:29:59', '09:30:00')) WHEN (TIMEDIFF(TIME(dateFin), '12:29:59') >= 0) AND (TIMEDIFF('12:29:59', TIME(datedebut)) >= 0) THEN time_to_sec(TIMEDIFF('12:29:59', TIME(datedebut))) WHEN (TIMEDIFF(TIME(dateFin), '09:30:00') >= 0) AND (TIMEDIFF('09:30:00', TIME(datedebut)) >= 0) THEN time_to_sec(TIMEDIFF(TIME(dateFin), '09:30:00')) WHEN (TIMEDIFF(TIME(dateFin), '09:30:00') >= 0) AND (TIMEDIFF('12:29:59', TIME(datedebut)) >= 0) THEN time_to_sec(TIMEDIFF(TIME(dateFin), TIME(datedebut))) ELSE time_to_sec(TIMEDIFF(TIME(dateFin), TIME(dateFin))) END AS 't_matin', CASE WHEN (TIMEDIFF(TIME(dateFin), '16:59:59') >= 0) AND (TIMEDIFF('14:00:00', TIME(datedebut)) >= 0) THEN time_to_sec(TIMEDIFF('16:59:59', '14:00:00')) WHEN (TIMEDIFF(TIME(dateFin), '16:59:59') >= 0) AND (TIMEDIFF('16:59:59', TIME(datedebut)) >= 0) THEN time_to_sec(TIMEDIFF('16:59:59', TIME(datedebut))) WHEN (TIMEDIFF(TIME(dateFin), '14:00:00') >= 0) AND (TIMEDIFF('14:00:00', TIME(datedebut)) >= 0) THEN time_to_sec(TIMEDIFF(TIME(dateFin), '14:00:00')) WHEN (TIMEDIFF(TIME(dateFin), '14:00:00') >= 0) AND (TIMEDIFF('16:59:59', TIME(datedebut)) >= 0) THEN time_to_sec(TIMEDIFF(TIME(dateFin), TIME(datedebut))) ELSE time_to_sec(TIMEDIFF(TIME(dateFin), TIME(dateFin))) END AS 't_apresmidi' from SM.utilisation where DATEDIFF(now(), datedebut) < 42 and time_to_sec(TIMEDIFF(TIME(dateFin), TIME(datedebut))) > 12 * 60 ) aaa group by sygfox, path, date(dateDebut), numerojoursemaine, mois, annee, numerosemaine, bat, etg ) aa where datediff(now(), date) < 42 group by bat, etg, date, numerojoursemaine, mois, annee, numerosemaine order by bat, etg, date, numerojoursemaine, mois, annee, numerosemaine",
		"Nombre de postes utilisés moins de 25 50 75 pourcent du temps sur une demi-journée")

	extractSql2Excel(db,
		"select bat, etg, date, numerojoursemaine, mois, annee, numerosemaine, ratio_poste_equi_9h, nb from ( select bat, etg, jour as date, numerojoursemaine, month(jour) as mois, annee, numerosemaine, avg( case when ( COALESCE(t08, 0)+ COALESCE(t09, 0)+ COALESCE(t10, 0)+ COALESCE(t11, 0)+ COALESCE(t12, 0)+ COALESCE(t13, 0)+ COALESCE(t14, 0)+ COALESCE(t15, 0)+ COALESCE(t16, 0)+ COALESCE(t17, 0)+ COALESCE(t18, 0) )> 9 * 60 * 60 then 1 else ( COALESCE(t08, 0)+ COALESCE(t09, 0)+ COALESCE(t10, 0)+ COALESCE(t11, 0)+ COALESCE(t12, 0)+ COALESCE(t13, 0)+ COALESCE(t14, 0)+ COALESCE(t15, 0)+ COALESCE(t16, 0)+ COALESCE(t17, 0)+ COALESCE(t18, 0) )/ 9 / 60 / 60 end ) as ratio_poste_equi_9h, count(1) as nb from sm.utilallglobal group by bat, etg, jour, numerojoursemaine, month(jour), annee, numerosemaine ) aa WHERE datediff(now(), date) < 42 order by bat, etg, date, numerojoursemaine, mois, annee, numerosemaine",
		"Taux moyen d'occupation rapporté à une journée de 9h (Durée Moyenne)")

	extractSql2Excel(db,
		"select bat, etg, date, numerojoursemaine, mois, annee, numerosemaine, ratio_poste_equi_9h, nb from ( select bat, etg, jour as date, numerojoursemaine, month(jour) as mois, annee, numerosemaine, sum( case when ( COALESCE(t08, 0)+ COALESCE(t09, 0)+ COALESCE(t10, 0)+ COALESCE(t11, 0)+ COALESCE(t12, 0)+ COALESCE(t13, 0)+ COALESCE(t14, 0)+ COALESCE(t15, 0)+ COALESCE(t16, 0)+ COALESCE(t17, 0)+ COALESCE(t18, 0) )> 9 * 60 * 60 then 1 else ( COALESCE(t08, 0)+ COALESCE(t09, 0)+ COALESCE(t10, 0)+ COALESCE(t11, 0)+ COALESCE(t12, 0)+ COALESCE(t13, 0)+ COALESCE(t14, 0)+ COALESCE(t15, 0)+ COALESCE(t16, 0)+ COALESCE(t17, 0)+ COALESCE(t18, 0) )/ 9 / 60 / 60 end ) as ratio_poste_equi_9h, count(1) as nb from sm.utilallglobal group by bat, etg, jour, numerojoursemaine, month(jour), annee, numerosemaine ) aa WHERE datediff(now(), date) < 42 order by bat, etg, date, numerojoursemaine, mois, annee, numerosemaine",
		"Taux moyen d'occupation rapporté à une journée de 9h (Somme durée)")

	extractSql2Excel(db,
		"select bat, etg, date, numerojoursemaine, mois, annee, numerosemaine, sup_1h, nb from ( select bat, etg, jour as date, numerojoursemaine, month(jour) as mois, annee, numerosemaine, sum(case when (COALESCE(t08, 0)+ COALESCE(t09, 0)+ COALESCE(t10, 0)+ COALESCE(t11, 0)+ COALESCE(t12, 0)+ COALESCE(t13, 0)+ COALESCE(t14, 0)+ COALESCE(t15, 0)+ COALESCE(t16, 0)+ COALESCE(t17, 0)+ COALESCE(t18, 0))> 60 * 60 then 1 else 0 end) as sup_1h, count(1) as nb from sm.utilallglobal group by bat, etg, jour, numerojoursemaine, month(jour), annee, numerosemaine ) aa where datediff(now(), date) < 42 and sup_1h / nb > 0.75 order by bat, etg, date, numerojoursemaine, mois, annee, numerosemaine",
		"Liste des saturation (75% de postes de travail occupés en simultané)")

	extractSql2Excel(db,
		"SELECT bat, etg, jour as date, numerojoursemaine, month(jour) as mois, annee, numerosemaine, sum(CASE when COALESCE(t08, 0) > 0 then 1 else 0 end) AS 't08', sum(CASE when COALESCE(t09, 0) > 0 then 1 else 0 end) AS 't09', sum(CASE when COALESCE(t10, 0) > 0 then 1 else 0 end) AS 't10', sum(CASE when COALESCE(t11, 0) > 0 then 1 else 0 end) AS 't11', sum(CASE when COALESCE(t12, 0) > 0 then 1 else 0 end) AS 't12', sum(CASE when COALESCE(t13, 0) > 0 then 1 else 0 end) AS 't13', sum(CASE when COALESCE(t14, 0) > 0 then 1 else 0 end) AS 't14', sum(CASE when COALESCE(t15, 0) > 0 then 1 else 0 end) AS 't15', sum(CASE when COALESCE(t16, 0) > 0 then 1 else 0 end) AS 't16', sum(CASE when COALESCE(t17, 0) > 0 then 1 else 0 end) AS 't17', sum(CASE when COALESCE(t18, 0) > 0 then 1 else 0 end) AS 't18', COUNT(1) AS NB from sm.utilallglobal WHERE datediff(now(), jour) < 42 and COALESCE(t08,0)+COALESCE(t09,0)+COALESCE(t10,0)+COALESCE(t11,0)+COALESCE(t12,0)+COALESCE(t13,0)+COALESCE(t14,0)+COALESCE(t15,0)+COALESCE(t16,0)+COALESCE(t17,0)+COALESCE(t18,0) > 0 group by bat, etg, jour, numerojoursemaine, month(jour), annee, numerosemaine order by bat, etg, jour, numerojoursemaine, month(jour), annee, numerosemaine",
		"Nombre de postes en fonctionnement")

	db.Close()
}
