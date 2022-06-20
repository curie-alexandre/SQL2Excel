package main

import (
	"archive/zip"
	"bufio"
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"io/fs"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"

	_ "github.com/go-sql-driver/mysql"

	_ "github.com/alexcesaro/log"
	"github.com/alexcesaro/log/stdlog"
)

type sensorMessage struct {
	Code                           string
	PathLocalisation               string
	Statut                         string
	DateDerniereModificationStatut string
	Client                         string
	TypeCapteur                    string
	TypeMessage                    string
	Timezone                       string
	HeureMessage                   string
	NCHM                           string
	Contenu                        string
	DetailMessage                  string
	NDerniereMAJ                   string
	Downlink                       string
	MP1TypeMessage                 string
	MP1Horodatage                  string
	MP1Detail                      string
	MP2TypeMessage                 string
	MP2Horodatage                  string
	MP2Detail                      string
	MP3TypeMessage                 string
	MP3Horodatage                  string
	MP3Detail                      string
	MP4TypeMessage                 string
	MP4Horodatage                  string
	MP4Detail                      string
}

/*
const dir = "c://Media/sensors_messages/csv"
const dataSourceName = "root:Police04@tcp(127.0.0.1:3306)/sm"
const driverName = "mysql"
*/
func ppmain() {

	db, err := sql.Open(driverName, dataSourceName)

	if err != nil {
		log.Fatal(err)
	}

	step01(db)

	step02(db)

	step03(db, "2022-06-19", "2022-06-30")

	step04(db, "2022-06-19", "2022-06-30")

	step07(db)

	logger.Info("FIN")
	reporting()
	mail()

	//step05(db)

	//step06(db, "2022-05-01", "2022-05-18")

	db.Close()
}

func step04(db *sql.DB, dateDebut string, dateFin string) {

	logger := stdlog.GetFromFlags()

	sqlDelete := "DELETE FROM msgs WHERE date_message between '" + dateDebut + "' AND '" + dateFin + "'"

	_, err := db.Exec(sqlDelete)

	if err != nil {
		logger.Info(sqlDelete)
		log.Fatal(err)
	}

	file, err := os.OpenFile(dir+"/test_"+dateDebut+"_"+dateFin+".txt", os.O_CREATE|os.O_WRONLY, 0644)

	if err != nil {
		log.Fatalf("failed creating file: %s", err)
	}

	datawriter := bufio.NewWriter(file)

	dateDebut2 := strings.ReplaceAll(dateDebut, "-", "/")
	dateFin2 := strings.ReplaceAll(dateFin, "-", "/")

	sql := "SELECT Sygfox, Path, Statut, Date_Statut, Client, Type_Capteur, Type_Message, Timezone, Date_Message, CAST(N_CHM as UNSIGNED) as N_CHM, Contenu, Detail_Message, N_MAJ, Downlink, Type_Message1, Date_Message1, Detail_Message1, Type_Message2, Date_Message2, Detail_Message2, Type_Message3, Date_Message3, Detail_Message3, Type_Message4, Date_Message4, Detail_Message4 FROM messageimport WHERE N_CHM <> '' AND N_CHM <> 'N/A' and date_message between '" + dateDebut2 + "' AND '" + dateFin2 + "' ORDER BY 1, LEFT(date_message, 10), CAST(N_CHM AS UNSIGNED)"

	res, err := db.Query(sql)

	if err != nil {
		log.Fatal(err)
	}
	var msgOld sensorMessage

	for res.Next() {
		var msg sensorMessage
		err := res.Scan(&msg.Code, &msg.PathLocalisation, &msg.Statut, &msg.DateDerniereModificationStatut, &msg.Client, &msg.TypeCapteur, &msg.TypeMessage, &msg.Timezone, &msg.HeureMessage, &msg.NCHM, &msg.Contenu, &msg.DetailMessage, &msg.NDerniereMAJ, &msg.Downlink, &msg.MP1TypeMessage, &msg.MP1Horodatage, &msg.MP1Detail, &msg.MP2TypeMessage, &msg.MP2Horodatage, &msg.MP2Detail, &msg.MP3TypeMessage, &msg.MP3Horodatage, &msg.MP3Detail, &msg.MP4TypeMessage, &msg.MP4Horodatage, &msg.MP4Detail)

		if err != nil {
			log.Fatal(err)
		}
		if msgOld.Code != msg.Code {

		} else {
			var msgNCHM int64
			var msgOldNCHM int64

			msgNCHM, _ = strconv.ParseInt(msg.NCHM, 0, 16)
			msgOldNCHM, _ = strconv.ParseInt(msgOld.NCHM, 0, 16)

			if msgNCHM == msgOldNCHM+1 {
				// on insere juste la ligne courante
				InsertLineMsg(db, datawriter, msg, msgOld, 0)
			} else if msgNCHM == msgOldNCHM+2 {
				// on ajoute msg1
				InsertLineMsg(db, datawriter, msg, msgOld, 1)
				// on insere juste la ligne courante
				InsertLineMsg(db, datawriter, msg, msgOld, 0)
			} else if msgNCHM == msgOldNCHM+3 {
				// on ajoute msg2
				InsertLineMsg(db, datawriter, msg, msgOld, 2)
				// on ajoute msg1
				InsertLineMsg(db, datawriter, msg, msgOld, 1)
				// on insere juste la ligne courante
				InsertLineMsg(db, datawriter, msg, msgOld, 0)
			} else if msgNCHM == msgOldNCHM+4 {
				// on ajoute msg3
				InsertLineMsg(db, datawriter, msg, msgOld, 3)
				// on ajoute msg2
				InsertLineMsg(db, datawriter, msg, msgOld, 2)
				// on ajoute msg1
				InsertLineMsg(db, datawriter, msg, msgOld, 1)
				// on insere juste la ligne courante
				InsertLineMsg(db, datawriter, msg, msgOld, 0)
			} else if msgNCHM == msgOldNCHM+5 {
				// on ajoute msg4
				InsertLineMsg(db, datawriter, msg, msgOld, 4)
				// on ajoute msg3
				InsertLineMsg(db, datawriter, msg, msgOld, 3)
				// on ajoute msg2
				InsertLineMsg(db, datawriter, msg, msgOld, 2)
				// on ajoute msg1
				InsertLineMsg(db, datawriter, msg, msgOld, 1)
				// on insere juste la ligne courante
				InsertLineMsg(db, datawriter, msg, msgOld, 0)
			} else {
				// on insere juste la ligne courante
				InsertLineMsg(db, datawriter, msg, msgOld, 0)
			}
		}
		msgOld = msg
	}
	datawriter.Flush()
	file.Close()

	res.Close()

	logger.Info(niv0)
	logger.Info(niv1)
	logger.Info(niv2)
	logger.Info(niv3)
	logger.Info(niv4)
}

func InsertLineMsg(db *sql.DB, datawriter *bufio.Writer, msg sensorMessage, msgOld sensorMessage, niveau int) {
	var msgNCHM int64

	msgNCHM, _ = strconv.ParseInt(msg.NCHM, 0, 16)
	if niveau == 0 {
		InsertLine(db, datawriter, msg.Code, msg.PathLocalisation, msg.TypeCapteur, msg.TypeMessage, msg.HeureMessage, msgNCHM, niveau)
	} else if niveau == 1 {
		if substr(msg.HeureMessage, 1, 10) == substr(msg.MP1Horodatage, 1, 10) {
			InsertLine(db, datawriter, msg.Code, msg.PathLocalisation, msg.TypeCapteur, msgOld.MP1TypeMessage, msgOld.MP1Horodatage, msgNCHM+1, niveau)
		}
	} else if niveau == 2 {
		if substr(msg.HeureMessage, 1, 10) == substr(msg.MP2Horodatage, 1, 10) {
			InsertLine(db, datawriter, msg.Code, msg.PathLocalisation, msg.TypeCapteur, msgOld.MP2TypeMessage, msgOld.MP2Horodatage, msgNCHM+2, niveau)
		}
	} else if niveau == 3 {
		if substr(msg.HeureMessage, 1, 10) == substr(msg.MP3Horodatage, 1, 10) {
			InsertLine(db, datawriter, msg.Code, msg.PathLocalisation, msg.TypeCapteur, msgOld.MP3TypeMessage, msgOld.MP3Horodatage, msgNCHM+3, niveau)
		}
	} else if niveau == 4 {
		if substr(msg.HeureMessage, 1, 10) == substr(msg.MP4Horodatage, 1, 10) {
			InsertLine(db, datawriter, msg.Code, msg.PathLocalisation, msg.TypeCapteur, msgOld.MP4TypeMessage, msgOld.MP4Horodatage, msgNCHM+4, niveau)
		}
	}
}

func InsertFileLine(datawriter *bufio.Writer, code string, localisation string, capteur string, typemsg string, heure string, nchm int64, niveau int) {
	datawriter.WriteString(fmt.Sprintf("%s,%s,%s,%s,%s,%d,%d\n", code, localisation, capteur, typemsg, heure, nchm, niveau))
}

func InsertDBLine(db *sql.DB, code string, localisation string, capteur string, typemsg string, heure string, nchm int64, niveau int) {
	if len(heure) > 0 {
		sqlInsert := fmt.Sprintf("insert into msgs (sigfox, path, type_capteur, type_message, date_message, n_chm, niveau) values ('%s', '%s', '%s', '%s', '%s', %d, %d)", code, localisation, capteur, typemsg, heure, nchm, niveau)

		_, err := db.Exec(sqlInsert)

		if err != nil {
			logger.Info(sqlInsert)
		}
	}
}

var niv0 = 0
var niv1 = 0
var niv2 = 0
var niv3 = 0
var niv4 = 0

func InsertLine(db *sql.DB, datawriter *bufio.Writer, code string, localisation string, capteur string, typemsg string, heure string, nchm int64, niveau int) {
	InsertFileLine(datawriter, code, localisation, capteur, typemsg, heure, nchm, niveau)
	InsertDBLine(db, code, localisation, capteur, typemsg, heure, nchm, niveau)
	switch niveau {
	case 0:
		niv0++
		break
	case 1:
		niv1++
		break
	case 2:
		niv2++
		break
	case 3:
		niv3++
		break
	case 4:
		niv4++
		break

	}
}

type Msg struct {
	sygfox       string
	path         string
	type_message string
	date_message string
}

func step03(db *sql.DB, dateDebut string, dateFin string) {
	logger := stdlog.GetFromFlags()

	sqlDelete := "DELETE FROM SM.UTILISATION WHERE dateDebut> '" + dateDebut + "' AND dateFin  < '" + dateFin + "' "

	_, err := db.Exec(sqlDelete)

	if err != nil {
		logger.Info(sqlDelete)
		log.Fatal(err)
	}

	sql := "SELECT sygfox, path, type_message, date_message FROM message WHERE type_Message in ('MCEO', 'MCEL') and Date_Message between '" + dateDebut + "' AND '" + dateFin + "' ORDER BY 2, 4"

	res, err := db.Query(sql)

	if err != nil {
		log.Fatal(err)
	}

	var oldMsgMCEO Msg
	var oldMsgMCEL Msg
	var oldMsg Msg

	oldMsgMCEO.date_message = ""
	oldMsgMCEO.sygfox = ""
	oldMsgMCEO.type_message = ""
	oldMsgMCEO.path = ""

	oldMsgMCEL.date_message = ""
	oldMsgMCEL.sygfox = ""
	oldMsgMCEL.type_message = ""
	oldMsgMCEL.path = ""

	oldMsg.date_message = ""
	oldMsg.sygfox = ""
	oldMsg.type_message = ""
	oldMsg.path = ""

	var oldDate string = ""

	for res.Next() {
		var msg Msg
		err := res.Scan(&msg.sygfox, &msg.path, &msg.type_message, &msg.date_message)

		if err != nil {
			log.Fatal(err)
		}

		if oldMsg.sygfox != msg.sygfox {
			oldMsg.sygfox = ""
			oldMsg.type_message = ""
			oldMsg.date_message = ""
			oldDate = ""
		}

		if oldDate != substr(msg.date_message, 0, 10) {
			oldDate = substr(msg.date_message, 0, 10)
			oldMsg.sygfox = msg.sygfox
			oldMsg.date_message = ""
			oldMsg.type_message = ""
		}

		if msg.type_message == "MCEL" && oldMsg.type_message == "" {
			logger.Info(
				"MCEL merde en premier:",
				msg,
			)
		} else if msg.type_message == "MCEO" && oldMsg.type_message != "MCEO" {
			oldMsgMCEO = msg
		} else if msg.type_message == "MCEL" && oldMsg.type_message != "MCEL" {
			if oldMsgMCEO.date_message != msg.date_message {

				sqlInsert := "insert into utilisation (sygfox, path, dateDebut, dateFin) values ('" + oldMsgMCEO.sygfox + "', '" + oldMsgMCEO.path + "', '" + oldMsgMCEO.date_message + "', '" + msg.date_message + "')"

				_, err := db.Exec(sqlInsert)

				if err != nil {
					logger.Info(sqlInsert)
					log.Fatal(err)
				}
			}
			oldMsgMCEL = msg
		}

		oldMsg = msg
	}
	res.Close()
}

func step06(db *sql.DB, dateDebut string, dateFin string) {
	logger := stdlog.GetFromFlags()

	sqlDelete := "DELETE FROM SM.UTILISATIONS WHERE dateDebut> '" + dateDebut + "' AND dateFin  < '" + dateFin + "' "

	_, err := db.Exec(sqlDelete)

	if err != nil {
		logger.Info(sqlDelete)
		log.Fatal(err)
	}

	sql := "SELECT sigfox, path, type_message, date_message FROM MSGS WHERE type_Message in ('MCEO', 'MCEL') and Date_Message between '" + dateDebut + "' AND '" + dateFin + "' ORDER BY 2, 4"

	res, err := db.Query(sql)

	if err != nil {
		log.Fatal(err)
	}

	var oldMsgMCEO Msg
	var oldMsgMCEL Msg
	var oldMsg Msg

	oldMsgMCEO.date_message = ""
	oldMsgMCEO.sygfox = ""
	oldMsgMCEO.type_message = ""
	oldMsgMCEO.path = ""

	oldMsgMCEL.date_message = ""
	oldMsgMCEL.sygfox = ""
	oldMsgMCEL.type_message = ""
	oldMsgMCEL.path = ""

	oldMsg.date_message = ""
	oldMsg.sygfox = ""
	oldMsg.type_message = ""
	oldMsg.path = ""

	var oldDate string = ""

	for res.Next() {
		var msg Msg
		err := res.Scan(&msg.sygfox, &msg.path, &msg.type_message, &msg.date_message)

		if err != nil {
			log.Fatal(err)
		}

		if oldMsg.sygfox != msg.sygfox {
			oldMsg.sygfox = ""
			oldMsg.type_message = ""
			oldMsg.date_message = ""
			oldDate = ""
		}

		if oldDate != substr(msg.date_message, 0, 10) {
			oldDate = substr(msg.date_message, 0, 10)
			oldMsg.sygfox = msg.sygfox
			oldMsg.date_message = ""
			oldMsg.type_message = ""
		}

		if msg.type_message == "MCEL" && oldMsg.type_message == "" {
			logger.Info(
				"MCEL merde en premier:",
				msg,
			)
		} else if msg.type_message == "MCEO" && oldMsg.type_message != "MCEO" {
			oldMsgMCEO = msg
		} else if msg.type_message == "MCEL" && oldMsg.type_message != "MCEL" {
			if oldMsgMCEO.date_message != msg.date_message {

				sqlInsert := "insert into utilisations (sigfox, path, dateDebut, dateFin) values ('" + oldMsgMCEO.sygfox + "', '" + oldMsgMCEO.path + "', '" + oldMsgMCEO.date_message + "', '" + msg.date_message + "')"

				_, err := db.Exec(sqlInsert)

				if err != nil {
					logger.Info(sqlInsert)
					log.Fatal(err)
				}
			}
			oldMsgMCEL = msg
		}

		oldMsg = msg
	}
	logger.Info("FIN:")
	res.Close()
}

var logger = stdlog.GetFromFlags()

func doSQL(db *sql.DB, sql string) {
	logger.Info(sql)
	_, err := db.Exec(sql)
	if err == nil {
		return
	}
	log.Fatalf("Error %v running SQL: %s", err, sql)
}

func step02(db *sql.DB) {
	arraySQL := [6]string{
		"TRUNCATE TABLE message;",
		"INSERT into message ( Sygfox, Path, Statut, Client, Type_Capteur, Type_Message, Date_Message, Passage) SELECT Sygfox, Path, Statut, Client, Type_Capteur, Type_Message, STR_TO_DATE( Date_Message, '%Y/%m/%d %H:%i:%s') AS Date_Message, 0 FROM messageimport WHERE Date_Message <> '1900-01-00 00:00:00' AND Date_Message <> 'N/A' AND Date_Message <> '';",
		"INSERT into message ( Sygfox, Path, Statut, Client, Type_Capteur, Type_Message, Date_Message, Passage ) SELECT Sygfox, Path, Statut, Client, Type_Capteur, Type_Message1, STR_TO_DATE( Date_Message1,'%Y/%m/%d %H:%i:%s') AS Date_Message, 1 FROM messageimport WHERE Date_Message1 <> '1900-01-00 00:00:00' AND Date_Message1 <> 'N/A' AND Date_Message1 <> '';",
		"INSERT into message ( Sygfox, Path, Statut, Client, Type_Capteur, Type_Message, Date_Message, Passage ) SELECT Sygfox, Path, Statut, Client, Type_Capteur, Type_Message2, STR_TO_DATE( Date_Message2,'%Y/%m/%d %H:%i:%s') AS Date_Message, 2 FROM messageimport WHERE Date_Message2 <> '1900-01-00 00:00:00' AND Date_Message2 <> 'N/A' AND Date_Message2 <> '';",
		"INSERT into message ( Sygfox, Path, Statut, Client, Type_Capteur, Type_Message, Date_Message, Passage ) SELECT Sygfox, Path, Statut, Client, Type_Capteur, Type_Message3, STR_TO_DATE( Date_Message3,'%Y/%m/%d %H:%i:%s') AS Date_Message, 3 FROM messageimport WHERE Date_Message3 <> '1900-01-00 00:00:00' AND Date_Message3 <> 'N/A' AND Date_Message3 <> '';",
		"INSERT into message ( Sygfox, Path, Statut, Client, Type_Capteur, Type_Message, Date_Message, Passage ) SELECT Sygfox, Path, Statut, Client, Type_Capteur, Type_Message4, STR_TO_DATE( Date_Message4,'%Y/%m/%d %H:%i:%s') AS Date_Message, 4 FROM messageimport WHERE Date_Message4 <> '1900-01-00 00:00:00' AND Date_Message4 <> 'N/A' AND Date_Message4 <> '';",
	}

	for _, sql := range arraySQL {
		doSQL(db, sql)
	}
}

func step02OLD(db *sql.DB) {
	arraySQL := [7]string{
		"DROP TABLE message;",
		"CREATE TABLE message ( Sygfox VARCHAR(50) NULL DEFAULT NULL COLLATE 'utf8mb4_0900_ai_ci', Path VARCHAR(32) NULL DEFAULT NULL COLLATE 'utf8mb4_0900_ai_ci', Statut CHAR(10) NULL DEFAULT NULL COLLATE 'utf8mb4_0900_ai_ci', Client CHAR(3) NULL DEFAULT NULL COLLATE 'utf8mb4_0900_ai_ci', Type_Capteur VARCHAR(50) NULL DEFAULT NULL COLLATE 'utf8mb4_0900_ai_ci', Type_Message VARCHAR(5) NULL DEFAULT NULL COLLATE 'utf8mb4_0900_ai_ci', Date_Message DATETIME NULL DEFAULT NULL, Passage int) COLLATE='utf8mb4_0900_ai_ci' ENGINE=InnoDB;",
		"INSERT into message ( Sygfox, Path, Statut, Client, Type_Capteur, Type_Message, Date_Message, Passage) SELECT Sygfox, Path, Statut, Client, Type_Capteur, Type_Message, STR_TO_DATE( Date_Message, '%Y/%m/%d %H:%i:%s') AS Date_Message, 0 FROM messageimport WHERE Date_Message <> '1900-01-00 00:00:00' AND Date_Message <> 'N/A' AND Date_Message <> '';",
		"INSERT into message ( Sygfox, Path, Statut, Client, Type_Capteur, Type_Message, Date_Message, Passage ) SELECT Sygfox, Path, Statut, Client, Type_Capteur, Type_Message1, STR_TO_DATE( Date_Message1,'%Y/%m/%d %H:%i:%s') AS Date_Message, 1 FROM messageimport WHERE Date_Message1 <> '1900-01-00 00:00:00' AND Date_Message1 <> 'N/A' AND Date_Message1 <> '';",
		"INSERT into message ( Sygfox, Path, Statut, Client, Type_Capteur, Type_Message, Date_Message, Passage ) SELECT Sygfox, Path, Statut, Client, Type_Capteur, Type_Message2, STR_TO_DATE( Date_Message2,'%Y/%m/%d %H:%i:%s') AS Date_Message, 2 FROM messageimport WHERE Date_Message2 <> '1900-01-00 00:00:00' AND Date_Message2 <> 'N/A' AND Date_Message2 <> '';",
		"INSERT into message ( Sygfox, Path, Statut, Client, Type_Capteur, Type_Message, Date_Message, Passage ) SELECT Sygfox, Path, Statut, Client, Type_Capteur, Type_Message3, STR_TO_DATE( Date_Message3,'%Y/%m/%d %H:%i:%s') AS Date_Message, 3 FROM messageimport WHERE Date_Message3 <> '1900-01-00 00:00:00' AND Date_Message3 <> 'N/A' AND Date_Message3 <> '';",
		"INSERT into message ( Sygfox, Path, Statut, Client, Type_Capteur, Type_Message, Date_Message, Passage ) SELECT Sygfox, Path, Statut, Client, Type_Capteur, Type_Message4, STR_TO_DATE( Date_Message4,'%Y/%m/%d %H:%i:%s') AS Date_Message, 4 FROM messageimport WHERE Date_Message4 <> '1900-01-00 00:00:00' AND Date_Message4 <> 'N/A' AND Date_Message4 <> '';",
	}

	for _, sql := range arraySQL {
		doSQL(db, sql)
	}
}

func step07(db *sql.DB) {
	arraySQL := [8]string{
		"TRUNCATE table refCapteurAnnee ;",
		"INSERT INTO refCapteurAnnee SELECT transco.*, calendrier.*, DAYOFWEEK(calendrier.jour) as numerojoursemaine, (datediff(calendrier.jour, concat(''+year(calendrier.jour),'-01-01'))+7-DAYOFWEEK(calendrier.jour)) div 7  as numerosemaine, year(jour) as annee FROM transco, calendrier where ouvre=1 and calendrier.jour < now();",
		"TRUNCATE table utilisationAll ;",
		"INSERT INTO utilisationAll select sygfox, path, dateDebut, dateFin, time_to_sec(TIMEDIFF(dateFin, dateDebut)) as timediff, DAYOFWEEK(dateDebut) as numerojoursemaine, MONTH(dateDebut) as mois, year(dateDebut) as annee, (datediff(datedebut, concat(''+year(datedebut),'-01-01'))+7-DAYOFWEEK(dateDebut)) div 7 as numerosemaine, case when (SUBSTRING_INDEX(SUBSTRING_INDEX(path, '/', 4), '/', -1) in ('F01', 'F02', 'F03', 'F04', 'F05')) then 'Opéra' else SUBSTRING_INDEX(SUBSTRING_INDEX(path, '/', 4), '/', -1) end AS bat, case when (SUBSTRING_INDEX(SUBSTRING_INDEX(path, '/', 4), '/', -1) in ('F01', 'F02', 'F03', 'F04', 'F05')) then SUBSTRING_INDEX(SUBSTRING_INDEX(path, '/', 4), '/', -1) else SUBSTRING_INDEX(SUBSTRING_INDEX(path, '/', 5), '/', -1) end AS etg, CASE WHEN (TIMEDIFF(TIME(dateFin), '08:59:59') >= 0) AND (TIMEDIFF('08:00:00', TIME(datedebut)) >= 0) THEN time_to_sec(TIMEDIFF( '08:59:59', '08:00:00')) WHEN (TIMEDIFF(TIME(dateFin), '08:59:59') >= 0) AND (TIMEDIFF('08:59:59', TIME(datedebut)) >= 0) THEN time_to_sec(TIMEDIFF( '08:59:59', TIME(datedebut))) WHEN (TIMEDIFF(TIME(dateFin), '08:00:00') >= 0) AND (TIMEDIFF('08:00:00', TIME(datedebut)) >= 0) THEN time_to_sec(TIMEDIFF( TIME(dateFin), '08:00:00')) WHEN (TIMEDIFF(TIME(dateFin), '08:00:00') >= 0) AND (TIMEDIFF('08:59:59', TIME(datedebut)) >= 0) THEN time_to_sec(TIMEDIFF( TIME(dateFin), TIME(datedebut))) ELSE time_to_sec(TIMEDIFF(TIME(dateFin), TIME(dateFin))) END AS 't08', CASE WHEN (TIMEDIFF(TIME(dateFin), '09:59:59') >= 0) AND (TIMEDIFF('09:00:00', TIME(datedebut)) >= 0) THEN time_to_sec(TIMEDIFF( '09:59:59', '09:00:00')) WHEN (TIMEDIFF(TIME(dateFin), '09:59:59') >= 0) AND (TIMEDIFF('09:59:59', TIME(datedebut)) >= 0) THEN time_to_sec(TIMEDIFF( '09:59:59', TIME(datedebut))) WHEN (TIMEDIFF(TIME(dateFin), '09:00:00') >= 0) AND (TIMEDIFF('09:00:00', TIME(datedebut)) >= 0) THEN time_to_sec(TIMEDIFF( TIME(dateFin), '09:00:00')) WHEN (TIMEDIFF(TIME(dateFin), '09:00:00') >= 0) AND (TIMEDIFF('09:59:59', TIME(datedebut)) >= 0) THEN time_to_sec(TIMEDIFF( TIME(dateFin), TIME(datedebut))) ELSE time_to_sec(TIMEDIFF(TIME(dateFin), TIME(dateFin))) END AS 't09', CASE WHEN (TIMEDIFF(TIME(dateFin), '10:59:59') >= 0) AND (TIMEDIFF('10:00:00', TIME(datedebut)) >= 0) THEN time_to_sec(TIMEDIFF( '10:59:59', '10:00:00')) WHEN (TIMEDIFF(TIME(dateFin), '10:59:59') >= 0) AND (TIMEDIFF('10:59:59', TIME(datedebut)) >= 0) THEN time_to_sec(TIMEDIFF( '10:59:59', TIME(datedebut))) WHEN (TIMEDIFF(TIME(dateFin), '10:00:00') >= 0) AND (TIMEDIFF('10:00:00', TIME(datedebut)) >= 0) THEN time_to_sec(TIMEDIFF( TIME(dateFin), '10:00:00')) WHEN (TIMEDIFF(TIME(dateFin), '10:00:00') >= 0) AND (TIMEDIFF('10:59:59', TIME(datedebut)) >= 0) THEN time_to_sec(TIMEDIFF( TIME(dateFin), TIME(datedebut))) ELSE time_to_sec(TIMEDIFF(TIME(dateFin), TIME(dateFin))) END AS 't10', CASE WHEN (TIMEDIFF(TIME(dateFin), '11:59:59') >= 0) AND (TIMEDIFF('11:00:00', TIME(datedebut)) >= 0) THEN time_to_sec(TIMEDIFF( '11:59:59', '11:00:00')) WHEN (TIMEDIFF(TIME(dateFin), '11:59:59') >= 0) AND (TIMEDIFF('11:59:59', TIME(datedebut)) >= 0) THEN time_to_sec(TIMEDIFF( '11:59:59', TIME(datedebut))) WHEN (TIMEDIFF(TIME(dateFin), '11:00:00') >= 0) AND (TIMEDIFF('11:00:00', TIME(datedebut)) >= 0) THEN time_to_sec(TIMEDIFF( TIME(dateFin), '11:00:00')) WHEN (TIMEDIFF(TIME(dateFin), '11:00:00') >= 0) AND (TIMEDIFF('11:59:59', TIME(datedebut)) >= 0) THEN time_to_sec(TIMEDIFF( TIME(dateFin), TIME(datedebut))) ELSE time_to_sec(TIMEDIFF(TIME(dateFin), TIME(dateFin))) END AS 't11', CASE WHEN (TIMEDIFF(TIME(dateFin), '12:59:59') >= 0) AND (TIMEDIFF('12:00:00', TIME(datedebut)) >= 0) THEN time_to_sec(TIMEDIFF( '12:59:59', '12:00:00')) WHEN (TIMEDIFF(TIME(dateFin), '12:59:59') >= 0) AND (TIMEDIFF('12:59:59', TIME(datedebut)) >= 0) THEN time_to_sec(TIMEDIFF( '12:59:59', TIME(datedebut))) WHEN (TIMEDIFF(TIME(dateFin), '12:00:00') >= 0) AND (TIMEDIFF('12:00:00', TIME(datedebut)) >= 0) THEN time_to_sec(TIMEDIFF( TIME(dateFin), '12:00:00')) WHEN (TIMEDIFF(TIME(dateFin), '12:00:00') >= 0) AND (TIMEDIFF('12:59:59', TIME(datedebut)) >= 0) THEN time_to_sec(TIMEDIFF( TIME(dateFin), TIME(datedebut))) ELSE time_to_sec(TIMEDIFF(TIME(dateFin), TIME(dateFin))) END AS 't12', CASE WHEN (TIMEDIFF(TIME(dateFin), '13:59:59') >= 0) AND (TIMEDIFF('13:00:00', TIME(datedebut)) >= 0) THEN time_to_sec(TIMEDIFF( '13:59:59', '13:00:00')) WHEN (TIMEDIFF(TIME(dateFin), '13:59:59') >= 0) AND (TIMEDIFF('13:59:59', TIME(datedebut)) >= 0) THEN time_to_sec(TIMEDIFF( '13:59:59', TIME(datedebut))) WHEN (TIMEDIFF(TIME(dateFin), '13:00:00') >= 0) AND (TIMEDIFF('13:00:00', TIME(datedebut)) >= 0) THEN time_to_sec(TIMEDIFF( TIME(dateFin), '13:00:00')) WHEN (TIMEDIFF(TIME(dateFin), '13:00:00') >= 0) AND (TIMEDIFF('13:59:59', TIME(datedebut)) >= 0) THEN time_to_sec(TIMEDIFF( TIME(dateFin), TIME(datedebut))) ELSE time_to_sec(TIMEDIFF(TIME(dateFin), TIME(dateFin))) END AS 't13', CASE WHEN (TIMEDIFF(TIME(dateFin), '14:59:59') >= 0) AND (TIMEDIFF('14:00:00', TIME(datedebut)) >= 0) THEN time_to_sec(TIMEDIFF( '14:59:59', '14:00:00')) WHEN (TIMEDIFF(TIME(dateFin), '14:59:59') >= 0) AND (TIMEDIFF('14:59:59', TIME(datedebut)) >= 0) THEN time_to_sec(TIMEDIFF( '14:59:59', TIME(datedebut))) WHEN (TIMEDIFF(TIME(dateFin), '14:00:00') >= 0) AND (TIMEDIFF('14:00:00', TIME(datedebut)) >= 0) THEN time_to_sec(TIMEDIFF( TIME(dateFin), '14:00:00')) WHEN (TIMEDIFF(TIME(dateFin), '14:00:00') >= 0) AND (TIMEDIFF('14:59:59', TIME(datedebut)) >= 0) THEN time_to_sec(TIMEDIFF( TIME(dateFin), TIME(datedebut))) ELSE time_to_sec(TIMEDIFF(TIME(dateFin), TIME(dateFin))) END AS 't14', CASE WHEN (TIMEDIFF(TIME(dateFin), '15:59:59') >= 0) AND (TIMEDIFF('15:00:00', TIME(datedebut)) >= 0) THEN time_to_sec(TIMEDIFF( '15:59:59', '15:00:00')) WHEN (TIMEDIFF(TIME(dateFin), '15:59:59') >= 0) AND (TIMEDIFF('15:59:59', TIME(datedebut)) >= 0) THEN time_to_sec(TIMEDIFF( '15:59:59', TIME(datedebut))) WHEN (TIMEDIFF(TIME(dateFin), '15:00:00') >= 0) AND (TIMEDIFF('15:00:00', TIME(datedebut)) >= 0) THEN time_to_sec(TIMEDIFF( TIME(dateFin), '15:00:00')) WHEN (TIMEDIFF(TIME(dateFin), '15:00:00') >= 0) AND (TIMEDIFF('15:59:59', TIME(datedebut)) >= 0) THEN time_to_sec(TIMEDIFF( TIME(dateFin), TIME(datedebut))) ELSE time_to_sec(TIMEDIFF(TIME(dateFin), TIME(dateFin))) END AS 't15', CASE WHEN (TIMEDIFF(TIME(dateFin), '16:59:59') >= 0) AND (TIMEDIFF('16:00:00', TIME(datedebut)) >= 0) THEN time_to_sec(TIMEDIFF( '16:59:59', '16:00:00')) WHEN (TIMEDIFF(TIME(dateFin), '16:59:59') >= 0) AND (TIMEDIFF('16:59:59', TIME(datedebut)) >= 0) THEN time_to_sec(TIMEDIFF( '16:59:59', TIME(datedebut))) WHEN (TIMEDIFF(TIME(dateFin), '16:00:00') >= 0) AND (TIMEDIFF('16:00:00', TIME(datedebut)) >= 0) THEN time_to_sec(TIMEDIFF( TIME(dateFin), '16:00:00')) WHEN (TIMEDIFF(TIME(dateFin), '16:00:00') >= 0) AND (TIMEDIFF('16:59:59', TIME(datedebut)) >= 0) THEN time_to_sec(TIMEDIFF( TIME(dateFin), TIME(datedebut))) ELSE time_to_sec(TIMEDIFF(TIME(dateFin), TIME(dateFin))) END AS 't16', CASE WHEN (TIMEDIFF(TIME(dateFin), '17:59:59') >= 0) AND (TIMEDIFF('17:00:00', TIME(datedebut)) >= 0) THEN time_to_sec(TIMEDIFF( '17:59:59', '17:00:00')) WHEN (TIMEDIFF(TIME(dateFin), '17:59:59') >= 0) AND (TIMEDIFF('17:59:59', TIME(datedebut)) >= 0) THEN time_to_sec(TIMEDIFF( '17:59:59', TIME(datedebut))) WHEN (TIMEDIFF(TIME(dateFin), '17:00:00') >= 0) AND (TIMEDIFF('17:00:00', TIME(datedebut)) >= 0) THEN time_to_sec(TIMEDIFF( TIME(dateFin), '17:00:00')) WHEN (TIMEDIFF(TIME(dateFin), '17:00:00') >= 0) AND (TIMEDIFF('17:59:59', TIME(datedebut)) >= 0) THEN time_to_sec(TIMEDIFF( TIME(dateFin), TIME(datedebut))) ELSE time_to_sec(TIMEDIFF(TIME(dateFin), TIME(dateFin))) END AS 't17', CASE WHEN (TIMEDIFF(TIME(dateFin), '18:59:59') >= 0) AND (TIMEDIFF('18:00:00', TIME(datedebut)) >= 0) THEN time_to_sec(TIMEDIFF( '18:59:59', '18:00:00')) WHEN (TIMEDIFF(TIME(dateFin), '18:59:59') >= 0) AND (TIMEDIFF('18:59:59', TIME(datedebut)) >= 0) THEN time_to_sec(TIMEDIFF( '18:59:59', TIME(datedebut))) WHEN (TIMEDIFF(TIME(dateFin), '18:00:00') >= 0) AND (TIMEDIFF('18:00:00', TIME(datedebut)) >= 0) THEN time_to_sec(TIMEDIFF( TIME(dateFin), '18:00:00')) WHEN (TIMEDIFF(TIME(dateFin), '18:00:00') >= 0) AND (TIMEDIFF('18:59:59', TIME(datedebut)) >= 0) THEN time_to_sec(TIMEDIFF( TIME(dateFin), TIME(datedebut))) ELSE time_to_sec(TIMEDIFF(TIME(dateFin), TIME(dateFin))) END AS 't18' from utilisation where time_to_sec(TIMEDIFF( TIME(dateFin), TIME(datedebut))) > 12*60; ",
		"TRUNCATE table utilisationAllGlobal;",
		"INSERT INTO utilisationAllGlobal select sygfox, path, sum(timediff) as timediff, numerojoursemaine, mois, annee, numerosemaine, bat, etg, sum(t08) as t08, sum(t09) as t09, sum(t10) as t10, sum(t11) as t11, sum(t12) as t12, sum(t13) as t13, sum(t14) as t14, sum(t15) as t15, sum(t16) as t16, sum(t17) as t17, sum(t18) as t18 FROM utilisationAll GROUP BY sygfox, path, numerojoursemaine, mois, annee, numerosemaine, bat, etg;",
		"TRUNCATE table utilAllGlobal;",
		"INSERT INTO utilAllGlobal SELECT refCapteurAnnee.*, timediff as temps, t08,t09,t10,t11,t12,t13,t14,t15,t16,t17,t18 FROM refCapteurAnnee left JOIN utilisationallglobal ON refCapteurAnnee.sygfox = utilisationallglobal.sygfox and refCapteurAnnee.annee = utilisationallglobal.annee and refCapteurAnnee.numerosemaine = utilisationallglobal.numerosemaine and refCapteurAnnee.numerojoursemaine = utilisationallglobal.numerojoursemaine ;",
	}

	for _, sql := range arraySQL {
		doSQL(db, sql)
	}
}

func step07OLD(db *sql.DB) {
	arraySQL := [8]string{
		"drop table refCapteurAnnee ;",
		"create table refCapteurAnnee as SELECT transco.*, calendrier.*, DAYOFWEEK(calendrier.jour) as numerojoursemaine, (datediff(calendrier.jour, concat(''+year(calendrier.jour),'-01-01'))+7-DAYOFWEEK(calendrier.jour)) div 7  as numerosemaine, year(jour) as annee FROM transco, calendrier where ouvre=1 and calendrier.jour < now();",
		"drop table utilisationAll ;",
		"create table utilisationAll as select sygfox, path, dateDebut, dateFin, time_to_sec(TIMEDIFF(dateFin, dateDebut)) as timediff, DAYOFWEEK(dateDebut) as numerojoursemaine, MONTH(dateDebut) as mois, year(dateDebut) as annee, (datediff(datedebut, concat(''+year(datedebut),'-01-01'))+7-DAYOFWEEK(dateDebut)) div 7 as numerosemaine, case when (SUBSTRING_INDEX(SUBSTRING_INDEX(path, '/', 4), '/', -1) in ('F01', 'F02', 'F03', 'F04', 'F05')) then 'Opéra' else SUBSTRING_INDEX(SUBSTRING_INDEX(path, '/', 4), '/', -1) end AS bat, case when (SUBSTRING_INDEX(SUBSTRING_INDEX(path, '/', 4), '/', -1) in ('F01', 'F02', 'F03', 'F04', 'F05')) then SUBSTRING_INDEX(SUBSTRING_INDEX(path, '/', 4), '/', -1) else SUBSTRING_INDEX(SUBSTRING_INDEX(path, '/', 5), '/', -1) end AS etg, CASE WHEN (TIMEDIFF(TIME(dateFin), '08:59:59') >= 0) AND (TIMEDIFF('08:00:00', TIME(datedebut)) >= 0) THEN time_to_sec(TIMEDIFF( '08:59:59', '08:00:00')) WHEN (TIMEDIFF(TIME(dateFin), '08:59:59') >= 0) AND (TIMEDIFF('08:59:59', TIME(datedebut)) >= 0) THEN time_to_sec(TIMEDIFF( '08:59:59', TIME(datedebut))) WHEN (TIMEDIFF(TIME(dateFin), '08:00:00') >= 0) AND (TIMEDIFF('08:00:00', TIME(datedebut)) >= 0) THEN time_to_sec(TIMEDIFF( TIME(dateFin), '08:00:00')) WHEN (TIMEDIFF(TIME(dateFin), '08:00:00') >= 0) AND (TIMEDIFF('08:59:59', TIME(datedebut)) >= 0) THEN time_to_sec(TIMEDIFF( TIME(dateFin), TIME(datedebut))) ELSE time_to_sec(TIMEDIFF(TIME(dateFin), TIME(dateFin))) END AS 't08', CASE WHEN (TIMEDIFF(TIME(dateFin), '09:59:59') >= 0) AND (TIMEDIFF('09:00:00', TIME(datedebut)) >= 0) THEN time_to_sec(TIMEDIFF( '09:59:59', '09:00:00')) WHEN (TIMEDIFF(TIME(dateFin), '09:59:59') >= 0) AND (TIMEDIFF('09:59:59', TIME(datedebut)) >= 0) THEN time_to_sec(TIMEDIFF( '09:59:59', TIME(datedebut))) WHEN (TIMEDIFF(TIME(dateFin), '09:00:00') >= 0) AND (TIMEDIFF('09:00:00', TIME(datedebut)) >= 0) THEN time_to_sec(TIMEDIFF( TIME(dateFin), '09:00:00')) WHEN (TIMEDIFF(TIME(dateFin), '09:00:00') >= 0) AND (TIMEDIFF('09:59:59', TIME(datedebut)) >= 0) THEN time_to_sec(TIMEDIFF( TIME(dateFin), TIME(datedebut))) ELSE time_to_sec(TIMEDIFF(TIME(dateFin), TIME(dateFin))) END AS 't09', CASE WHEN (TIMEDIFF(TIME(dateFin), '10:59:59') >= 0) AND (TIMEDIFF('10:00:00', TIME(datedebut)) >= 0) THEN time_to_sec(TIMEDIFF( '10:59:59', '10:00:00')) WHEN (TIMEDIFF(TIME(dateFin), '10:59:59') >= 0) AND (TIMEDIFF('10:59:59', TIME(datedebut)) >= 0) THEN time_to_sec(TIMEDIFF( '10:59:59', TIME(datedebut))) WHEN (TIMEDIFF(TIME(dateFin), '10:00:00') >= 0) AND (TIMEDIFF('10:00:00', TIME(datedebut)) >= 0) THEN time_to_sec(TIMEDIFF( TIME(dateFin), '10:00:00')) WHEN (TIMEDIFF(TIME(dateFin), '10:00:00') >= 0) AND (TIMEDIFF('10:59:59', TIME(datedebut)) >= 0) THEN time_to_sec(TIMEDIFF( TIME(dateFin), TIME(datedebut))) ELSE time_to_sec(TIMEDIFF(TIME(dateFin), TIME(dateFin))) END AS 't10', CASE WHEN (TIMEDIFF(TIME(dateFin), '11:59:59') >= 0) AND (TIMEDIFF('11:00:00', TIME(datedebut)) >= 0) THEN time_to_sec(TIMEDIFF( '11:59:59', '11:00:00')) WHEN (TIMEDIFF(TIME(dateFin), '11:59:59') >= 0) AND (TIMEDIFF('11:59:59', TIME(datedebut)) >= 0) THEN time_to_sec(TIMEDIFF( '11:59:59', TIME(datedebut))) WHEN (TIMEDIFF(TIME(dateFin), '11:00:00') >= 0) AND (TIMEDIFF('11:00:00', TIME(datedebut)) >= 0) THEN time_to_sec(TIMEDIFF( TIME(dateFin), '11:00:00')) WHEN (TIMEDIFF(TIME(dateFin), '11:00:00') >= 0) AND (TIMEDIFF('11:59:59', TIME(datedebut)) >= 0) THEN time_to_sec(TIMEDIFF( TIME(dateFin), TIME(datedebut))) ELSE time_to_sec(TIMEDIFF(TIME(dateFin), TIME(dateFin))) END AS 't11', CASE WHEN (TIMEDIFF(TIME(dateFin), '12:59:59') >= 0) AND (TIMEDIFF('12:00:00', TIME(datedebut)) >= 0) THEN time_to_sec(TIMEDIFF( '12:59:59', '12:00:00')) WHEN (TIMEDIFF(TIME(dateFin), '12:59:59') >= 0) AND (TIMEDIFF('12:59:59', TIME(datedebut)) >= 0) THEN time_to_sec(TIMEDIFF( '12:59:59', TIME(datedebut))) WHEN (TIMEDIFF(TIME(dateFin), '12:00:00') >= 0) AND (TIMEDIFF('12:00:00', TIME(datedebut)) >= 0) THEN time_to_sec(TIMEDIFF( TIME(dateFin), '12:00:00')) WHEN (TIMEDIFF(TIME(dateFin), '12:00:00') >= 0) AND (TIMEDIFF('12:59:59', TIME(datedebut)) >= 0) THEN time_to_sec(TIMEDIFF( TIME(dateFin), TIME(datedebut))) ELSE time_to_sec(TIMEDIFF(TIME(dateFin), TIME(dateFin))) END AS 't12', CASE WHEN (TIMEDIFF(TIME(dateFin), '13:59:59') >= 0) AND (TIMEDIFF('13:00:00', TIME(datedebut)) >= 0) THEN time_to_sec(TIMEDIFF( '13:59:59', '13:00:00')) WHEN (TIMEDIFF(TIME(dateFin), '13:59:59') >= 0) AND (TIMEDIFF('13:59:59', TIME(datedebut)) >= 0) THEN time_to_sec(TIMEDIFF( '13:59:59', TIME(datedebut))) WHEN (TIMEDIFF(TIME(dateFin), '13:00:00') >= 0) AND (TIMEDIFF('13:00:00', TIME(datedebut)) >= 0) THEN time_to_sec(TIMEDIFF( TIME(dateFin), '13:00:00')) WHEN (TIMEDIFF(TIME(dateFin), '13:00:00') >= 0) AND (TIMEDIFF('13:59:59', TIME(datedebut)) >= 0) THEN time_to_sec(TIMEDIFF( TIME(dateFin), TIME(datedebut))) ELSE time_to_sec(TIMEDIFF(TIME(dateFin), TIME(dateFin))) END AS 't13', CASE WHEN (TIMEDIFF(TIME(dateFin), '14:59:59') >= 0) AND (TIMEDIFF('14:00:00', TIME(datedebut)) >= 0) THEN time_to_sec(TIMEDIFF( '14:59:59', '14:00:00')) WHEN (TIMEDIFF(TIME(dateFin), '14:59:59') >= 0) AND (TIMEDIFF('14:59:59', TIME(datedebut)) >= 0) THEN time_to_sec(TIMEDIFF( '14:59:59', TIME(datedebut))) WHEN (TIMEDIFF(TIME(dateFin), '14:00:00') >= 0) AND (TIMEDIFF('14:00:00', TIME(datedebut)) >= 0) THEN time_to_sec(TIMEDIFF( TIME(dateFin), '14:00:00')) WHEN (TIMEDIFF(TIME(dateFin), '14:00:00') >= 0) AND (TIMEDIFF('14:59:59', TIME(datedebut)) >= 0) THEN time_to_sec(TIMEDIFF( TIME(dateFin), TIME(datedebut))) ELSE time_to_sec(TIMEDIFF(TIME(dateFin), TIME(dateFin))) END AS 't14', CASE WHEN (TIMEDIFF(TIME(dateFin), '15:59:59') >= 0) AND (TIMEDIFF('15:00:00', TIME(datedebut)) >= 0) THEN time_to_sec(TIMEDIFF( '15:59:59', '15:00:00')) WHEN (TIMEDIFF(TIME(dateFin), '15:59:59') >= 0) AND (TIMEDIFF('15:59:59', TIME(datedebut)) >= 0) THEN time_to_sec(TIMEDIFF( '15:59:59', TIME(datedebut))) WHEN (TIMEDIFF(TIME(dateFin), '15:00:00') >= 0) AND (TIMEDIFF('15:00:00', TIME(datedebut)) >= 0) THEN time_to_sec(TIMEDIFF( TIME(dateFin), '15:00:00')) WHEN (TIMEDIFF(TIME(dateFin), '15:00:00') >= 0) AND (TIMEDIFF('15:59:59', TIME(datedebut)) >= 0) THEN time_to_sec(TIMEDIFF( TIME(dateFin), TIME(datedebut))) ELSE time_to_sec(TIMEDIFF(TIME(dateFin), TIME(dateFin))) END AS 't15', CASE WHEN (TIMEDIFF(TIME(dateFin), '16:59:59') >= 0) AND (TIMEDIFF('16:00:00', TIME(datedebut)) >= 0) THEN time_to_sec(TIMEDIFF( '16:59:59', '16:00:00')) WHEN (TIMEDIFF(TIME(dateFin), '16:59:59') >= 0) AND (TIMEDIFF('16:59:59', TIME(datedebut)) >= 0) THEN time_to_sec(TIMEDIFF( '16:59:59', TIME(datedebut))) WHEN (TIMEDIFF(TIME(dateFin), '16:00:00') >= 0) AND (TIMEDIFF('16:00:00', TIME(datedebut)) >= 0) THEN time_to_sec(TIMEDIFF( TIME(dateFin), '16:00:00')) WHEN (TIMEDIFF(TIME(dateFin), '16:00:00') >= 0) AND (TIMEDIFF('16:59:59', TIME(datedebut)) >= 0) THEN time_to_sec(TIMEDIFF( TIME(dateFin), TIME(datedebut))) ELSE time_to_sec(TIMEDIFF(TIME(dateFin), TIME(dateFin))) END AS 't16', CASE WHEN (TIMEDIFF(TIME(dateFin), '17:59:59') >= 0) AND (TIMEDIFF('17:00:00', TIME(datedebut)) >= 0) THEN time_to_sec(TIMEDIFF( '17:59:59', '17:00:00')) WHEN (TIMEDIFF(TIME(dateFin), '17:59:59') >= 0) AND (TIMEDIFF('17:59:59', TIME(datedebut)) >= 0) THEN time_to_sec(TIMEDIFF( '17:59:59', TIME(datedebut))) WHEN (TIMEDIFF(TIME(dateFin), '17:00:00') >= 0) AND (TIMEDIFF('17:00:00', TIME(datedebut)) >= 0) THEN time_to_sec(TIMEDIFF( TIME(dateFin), '17:00:00')) WHEN (TIMEDIFF(TIME(dateFin), '17:00:00') >= 0) AND (TIMEDIFF('17:59:59', TIME(datedebut)) >= 0) THEN time_to_sec(TIMEDIFF( TIME(dateFin), TIME(datedebut))) ELSE time_to_sec(TIMEDIFF(TIME(dateFin), TIME(dateFin))) END AS 't17', CASE WHEN (TIMEDIFF(TIME(dateFin), '18:59:59') >= 0) AND (TIMEDIFF('18:00:00', TIME(datedebut)) >= 0) THEN time_to_sec(TIMEDIFF( '18:59:59', '18:00:00')) WHEN (TIMEDIFF(TIME(dateFin), '18:59:59') >= 0) AND (TIMEDIFF('18:59:59', TIME(datedebut)) >= 0) THEN time_to_sec(TIMEDIFF( '18:59:59', TIME(datedebut))) WHEN (TIMEDIFF(TIME(dateFin), '18:00:00') >= 0) AND (TIMEDIFF('18:00:00', TIME(datedebut)) >= 0) THEN time_to_sec(TIMEDIFF( TIME(dateFin), '18:00:00')) WHEN (TIMEDIFF(TIME(dateFin), '18:00:00') >= 0) AND (TIMEDIFF('18:59:59', TIME(datedebut)) >= 0) THEN time_to_sec(TIMEDIFF( TIME(dateFin), TIME(datedebut))) ELSE time_to_sec(TIMEDIFF(TIME(dateFin), TIME(dateFin))) END AS 't18' from utilisation where time_to_sec(TIMEDIFF( TIME(dateFin), TIME(datedebut))) > 12*60; ",
		"drop table utilisationAllGlobal;",
		"create table utilisationAllGlobal as select sygfox, path, sum(timediff) as timediff, numerojoursemaine, mois, annee, numerosemaine, bat, etg, sum(t08) as t08, sum(t09) as t09, sum(t10) as t10, sum(t11) as t11, sum(t12) as t12, sum(t13) as t13, sum(t14) as t14, sum(t15) as t15, sum(t16) as t16, sum(t17) as t17, sum(t18) as t18 FROM utilisationAll GROUP BY sygfox, path, numerojoursemaine, mois, annee, numerosemaine, bat, etg;",
		"drop table utilAllGlobal;",
		"create table utilAllGlobal as SELECT refCapteurAnnee.*, timediff as temps, t08,t09,t10,t11,t12,t13,t14,t15,t16,t17,t18 FROM refCapteurAnnee left JOIN utilisationallglobal ON refCapteurAnnee.sygfox = utilisationallglobal.sygfox and refCapteurAnnee.annee = utilisationallglobal.annee and refCapteurAnnee.numerosemaine = utilisationallglobal.numerosemaine and refCapteurAnnee.numerojoursemaine = utilisationallglobal.numerojoursemaine ;",
	}

	for _, sql := range arraySQL {
		doSQL(db, sql)
	}
}

func step01(db *sql.DB) {

	files, err := ioutil.ReadDir(dir)
	if err != nil {
		log.Fatal(err)
	}

	for _, file := range files {
		if strings.HasSuffix(file.Name(), ".csv") && strings.HasPrefix(file.Name(), "sensors_messages_") {
			traitement(file, db)
		}
	}
}

func readDataSM(fileName string) ([][]string, error) {

	f, err := os.Open(fileName)

	if err != nil {
		return [][]string{}, err
	}

	r := csv.NewReader(f)
	r.Comma = ';'
	r.Comment = '#'

	// skip first line
	if _, err := r.Read(); err != nil {
		return [][]string{}, err
	}

	records, err := r.ReadAll()

	f.Close()

	if err != nil {
		return [][]string{}, err
	}

	return records, nil
}

func substr(input string, start int, length int) string {
	asRunes := []rune(input)

	if start >= len(asRunes) {
		return ""
	}

	if start+length > len(asRunes) {
		length = len(asRunes) - start
	}

	return string(asRunes[start : start+length])
}

func traitement(file fs.FileInfo, db *sql.DB) {
	logger := stdlog.GetFromFlags()
	logger.Info(file.Name())

	records, err := readDataSM(dir + "/" + file.Name())
	if err != nil {
		log.Fatal(err)
	}

	nbLigne := 0
	for _, record := range records {
		nbLigne++

		sensorMessage := sensorMessage{
			Code:                           record[0],
			PathLocalisation:               record[1],
			Statut:                         record[2],
			DateDerniereModificationStatut: record[3],
			Client:                         record[4],
			TypeCapteur:                    record[5],
			TypeMessage:                    record[6],
			Timezone:                       record[7],
			HeureMessage:                   record[8],
			NCHM:                           record[9],
			Contenu:                        record[10],
			DetailMessage:                  record[11],
			NDerniereMAJ:                   record[12],
			Downlink:                       record[13],
			MP1TypeMessage:                 record[14],
			MP1Horodatage:                  record[15],
			MP1Detail:                      record[16],
			MP2TypeMessage:                 record[17],
			MP2Horodatage:                  record[18],
			MP2Detail:                      record[19],
			MP3TypeMessage:                 record[20],
			MP3Horodatage:                  record[21],
			MP3Detail:                      record[22],
			MP4TypeMessage:                 record[23],
			MP4Horodatage:                  record[24],
			MP4Detail:                      record[25],
		}

		if nbLigne == 1 {
			sql := "DELETE FROM messageimport where date_message like '" + substr(sensorMessage.HeureMessage, 0, 10) + "%'"
			_, err := db.Exec(sql)

			if err != nil {
				logger.Info(sql)
				log.Fatal(err)
			}
		}

		sql := "INSERT INTO `messageimport` (`Sygfox`, `Path`, `Statut`, `Date_Statut`, `Client`, `Type_Capteur`, `Type_Message`, `Timezone`, `Date_Message`, `N_CHM`, `Contenu`, `Detail_Message`, `N_MAJ`, `Downlink`, `Type_Message1`, `Date_Message1`, `Detail_Message1`, `Type_Message2`, `Date_Message2`, `Detail_Message2`, `Type_Message3`, `Date_Message3`, `Detail_Message3`, `Type_Message4`, `Date_Message4`, `Detail_Message4`)"
		sql += " values  "
		sql += "('" + sensorMessage.Code
		sql += "','" + sensorMessage.PathLocalisation
		sql += "','" + sensorMessage.Statut
		sql += "','" + sensorMessage.DateDerniereModificationStatut
		sql += "','" + sensorMessage.Client
		sql += "','" + sensorMessage.TypeCapteur
		sql += "','" + sensorMessage.TypeMessage
		sql += "','" + sensorMessage.Timezone
		sql += "','" + sensorMessage.HeureMessage
		sql += "','" + sensorMessage.NCHM
		sql += "','" + sensorMessage.Contenu
		sql += "','" + sensorMessage.DetailMessage
		sql += "','" + sensorMessage.NDerniereMAJ
		sql += "','" + sensorMessage.Downlink
		sql += "','" + sensorMessage.MP1TypeMessage
		sql += "','" + sensorMessage.MP1Horodatage
		sql += "','" + sensorMessage.MP1Detail
		sql += "','" + sensorMessage.MP2TypeMessage
		sql += "','" + sensorMessage.MP2Horodatage
		sql += "','" + sensorMessage.MP2Detail
		sql += "','" + sensorMessage.MP3TypeMessage
		sql += "','" + sensorMessage.MP3Horodatage
		sql += "','" + sensorMessage.MP3Detail
		sql += "','" + sensorMessage.MP4TypeMessage
		sql += "','" + sensorMessage.MP4Horodatage
		sql += "','" + sensorMessage.MP4Detail
		sql += "')"

		_, err := db.Exec(sql)

		if err != nil {
			logger.Info(sql)
			log.Fatal(err)
		}

	}

	logger.Info(nbLigne)

	archiveData(file, err)
}

func archiveData(file fs.FileInfo, err error) {

	oldLocation := dir + "/" + file.Name()
	newLocation := dir + "/OK/" + file.Name()
	/*
		err = os.Rename(oldLocation, newLocation)
		if err != nil {
			log.Fatal(err)
		}
	*/

	archive, err := os.Create(newLocation + ".zip")
	if err != nil {
		panic(err)
	}

	defer archive.Close()

	zipWriter := zip.NewWriter(archive)

	fileData, err := os.Open(oldLocation)
	if err != nil {
		panic(err)
	}

	aZipWriter, err := zipWriter.Create(file.Name())
	if err != nil {
		panic(err)
	}
	if _, err := io.Copy(aZipWriter, fileData); err != nil {
		panic(err)
	}
	fileData.Close()

	zipWriter.Close()

	os.Remove(oldLocation)
}
