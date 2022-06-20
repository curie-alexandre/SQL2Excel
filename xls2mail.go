package main

import (
	"fmt"
	"log"
	"time"

	"gopkg.in/gomail.v2"
)

func mail() {
	log.Println("Debut ", "mail")
	m := gomail.NewMessage()
	m.SetHeader("From", "alexandre.curie.fr@free.fr")
	m.SetHeader("To", "gaetane.benoitarnulf@axa.fr", "nathalie.parrot@axa.fr", "florence.carlin@axa.fr")
	m.SetHeader("Cc", "jeanbaptiste.jacquelin.neurones@axa.fr", "alexandre.curie.opteamis@axa.fr")
	m.SetHeader("Bcc", "alexandre.curie.fr@free.fr")
	m.SetHeader("Subject", fmt.Sprintf("[Stats Capteurs] > %v", time.Now().Format("2006-01-02")))
	m.SetBody("text/html", fmt.Sprintf("[Stats Capteurs] <b>%v</b>!", time.Now().Format("2006-01-02")))
	m.Attach(fmt.Sprintf("c:\\Media\\dev\\messageWX\\dump\\%v - %s.xlsx", time.Now().Format("2006-01-02"), "Durée moyenne d'occupation des postes occupés en minute"))
	m.Attach(fmt.Sprintf("c:\\Media\\dev\\messageWX\\dump\\%v - %s.xlsx", time.Now().Format("2006-01-02"), "Durée moyenne d'occupation des postes occupés versus l'intégralité des postes en minute"))
	m.Attach(fmt.Sprintf("c:\\Media\\dev\\messageWX\\dump\\%v - %s.xlsx", time.Now().Format("2006-01-02"), "Durée moyenne d'utilisation d'un poste sur postes occupés"))
	m.Attach(fmt.Sprintf("c:\\Media\\dev\\messageWX\\dump\\%v - %s.xlsx", time.Now().Format("2006-01-02"), "Nombre de postes utilisés moins de 25 50 75 pourcent du temps sur une demi-journée"))
	m.Attach(fmt.Sprintf("c:\\Media\\dev\\messageWX\\dump\\%v - %s.xlsx", time.Now().Format("2006-01-02"), "Taux moyen d'occupation rapporté à une journée de 9h (Durée Moyenne)"))
	m.Attach(fmt.Sprintf("c:\\Media\\dev\\messageWX\\dump\\%v - %s.xlsx", time.Now().Format("2006-01-02"), "Taux moyen d'occupation rapporté à une journée de 9h (Somme durée)"))
	m.Attach(fmt.Sprintf("c:\\Media\\dev\\messageWX\\dump\\%v - %s.xlsx", time.Now().Format("2006-01-02"), "Liste des saturation (75% de postes de travail occupés en simultané)"))
	m.Attach(fmt.Sprintf("c:\\Media\\dev\\messageWX\\dump\\%v - %s.xlsx", time.Now().Format("2006-01-02"), "Nombre de postes en fonctionnement"))

	d := gomail.NewDialer("smtp.free.fr", 587, "alexandre.curie.fr@free.fr", "Police04")

	// Send the email to Bob, Cora and Dan.
	if err := d.DialAndSend(m); err != nil {
		panic(err)
	}
	log.Println("Fin   ", "mail")
}

func mailold() {
	log.Println("Debut ", "mail")
	m := gomail.NewMessage()
	m.SetHeader("From", "alexandre.curie.fr@free.fr")
	m.SetHeader("To", "gaetane.benoitarnulf@axa.fr", "nathalie.parrot@axa.fr", "florence.carlin@axa.fr")
	m.SetHeader("Cc", "jeanbaptiste.jacquelin.neurones@axa.fr", "alexandre.curie.opteamis@axa.fr")
	m.SetHeader("Bcc", "alexandre.curie.fr@free.fr")
	m.SetHeader("Subject", fmt.Sprintf("[Stats Capteurs] > %v", time.Now().Format("2006-01-02")))
	m.SetBody("text/html", fmt.Sprintf("[Stats Capteurs] <b>%v</b>!", time.Now().Format("2006-01-02")))
	m.Attach(fmt.Sprintf("c:\\Media\\dev\\messageWX\\dump\\%v - %s.xlsx", time.Now().Format("2006-01-02"), "Durée moyenne d'occupation des postes occupés en seconde"))
	m.Attach(fmt.Sprintf("c:\\Media\\dev\\messageWX\\dump\\%v - %s.xlsx", time.Now().Format("2006-01-02"), "Durée moyenne d'occupation des postes occupés versus l'intégralité des postes"))
	m.Attach(fmt.Sprintf("c:\\Media\\dev\\messageWX\\dump\\%v - %s.xlsx", time.Now().Format("2006-01-02"), "Durée moyenne d'utilisation d'un poste sur postes occupés"))
	m.Attach(fmt.Sprintf("c:\\Media\\dev\\messageWX\\dump\\%v - %s.xlsx", time.Now().Format("2006-01-02"), "Nombre de postes utilisés moins de 25 50 75 pourcent du temps sur une demi-journée"))
	m.Attach(fmt.Sprintf("c:\\Media\\dev\\messageWX\\dump\\%v - %s.xlsx", time.Now().Format("2006-01-02"), "Taux moyen d'occupation rapporté à une journée de 9h (Durée Moyenne)"))
	m.Attach(fmt.Sprintf("c:\\Media\\dev\\messageWX\\dump\\%v - %s.xlsx", time.Now().Format("2006-01-02"), "Taux moyen d'occupation rapporté à une journée de 9h (Somme durée)"))
	m.Attach(fmt.Sprintf("c:\\Media\\dev\\messageWX\\dump\\%v - %s.xlsx", time.Now().Format("2006-01-02"), "Liste des saturation (75% de postes de travail occupés en simultané)"))
	m.Attach(fmt.Sprintf("c:\\Media\\dev\\messageWX\\dump\\%v - %s.xlsx", time.Now().Format("2006-01-02"), "Nombre de postes en fonctionnement"))

	d := gomail.NewDialer("smtp.free.fr", 587, "alexandre.curie.fr@free.fr", "Police04")

	// Send the email to Bob, Cora and Dan.
	if err := d.DialAndSend(m); err != nil {
		panic(err)
	}
	log.Println("Fin   ", "mail")
}
