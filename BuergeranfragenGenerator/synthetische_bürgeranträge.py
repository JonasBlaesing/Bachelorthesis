#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
import json
import random
import requests
import argparse
import datetime
import re
from typing import List, Dict, Any, Callable, Union, Pattern
import os

class SyntheticQueryGenerator:
    """
    Generiert synthetische Bürgeranfragen mit Ollama und speichert sie in einer CSV-Datei sowie einzelne JSON-Dateien.
    """
    
    def __init__(self, 
                 model_name: str = "llama3", 
                 ollama_url: str = "http://localhost:11434",
                 num_queries_per_category: int = 1000,
                 output_file: str = "synthetische_buergeranfragen.csv",
                 json_dir: str = "json_anfragen"):
        """
        Initialisiert den Generator.
        
        Args:
            model_name: Name des Ollama-Modells
            ollama_url: URL des Ollama-Servers
            num_queries_per_category: Anzahl der zu generierenden Anfragen pro Kategorie
            output_file: Name der Ausgabedatei
            json_dir: Verzeichnis, in dem die JSON-Dateien gespeichert werden
        """
        self.model_name = model_name
        self.ollama_url = ollama_url
        self.num_queries_per_category = num_queries_per_category
        self.output_file = output_file
        self.json_dir = json_dir
        
        # Stelle sicher, dass das JSON-Verzeichnis existiert
        os.makedirs(self.json_dir, exist_ok=True)
        
        # Vornamen und Nachnamen für die Generierung
        self.first_names = [
            "Max", "Julia", "Alexander", "Sophie", "Michael", "Emma", "Thomas", "Anna", "Markus", "Laura",
            "Andreas", "Sarah", "Stefan", "Lisa", "Christian", "Lena", "Daniel", "Hannah", "Tobias", "Leonie",
            "Jan", "Marie", "Sebastian", "Katharina", "Felix", "Johanna", "Simon", "Nina", "Florian", "Maria",
            "Lukas", "Sophia", "Leon", "Melanie", "Tim", "Sandra", "Paul", "Nicole", "Jonas", "Claudia",
            "David", "Sabine", "Philipp", "Monika", "Benjamin", "Anja", "Matthias", "Christine", "Niklas", "Petra"
        ]
        
        self.last_names = [
            "Müller", "Schmidt", "Schneider", "Fischer", "Weber", "Meyer", "Wagner", "Becker", "Schulz", "Hoffmann",
            "Koch", "Richter", "Bauer", "Klein", "Wolf", "Schröder", "Neumann", "Schwarz", "Zimmermann", "Braun",
            "Krüger", "Hofmann", "Hartmann", "Lange", "Schmitt", "Werner", "Schmitz", "Krause", "Meier", "Lehmann",
            "Schmid", "Schulze", "Maier", "Köhler", "Herrmann", "König", "Walter", "Huber", "Kaiser", "Peters",
            "Fuchs", "Lang", "Scholz", "Möller", "Weiß", "Jung", "Hahn", "Schubert", "Vogel", "Friedrich"
        ]
        
        # Kategorien und ihre spezifischen Eigenschaften
        self.categories = {
            "KFZ-Zulassung": {
                "topic_keywords": ["Auto", "Fahrzeug", "KFZ", "PKW", "Zulassung", "Anmeldung", "Ummeldung", "Kennzeichen", 
                                   "Wunschkennzeichen", "Abmeldung", "TÜV", "HU", "Hauptuntersuchung"],
                "common_questions": [
                    "Wie kann ich mein Auto anmelden?",
                    "Welche Unterlagen brauche ich für die KFZ-Zulassung?",
                    "Wie viel kostet die Ummeldung eines Autos?",
                    "Kann ich online einen Termin für die KFZ-Zulassung vereinbaren?",
                    "Wie bekomme ich ein Wunschkennzeichen?",
                    "Muss ich für die Abmeldung meines Autos persönlich erscheinen?",
                    "Kann ich mein Auto auch in einem anderen Landkreis zulassen?",
                    "Wie lange dauert die Zulassung eines KFZ?",
                    "Was muss ich bei einem Autokauf beachten bezüglich Ummeldung?"
                ],
                "subjects": [
                    "KFZ-Zulassung", "Fahrzeuganmeldung", "Wunschkennzeichen", "Fahrzeugabmeldung",
                    "Ummeldung Auto", "Autoabmeldung", "Kfz-Papiere", "Fahrzeugpapiere", "Zulassungsbescheinigung",
                    "Autoanmeldung", "Kennzeichen Reservierung", "KFZ-Ummeldung", "Fahrzeugschein"
                ]
            },
            "Gewerbeanmeldung": {
                "topic_keywords": ["Gewerbe", "Anmeldung", "Gewerbeschein", "Kleingewerbe", "Freiberufler", "Handelsregister",
                                  "Einzelunternehmen", "GmbH", "Firma", "selbständig", "Gewerbesteuer", "Gewerbeamt"],
                "common_questions": [
                    "Wie melde ich ein Gewerbe an?",
                    "Welche Unterlagen benötige ich für die Gewerbeanmeldung?",
                    "Was kostet eine Gewerbeanmeldung?",
                    "Unterschied zwischen Freiberufler und Gewerbetreibender?",
                    "Brauche ich für ein Kleingewerbe einen Gewerbeschein?",
                    "Muss ich mein Gewerbe im Handelsregister eintragen lassen?",
                    "Kann ich ein Gewerbe auch online anmelden?",
                    "Welche Gewerbearten gibt es?",
                    "Was muss ich bei einer GmbH-Gründung beachten?"
                ],
                "subjects": [
                    "Gewerbeanmeldung", "Gewerbeschein", "Kleingewerbe", "Gewerbeummeldung",
                    "Gewerbeanmeldung online", "Nebengewerbe", "Gewerbeabmeldung",
                    "Gewerbeanmeldung Kosten", "Handelsregistereintrag", "GmbH-Gründung",
                    "Gewerbesteuer", "Gewerbeunterlagen", "Einzelunternehmen"
                ]
            },
            "Hundesteuer": {
                "topic_keywords": ["Hund", "Hundesteuer", "Anmeldung", "Steuer", "Hundemarke", "Ermäßigung", "Befreiung",
                                  "Kampfhund", "Listenhund", "Welpe", "Abmeldung", "Hundehalter"],
                "common_questions": [
                    "Wie melde ich meinen Hund an?",
                    "Wie hoch ist die Hundesteuer?",
                    "Ab wann muss ich Hundesteuer bezahlen?",
                    "Gibt es eine Ermäßigung der Hundesteuer?",
                    "Was passiert, wenn ich meinen Hund nicht anmelde?",
                    "Wie kann ich die Hundesteuer-Befreiung beantragen?",
                    "Wie melde ich meinen Hund ab, wenn er verstorben ist?",
                    "Muss ich bei einem Umzug meinen Hund neu anmelden?",
                    "Gibt es für Servicehunde oder Blindenführhunde Ausnahmen?"
                ],
                "subjects": [
                    "Hundesteuerpflicht", "Hundeanmeldung", "Hundesteuerermäßigung", "Hundesteuerbefreiung",
                    "Hundeabmeldung", "Hundemarke", "Hundesteuer Höhe", "Zweithund", "Listenhund",
                    "Hundesteuer Fälligkeit", "Welpenmeldung", "Hundesteuer Umzug", "Hundesteuer Antrag"
                ]
            }
        }
        
        # Verschiedene Fehlertypen für realistischere Anfragen
        self.error_types = {
            "typos": {
                "probability": 0.4,
                "examples": {
                    "ein": "einn", "ich": "ihc", "und": "udn", "für": "fuer", "zur": "zru",
                    "mein": "meine", "die": "dei", "das": "dsa", "wie": "wei", "ist": "sit",
                    "kann": "kan", "wann": "wan", "Termin": "Termni", "brauche": "braucha",
                    "Unterlagen": "Unteralgen", "Anmeldung": "Anmelung"
                }
            },
            "grammar": {
                "probability": 0.3,
                "examples": [
                    "Ich will wissen wann kann ich kommen",
                    "Wo muss hingehen für Anmeldung",
                    "Was kostet für ein Hund anmelden",
                    "Brauche Hilfe mit die Unterlagen",
                    "Wann ist geöffnet das Amt"
                ]
            },
            "dialect": {
                "probability": 0.2,
                "examples": {
                    "standard": ["Was", "Wie", "Wann", "Ich möchte", "Guten Tag", "Könnten Sie", "bitte"],
                    "bavarian": ["Wos", "Wia", "Wonn", "I mecht", "Servus", "Kenntns", "bittschön"],
                    "swabian": ["Was", "Wie", "Wenn", "I will", "Grüß Gott", "Könntet Se", "bidde"],
                    "berlin": ["Wat", "Wie", "Wann", "Ick will", "Tach", "Könnse", "ma"]
                }
            }
        }
        
        # Typische Einleitungen und Abschlüsse für Anfragen
        self.query_intros = [
            "Hallo,", "Guten Tag,", "Sehr geehrte Damen und Herren,", "Grüß Gott,",
            "Servus,", "Moin,", "Tach,", "Hallöchen,", "Gude,", "Tag auch,"
        ]
        
        self.query_outros = [
            "Mit freundlichen Grüßen", "Viele Grüße", "Danke im Voraus", "Danke für Ihre Hilfe",
            "Bitte um schnelle Antwort", "Dankeschön", "Vielen Dank", "MfG", "LG", "Schönen Tag noch",
            "Ich freue mich auf Ihre Antwort"
        ]
    
    def _generate_email(self, first_name: str, last_name: str) -> str:
        """
        Generiert eine E-Mail-Adresse basierend auf Vor- und Nachnamen.
        
        Args:
            first_name: Vorname
            last_name: Nachname
            
        Returns:
            Eine generierte E-Mail-Adresse
        """
        domains = ["example.com", "mail.de", "gmx.de", "web.de", "t-online.de", "gmail.com", "outlook.com"]
        email_formats = [
            f"{first_name.lower()}.{last_name.lower()}@{random.choice(domains)}",
            f"{last_name.lower()}.{first_name.lower()}@{random.choice(domains)}",
            f"{first_name.lower()}{last_name.lower()}@{random.choice(domains)}",
            f"{first_name.lower()[0]}.{last_name.lower()}@{random.choice(domains)}",
            f"{first_name.lower()}{random.randint(1, 99)}@{random.choice(domains)}"
        ]
        return random.choice(email_formats)
    
    def _generate_prompt(self, category: str, first_name: str, last_name: str) -> str:
        """
        Erstellt einen Prompt für Ollama zur Generierung einer realistischen Bürgeranfrage.
        
        Args:
            category: Die Kategorie der Anfrage (KFZ-Zulassung, Gewerbeanmeldung, Hundesteuer)
            first_name: Vorname der Person
            last_name: Nachname der Person
        
        Returns:
            Der generierte Prompt als String
        """
        cat_info = self.categories[category]
        keywords = random.sample(cat_info["topic_keywords"], k=random.randint(1, 3))
        example_question = random.choice(cat_info["common_questions"])
        
        # Bestimme, ob Fehler eingefügt werden sollen
        include_typos = random.random() < self.error_types["typos"]["probability"]
        include_grammar_errors = random.random() < self.error_types["grammar"]["probability"]
        include_dialect = random.random() < self.error_types["dialect"]["probability"]
        
        error_instructions = ""
        if include_typos:
            error_instructions += "Füge einige realistische Tippfehler ein. "
        if include_grammar_errors:
            error_instructions += "Verwende gelegentlich fehlerhafte Grammatik. "
        if include_dialect:
            dialect = random.choice(["bavarian", "swabian", "berlin"])
            error_instructions += f"Verwende leichte {dialect} Dialekt-Elemente. "
        
        prompt = f"""
        Erstelle eine realistische Bürgeranfrage zum Thema {category}. 
        Die Anfrage sollte eine einfache E-Mail oder ein Formular-Text sein, der an eine Behörde gerichtet ist.
        Die Person heißt {first_name} {last_name} und schreibt in der ersten Person ("ich", "mein", "mir").
        
        Verwende einige dieser Schlüsselwörter: {', '.join(keywords)}
        Orientiere dich an Fragen wie: "{example_question}"
        
        Die Anfrage sollte:
        - Wie eine echte Bürgeranfrage klingen
        - Zwischen 30 und 150 Wörtern lang sein
        - Eine konkrete Frage oder ein Anliegen enthalten
        - Optional mit einer Begrüßung beginnen und einer Verabschiedung enden
        - In der ersten Person geschrieben sein ("ich möchte", "mein Problem ist", etc.)
        
        {error_instructions}
        
        WICHTIG: 
        - Gib NUR den Text der Anfrage zurück, ohne Einleitung. Beginne direkt mit der Anfrage, 
          ohne Phrasen wie "Hier ist die Bürgeranfrage:" oder "Here is a citizen inquiry:".
        - Verwende KEINE Platzhalter wie [Name], [Adresse], [Telefon] usw. 
        - Falls du Namen brauchst, verwende {first_name} {last_name}.
        - Schreibe immer in der ersten Person ("ich", "mein", "mir", "mich").
        """
        
        return prompt
    
    def _call_ollama(self, prompt: str) -> str:
        """
        Sendet einen Prompt an Ollama und gibt die Antwort zurück.
        
        Args:
            prompt: Der Prompt für das Modell
        
        Returns:
            Die bereinigte generierte Antwort als String
        """
        try:
            response = requests.post(
                f"{self.ollama_url}/api/generate",
                json={
                    "model": self.model_name,
                    "prompt": prompt,
                    "stream": False
                },
                timeout=60
            )
            
            if response.status_code == 200:
                text = response.json().get("response", "").strip()
                
                # Bereinige den Text von unerwünschten Einleitungen
                text = self._clean_generated_text(text)
                return text
            else:
                print(f"Fehler beim Aufruf von Ollama: {response.status_code}")
                print(f"Response: {response.text}")
                return f"[Fehler bei der Generierung: HTTP {response.status_code}]"
                
        except requests.exceptions.RequestException as e:
            print(f"Fehler bei der Verbindung zu Ollama: {str(e)}")
            return f"[Verbindungsfehler: {str(e)}]"
    
    def _clean_generated_text(self, text: str) -> str:
        """
        Bereinigt den generierten Text von unerwünschten Einleitungen und Formatierungen.
        
        Args:
            text: Der zu bereinigende Text
            
        Returns:
            Der bereinigte Text
        """
        # Liste von Einleitungsphrasen, die entfernt werden sollen
        intro_phrases = [
            "Hier ist die realistische Bürgeranfrage:",
            "Hier ist eine realistische Bürgeranfrage:",
            "Hier ist eine mögliche Bürgeranfrage:",
            "Hier ist die Bürgeranfrage:",
            "Here is a realistic citizen inquiry about",
            "Here is a realistic citizen's request",
            "Subject:",
            "Betreff:",
            "Here is a realistic citizen's inquiry",
            "Die Bürgeranfrage lautet:",
            "Realistische Bürgeranfrage:",
            "Eine realistische Bürgeranfrage zum Thema",
        ]
        
        # Überprüfe, ob der Text mit einer der Einleitungsphrasen beginnt
        for phrase in intro_phrases:
            if text.startswith(phrase):
                # Entferne die Einleitung und bereinige den Anfang
                text = text[len(phrase):].strip()
                # Manchmal folgt ein Doppelpunkt oder Anführungszeichen nach der Einleitung
                text = text.lstrip(':"\'')
                break
        
        # Überprüfe auf englische Sätze im Text und entferne sie
        english_sentences = [
            "This is a realistic query about",
            "Here is the request:",
            "Here's a realistic citizen inquiry",
        ]
        
        for sentence in english_sentences:
            if sentence.lower() in text.lower():
                # Teile den Text beim englischen Satz
                parts = text.split(sentence, 1)
                if len(parts) > 1:
                    # Wenn der Satz am Anfang steht, nimm nur den Teil danach
                    if not parts[0].strip():
                        text = parts[1].strip().lstrip(':"\'')
                    else:
                        # Sonst behalte den Teil vor dem englischen Satz
                        text = parts[0].strip()
        
        return text.strip()
    
    def _generate_subject(self, category: str, message: str) -> str:
        """
        Generiert einen passenden Betreff für die Anfrage.
        
        Args:
            category: Kategorie der Anfrage
            message: Text der Anfrage
            
        Returns:
            Ein generierter Betreff für die Nachricht
        """
        # Manchmal einfach einen Standard-Betreff aus der Kategorie auswählen
        if random.random() < 0.4:
            return random.choice(self.categories[category]["subjects"])
        
        # Sonst einen benutzerdefinierten Betreff basierend auf der Nachricht erstellen
        # Message in Wörter aufteilen und ein paar davon auswählen
        words = message.split()
        
        # Kurze Keywords aus der Nachricht extrahieren
        keywords = []
        for word in words:
            if len(word) > 3 and not word.lower().startswith(("hallo", "guten", "sehr", "liebe", "mfg", "mit", "danke")):
                keywords.append(word.strip(",.?!:;"))
                if len(keywords) >= 4:
                    break
        
        if not keywords:
            return random.choice(self.categories[category]["subjects"])
        
        # Zufällig 1-3 Keywords auswählen für den Betreff
        chosen_keywords = random.sample(keywords, k=min(len(keywords), random.randint(1, 3)))
        
        # Betreff zusammenbauen
        subject = " ".join(chosen_keywords)
        
        # Manchmal "Frage zu", "Anfrage:", etc. am Anfang hinzufügen
        prefixes = ["", "", "", "Frage zu ", "Anfrage: ", "Info zu ", "Betreff: ", ""]
        subject = random.choice(prefixes) + subject
        
        # Betreff auf 50 Zeichen begrenzen und sicherstellen, dass er nicht leer ist
        subject = subject[:50].strip()
        if not subject:
            subject = random.choice(self.categories[category]["subjects"])
        
        return subject
    
    def generate_queries(self) -> List[Dict[str, Any]]:
        """
        Generiert die angegebene Anzahl von Anfragen für jede Kategorie.
        
        Returns:
            Eine Liste von Dictionaries mit den generierten Anfragen
        """
        all_queries = []
        next_id = 1
        
        for category in self.categories:
            print(f"Generiere {self.num_queries_per_category} Anfragen für Kategorie '{category}'...")
            
            for i in range(self.num_queries_per_category):
                # Name und E-Mail generieren
                first_name = random.choice(self.first_names)
                last_name = random.choice(self.last_names)
                email = self._generate_email(first_name, last_name)
                
                # Anfrage generieren mit personalisierten Namen
                prompt = self._generate_prompt(category, first_name, last_name)
                nachricht = self._call_ollama(prompt)
                
                if not nachricht.startswith("[Fehler"):
                    # Betreff generieren
                    betreff = self._generate_subject(category, nachricht)
                    
                    # Unique ID erstellen
                    uid = next_id
                    next_id += 1
                    
                    # Datensatz erstellen
                    query = {
                        "id": uid,
                        "vorname": first_name,
                        "nachname": last_name,
                        "e_mail": email,
                        "betreff": betreff,
                        "nachricht": nachricht,
                        "kategorie": category
                    }
                    
                    all_queries.append(query)
                    print(f"  Anfrage {i+1}/{self.num_queries_per_category} erstellt")
                    
                    # Speichere die JSON-Datei
                    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S_%f")[:17]
                    json_filename = f"{last_name}_{first_name}_{timestamp}.json"
                    json_path = os.path.join(self.json_dir, json_filename)
                    with open(json_path, 'w', encoding='utf-8') as f:
                        json.dump(query, f, ensure_ascii=False, indent=4)
                        
                else:
                    print(f"  Fehler bei Anfrage {i+1}/{self.num_queries_per_category}: {nachricht}")
        
        return all_queries
    
    def save_to_csv(self, queries: List[Dict[str, Any]]) -> None:
        """
        Speichert die generierten Anfragen in einer CSV-Datei.
        Mischt die Daten vor dem Speichern.
        
        Args:
            queries: Liste der generierten Anfragen
        """
        if not queries:
            print("Keine Anfragen zum Speichern generiert.")
            return
        
        # Mische die Daten vor dem Speichern
        random.shuffle(queries)
        print(f"Daten wurden durchmischt vor dem Speichern.")
        
        with open(self.output_file, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['id', 'vorname', 'nachname', 'e_mail', 'betreff', 'nachricht', 'kategorie']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            writer.writeheader()
            for query in queries:
                writer.writerow(query)
        
        print(f"Erfolgreich {len(queries)} Anfragen in '{self.output_file}' gespeichert.")
    
    def run(self) -> None:
        """
        Führt den gesamten Generierungsprozess durch.
        """
        print(f"Starte Generierung mit Ollama-Modell '{self.model_name}'...")
        queries = self.generate_queries()
        self.save_to_csv(queries)
        print(f"Fertig! {len(queries)} Anfragen wurden generiert.")
        print(f"- CSV-Datei: {self.output_file}")
        print(f"- JSON-Dateien: {self.json_dir}/")


def load_config_from_json(json_file: str) -> Dict[str, Any]:
    """
    Lädt Konfigurationsdaten aus einer JSON-Datei.
    
    Args:
        json_file: Pfad zur JSON-Datei
        
    Returns:
        Dictionary mit den Konfigurationsdaten
    """
    try:
        with open(json_file, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"Warnung: Konfigurationsdatei '{json_file}' nicht gefunden. Verwende Standardeinstellungen.")
        return {}
    except json.JSONDecodeError:
        print(f"Warnung: Fehler beim Parsen der Konfigurationsdatei '{json_file}'. Verwende Standardeinstellungen.")
        return {}


def main():
    """
    Hauptfunktion zum Ausführen des Scripts
    """
    parser = argparse.ArgumentParser(description='Generiert synthetische Bürgeranfragen mit Ollama')
    parser.add_argument('--model', type=str, default="llama3", help='Name des Ollama-Modells')
    parser.add_argument('--url', type=str, default="http://localhost:11434", help='URL des Ollama-Servers')
    parser.add_argument('--config', type=str, help='Pfad zur JSON-Konfigurationsdatei')
    parser.add_argument('--num', type=int, default=30, help='Anzahl der Anfragen pro Kategorie')
    parser.add_argument('--output', type=str, default="synthetische_buergeranfragen.csv", help='Name der Ausgabedatei')
    parser.add_argument('--json-dir', type=str, default="json_anfragen", help='Verzeichnis für JSON-Dateien')
    
    args = parser.parse_args()
    
    # Lade Konfiguration aus JSON-Datei, falls angegeben
    config = {}
    if args.config:
        config = load_config_from_json(args.config)
    
    # Verwende Werte aus der Konfiguration oder aus den Argumenten
    model_name = config.get('model_name', args.model)
    ollama_url = config.get('ollama_url', args.url)
    num_queries = config.get('num_queries_per_category', args.num)
    output_file = config.get('output_file', args.output)
    json_dir = config.get('json_dir', args.json_dir)
    
    # Erstelle und starte den Generator
    generator = SyntheticQueryGenerator(
        model_name=model_name,
        ollama_url=ollama_url,
        num_queries_per_category=num_queries,
        output_file=output_file,
        json_dir=json_dir
    )
    
    generator.run()


if __name__ == "__main__":
    main()