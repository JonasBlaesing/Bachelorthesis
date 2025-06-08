import csv
import requests
from bs4 import BeautifulSoup
import time
import re
from datetime import datetime

def parse_response(html_content):
    """
    Parst die HTML-Antwort und extrahiert Kategorie und Konfidenz
    """
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Kategorie extrahieren
    category_div = soup.find('div', class_='category-name')
    category = category_div.text.strip() if category_div else "Unbekannt"
    
    # Konfidenz extrahieren - suche nach "Konfidenz: XX%"
    confidence_text = soup.find(text=re.compile(r'Konfidenz:'))
    confidence = 0
    if confidence_text:
        match = re.search(r'(\d+)%', confidence_text.parent.text)
        if match:
            confidence = int(match.group(1))
    
    return category, confidence

def process_csv(input_file, output_file, base_url="http://localhost:5000"):
    """
    Liest CSV-Datei, sendet Anfragen an Flask-App und speichert Ergebnisse
    """
    results = []
    
    # CSV-Datei lesen
    with open(input_file, 'r', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        total_rows = sum(1 for _ in reader)
        csvfile.seek(0)
        reader = csv.DictReader(csvfile)
        
        print(f"Verarbeite {total_rows} Anfragen...")
        
        for idx, row in enumerate(reader, 1):
            # Fortschritt anzeigen
            print(f"\nVerarbeite Anfrage {idx}/{total_rows}: {row['vorname']} {row['nachname']}")
            
            # Daten für POST-Request vorbereiten
            form_data = {
                'first_name': row['vorname'],
                'last_name': row['nachname'],
                'e_mail': row['e_mail'],
                'subject': row['betreff'],
                'message': row['nachricht']
            }
            
            try:
                # Request senden
                response = requests.post(f"{base_url}/upload", data=form_data)
                
                if response.status_code == 200:
                    # Kategorie und Konfidenz aus HTML extrahieren
                    assigned_category, confidence = parse_response(response.text)
                    
                    # Ergebnis speichern
                    result = {
                        'id': row['id'],
                        'vorname': row['vorname'],
                        'nachname': row['nachname'],
                        'e_mail': row['e_mail'],
                        'betreff': row['betreff'],
                        'nachricht': row['nachricht'],
                        'erwartete_kategorie': row['kategorie'],
                        'zugeordnete_kategorie': assigned_category,
                        'konfidenz': confidence,
                        'korrekt': row['kategorie'] == assigned_category
                    }
                    results.append(result)
                    
                    print(f"  → Zugeordnet: {assigned_category} (Konfidenz: {confidence}%)")
                    print(f"  → Erwartet: {row['kategorie']}")
                    print(f"  → {'✓ Korrekt' if result['korrekt'] else '✗ Falsch'}")
                    
                else:
                    print(f"  → Fehler: HTTP {response.status_code}")
                    
            except Exception as e:
                print(f"  → Fehler bei Verarbeitung: {str(e)}")
            
            # Kleine Pause zwischen Requests
            time.sleep(0.5)
    
    # Ergebnisse in neue CSV-Datei schreiben
    if results:
        fieldnames = ['id', 'vorname', 'nachname', 'e_mail', 'betreff', 'nachricht', 
                     'erwartete_kategorie', 'zugeordnete_kategorie', 'konfidenz', 'korrekt']
        
        with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(results)
        
        # Statistiken berechnen
        total = len(results)
        correct = sum(1 for r in results if r['korrekt'])
        accuracy = (correct / total) * 100
        
        # Statistiken nach Kategorie
        category_stats = {}
        for result in results:
            cat = result['erwartete_kategorie']
            if cat not in category_stats:
                category_stats[cat] = {'total': 0, 'correct': 0}
            category_stats[cat]['total'] += 1
            if result['korrekt']:
                category_stats[cat]['correct'] += 1
        
        # Durchschnittliche Konfidenz
        avg_confidence = sum(r['konfidenz'] for r in results) / total
        avg_confidence_correct = sum(r['konfidenz'] for r in results if r['korrekt']) / correct if correct > 0 else 0
        avg_confidence_wrong = sum(r['konfidenz'] for r in results if not r['korrekt']) / (total - correct) if (total - correct) > 0 else 0
        
        print(f"\n{'='*60}")
        print(f"ERGEBNISSE ZUSAMMENFASSUNG")
        print(f"{'='*60}")
        print(f"Gesamtanzahl Anfragen: {total}")
        print(f"Korrekt klassifiziert: {correct} ({accuracy:.1f}%)")
        print(f"Durchschnittliche Konfidenz: {avg_confidence:.1f}%")
        print(f"  - Bei korrekten: {avg_confidence_correct:.1f}%")
        print(f"  - Bei falschen: {avg_confidence_wrong:.1f}%")
        print(f"\nGenauigkeit nach Kategorie:")
        for cat, stats in category_stats.items():
            cat_accuracy = (stats['correct'] / stats['total']) * 100 if stats['total'] > 0 else 0
            print(f"  - {cat}: {stats['correct']}/{stats['total']} ({cat_accuracy:.1f}%)")
        
        print(f"\nErgebnisse gespeichert in: {output_file}")
        
        # Optional: Fehlerhafte Klassifizierungen anzeigen
        print(f"\n{'='*60}")
        print("FALSCHE KLASSIFIZIERUNGEN:")
        print(f"{'='*60}")
        wrong_classifications = [r for r in results if not r['korrekt']]
        if wrong_classifications:
            for r in wrong_classifications[:5]:  # Zeige max. 5 Beispiele
                print(f"\nID {r['id']}: {r['vorname']} {r['nachname']}")
                print(f"Betreff: {r['betreff'][:50]}...")
                print(f"Erwartet: {r['erwartete_kategorie']}")
                print(f"Zugeordnet: {r['zugeordnete_kategorie']} (Konfidenz: {r['konfidenz']}%)")
        else:
            print("Keine falschen Klassifizierungen!")

def main():
    # Konfiguration
    INPUT_FILE = "synthetische_buergeranfragen.csv"
    OUTPUT_FILE = f"test_ergebnisse_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    FLASK_URL = "http://localhost:5000"  # Anpassen falls Flask auf anderem Port läuft
    
    print("Bürgeranfragen Test-Script")
    print("=" * 60)
    print(f"Input-Datei: {INPUT_FILE}")
    print(f"Output-Datei: {OUTPUT_FILE}")
    print(f"Flask-Server: {FLASK_URL}")
    print("=" * 60)
    
    # Prüfen ob Flask-Server erreichbar ist
    try:
        response = requests.get(FLASK_URL)
        if response.status_code == 200:
            print("✓ Flask-Server ist erreichbar\n")
        else:
            print(f"⚠ Flask-Server antwortet mit Status {response.status_code}")
            print("Stelle sicher, dass die Flask-App läuft!")
            return
    except Exception as e:
        print(f"✗ Flask-Server nicht erreichbar: {str(e)}")
        print("Stelle sicher, dass die Flask-App mit 'python app.py' gestartet wurde!")
        return
    
    # CSV verarbeiten
    try:
        process_csv(INPUT_FILE, OUTPUT_FILE, FLASK_URL)
    except FileNotFoundError:
        print(f"✗ Datei '{INPUT_FILE}' nicht gefunden!")
        print("Stelle sicher, dass die CSV-Datei im gleichen Verzeichnis liegt.")
    except Exception as e:
        print(f"✗ Fehler beim Verarbeiten: {str(e)}")

if __name__ == "__main__":
    main()