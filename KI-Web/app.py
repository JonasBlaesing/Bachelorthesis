from flask import Flask, render_template, request
import os
from datetime import datetime
import json
import requests

app = Flask(__name__)
UPLOAD_FOLDER = 'uploads'
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

# Ollama-Konfiguration
OLLAMA_URL = "http://localhost:11434"
OLLAMA_MODEL = "llama3"  # oder ein anderes verfügbares Modell

# Kategorien-Definitionen
CATEGORIES = {
    "KFZ-Zulassung": {
        "keywords": ["auto", "fahrzeug", "kfz", "pkw", "zulassung", "anmeldung", "ummeldung", 
                     "kennzeichen", "wunschkennzeichen", "abmeldung", "tüv", "hu", "hauptuntersuchung"],
        "description": "Anfragen zu Fahrzeugzulassungen, Ummeldungen und Kennzeichen"
    },
    "Gewerbeanmeldung": {
        "keywords": ["gewerbe", "gewerbeschein", "kleingewerbe", "freiberufler", "handelsregister",
                     "einzelunternehmen", "gmbh", "firma", "selbständig", "gewerbesteuer", "gewerbeamt"],
        "description": "Anfragen zu Gewerbeanmeldungen und geschäftlichen Tätigkeiten"
    },
    "Hundesteuer": {
        "keywords": ["hund", "hundesteuer", "hundemarke", "welpe", "haustier", "vierbeiner",
                     "kampfhund", "listenhund", "hundehalter", "hundeanmeldung"],
        "description": "Anfragen zur Anmeldung von Hunden und Hundesteuer"
    },
    "Nicht zuordenbar": {
        "keywords": [],
        "description": "Anfragen, die keiner der vordefinierten Kategorien zugeordnet werden können"
    }
}

# Konfidenz-Schwellenwert für die Kategorisierung
CONFIDENCE_THRESHOLD = 50

def classify_with_ollama(text):
    """
    Klassifiziert eine Anfrage mithilfe von Ollama in eine der Kategorien.
    
    Args:
        text: Der zu klassifizierende Text (Betreff + Nachricht)
        
    Returns:
        Tuple aus (Kategorie, Konfidenz)
    """
    # Erstelle einen Prompt für die Klassifikation (ohne "Nicht zuordbar")
    main_categories = {k: v for k, v in CATEGORIES.items() if k != "Nicht zuordbar"}
    categories_list = "\n".join([f"- {cat}: {info['description']}" for cat, info in main_categories.items()])
    
    prompt = f"""
    Klassifiziere die folgende Bürgeranfrage in GENAU EINE der folgenden Kategorien:
    
    {categories_list}
    
    Anfrage:
    {text}
    
    WICHTIG: 
    - Antworte NUR mit dem exakten Kategorienamen und einer Konfidenzbewertung
    - Wenn die Anfrage zu keiner Kategorie passt, antworte mit "KEINE|0"
    
    Format: KATEGORIE|KONFIDENZ
    Beispiel: KFZ-Zulassung|95
    """
    
    try:
        response = requests.post(
            f"{OLLAMA_URL}/api/generate",
            json={
                "model": OLLAMA_MODEL,
                "prompt": prompt,
                "stream": False,
                "temperature": 0.1  # Niedrige Temperatur für konsistente Antworten
            },
            timeout=30
        )
        
        if response.status_code == 200:
            result = response.json().get("response", "").strip()
            
            # Parse die Antwort
            if "|" in result:
                parts = result.split("|")
                category = parts[0].strip()
                try:
                    confidence = int(parts[1].strip().replace("%", ""))
                except:
                    confidence = 0
                
                # Prüfe ob es "KEINE" ist oder Konfidenz zu niedrig
                if category == "KEINE" or confidence < CONFIDENCE_THRESHOLD:
                    return "Nicht zuordbar", confidence
                
                # Validiere die Kategorie
                if category in main_categories:
                    return category, confidence
            
            # Fallback: Versuche die Kategorie direkt zu extrahieren
            for cat in main_categories:
                if cat.lower() in result.lower():
                    return cat, 80  # Standard-Konfidenz
            
            # Wenn keine Kategorie gefunden wurde, verwende Keyword-Matching
            return keyword_based_classification(text)
            
        else:
            print(f"Ollama-Fehler: {response.status_code}")
            return keyword_based_classification(text)
            
    except Exception as e:
        print(f"Fehler bei Ollama-Klassifikation: {str(e)}")
        return keyword_based_classification(text)

def keyword_based_classification(text):
    """
    Fallback-Klassifikation basierend auf Schlüsselwörtern.
    
    Args:
        text: Der zu klassifizierende Text
        
    Returns:
        Tuple aus (Kategorie, Konfidenz)
    """
    text_lower = text.lower()
    scores = {}
    
    # Nur die Hauptkategorien bewerten (nicht "Nicht zuordbar")
    main_categories = {k: v for k, v in CATEGORIES.items() if k != "Nicht zuordbar"}
    
    for category, info in main_categories.items():
        score = sum(1 for keyword in info["keywords"] if keyword in text_lower)
        scores[category] = score
    
    # Finde die Kategorie mit dem höchsten Score
    if scores:
        best_category = max(scores, key=scores.get)
        max_score = scores[best_category]
        
        if max_score > 0:
            # Berechne Konfidenz basierend auf gefundenen Keywords
            confidence = min(100, max_score * 20)  # 20% pro gefundenem Keyword
            
            # Prüfe ob Konfidenz über dem Schwellenwert liegt
            if confidence >= CONFIDENCE_THRESHOLD:
                return best_category, confidence
    
    # Wenn nichts gefunden wurde oder Konfidenz zu niedrig
    return "Nicht zuordbar", 30

@app.route('/')
def index():
    return render_template('upload.html')

@app.route('/upload', methods=['POST'])
def upload():
    # Eingaben erfassen
    first_name = request.form.get('first_name', '')
    last_name = request.form.get('last_name', '')
    subject = request.form.get('subject', '')
    e_mail = request.form.get('e_mail', '')
    message = request.form.get('message', '')
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    # Kombiniere Betreff und Nachricht für die Klassifikation
    full_text = f"{subject} {message}"
    
    # Klassifiziere die Anfrage
    category, confidence = classify_with_ollama(full_text)
    
    # Basisdateiname
    base_name = f"{last_name}_{first_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    

    # JSON-Daten vorbereiten
    data = {
        "zeitstempel": timestamp,
        "vorname": first_name,
        "nachname": last_name,
        "betreff": subject,
        "e_mail": e_mail,
        "nachricht": message,
        "kategorie": category,
        "konfidenz": confidence
    }
    
    # JSON-Datei speichern
    json_filename = f"{base_name}.json"
    json_path = os.path.join(UPLOAD_FOLDER, json_filename)
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
    
    return render_template('bestätigung.html', 
                         kategorie=category, 
                         konfidenz=confidence)

if __name__ == '__main__':
    app.run(debug=True)