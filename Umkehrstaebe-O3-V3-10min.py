import asyncio
import alpaca_trade_api as tradeapi
from alpaca_trade_api.stream import Stream
import datetime

# === API-Konfiguration ===
API_KEY = 'PK6BCDFSK9I0CRHD2XCU'
API_SECRET = 'WNG1EKfH7WZfYggeCtirEIaIS1glCz2yUa3qyock'
BASE_URL = 'https://paper-api.alpaca.markets'  # Paper Trading URL

# REST-Client für Ordermanagement
api = tradeapi.REST(API_KEY, API_SECRET, BASE_URL, api_version='v2')
symbol = "SPY"

# Globaler Zustand für einen aktiven Trade (None, falls kein Trade aktiv)
# Gespeichert werden:
#   - type: 'long' oder 'short'
#   - entry: Einstiegspreis
#   - trailing_stop: aktuell gesetzter Stop
#   - pending_outside_bar: Kandidat für den Aussenstab (wird noch nicht als Aussenstab gewertet)
#   - confirmed_outside_bar: bestätigter Aussenstab (wird gesetzt, sobald der nachfolgende Bar als Innenstab gilt)
#   - inside_series: Flag, ob bereits ein Innenstab in der aktuellen Reihe registriert wurde
#   - entry_bar: der 10‑Minuten‑Bar, an dem der Trade eingestiegen wurde (als Aggregat)
current_position = None

# Für die Aggregation der 1‑Minuten‑Bars zu 10‑Minuten‑Bars
minute_bars = []  # Hier werden die empfangenen 1‑Minuten‑Bars gesammelt

# Für die Kandidatenlogik der Umkehrstäbe
prev_agg_bar = None                 # Letzter verarbeiteter 10‑Minuten‑Bar
candidate_short_reversal = None       # Kandidat für einen Short-Umkehrstab
candidate_long_reversal = None        # Kandidat für einen Long-Umkehrstab

# --- Funktion: Aggregation von 1‑Minuten‑Bars zu einem 10‑Minuten‑Bar ---
def aggregate_bars(bars):
    """
    Aggregiert eine Liste von 1‑Minuten‑Bars (in chronologischer Reihenfolge) zu einem 10‑Minuten‑Bar.
      - open: Open des ersten Bars
      - high: Maximaler High-Wert
      - low: Minimaler Low-Wert
      - close: Close des letzten Bars
      - timestamp: Timestamp des letzten Bars (als Referenz)
    """
    open_price = bars[0]['open']
    high_price = max(b['high'] for b in bars)
    low_price = min(b['low'] for b in bars)
    close_price = bars[-1]['close']
    timestamp = bars[-1]['timestamp']
    return {
        'open': open_price,
        'high': high_price,
        'low': low_price,
        'close': close_price,
        'timestamp': timestamp
    }

# --- Hilfsfunktion: Prüft, ob ein Bar ein Innenstab relativ zu einem Referenz-Bar ist ---
def is_inside_bar(current, reference):
    """
    Ein Bar (current) gilt als Innenstab, wenn sein Open und Close innerhalb der Spanne (Low, High)
    des Referenz-Bars liegen.
    """
    return (reference['low'] < current['open'] < reference['high'] and
            reference['low'] < current['close'] < reference['high'])

# --- Orderfunktionen ---
def submit_long_order(symbol, qty):
    try:
        order = api.submit_order(
            symbol=symbol,
            qty=qty,
            side='buy',
            type='market',
            time_in_force='day'
        )
        print("Long-Order gesendet:", order)
        return order
    except Exception as e:
        print("Fehler beim Senden der Long-Order:", e)
        return None

def submit_short_order(symbol, qty):
    try:
        order = api.submit_order(
            symbol=symbol,
            qty=qty,
            side='sell',
            type='market',
            time_in_force='day'
        )
        print("Short-Order gesendet:", order)
        return order
    except Exception as e:
        print("Fehler beim Senden der Short-Order:", e)
        return None

def submit_exit_order(symbol, qty, side):
    try:
        order = api.submit_order(
            symbol=symbol,
            qty=qty,
            side=side,  # Bei Long: 'sell', bei Short: 'buy'
            type='market',
            time_in_force='day'
        )
        print("Exit-Order gesendet:", order)
        return order
    except Exception as e:
        print("Fehler beim Senden der Exit-Order:", e)
        return None

# --- Funktion zur Verarbeitung eines 10‑Minuten‑Bars (Aggregat) ---
def process_10min_bar(current):
    """
    Wendet die Strategie-Logik (Einstieg, Trailing-Stop-Management, Ausstieg)
    auf den aktuellen 10‑Minuten‑Bar an.
    
    Für den Short-Einstieg:
      - Es wird ein Umkehrstab-Kandidat gesetzt, wenn der vorherige 10‑Minuten‑Bar bullisch war
        und der aktuelle 10‑Minuten‑Bar bearish ist.
      - Wird im nächsten Bar das Tief des Kandidaten unterschritten, erfolgt der Short-Einstieg.

    Für den Long-Einstieg (spiegelverkehrt):
      - Es wird ein Umkehrstab-Kandidat gesetzt, wenn der vorherige 10‑Minuten‑Bar bärisch war
        und der aktuelle 10‑Minuten‑Bar bullish ist.
      - Wird im nächsten Bar das Hoch des Kandidaten überschritten, erfolgt der Long-Einstieg.
    """
    global current_position, prev_agg_bar, candidate_short_reversal, candidate_long_reversal

    print(f"[{current['timestamp']}] 10-Minuten-Bar: O={current['open']} H={current['high']} L={current['low']} C={current['close']}")

    # --- Kein aktiver Trade: Einstiegssignal prüfen ---
    if current_position is None:
        # Short-Einstieg:
        if candidate_short_reversal is None and prev_agg_bar is not None:
            # Kandidat setzen, wenn der vorherige Bar bullisch war und der aktuelle bearish ist:
            if prev_agg_bar['close'] > prev_agg_bar['open'] and current['close'] < current['open']:
                candidate_short_reversal = current
                print(f"Kandidat Short-Umkehrstab erkannt bei {current['timestamp']}. Warte auf Unterschreitung des Tiefs ({current['low']}).")
        
        # Prüfe, ob ein Kandidat für Short bereits besteht und ob der aktuelle Bar das Tief des Kandidaten unterschreitet:
        if candidate_short_reversal is not None:
            #TODO: Wenn candidate_short_reversal = current gerade gesetzt wurde sind die Werte identisch und der Einstieg wird nicht ausgelöst
            #      Das könnte korrekt sein? Analoge stellt sich bei Frage bei Long-Einstieg
            if current['low'] < candidate_short_reversal['low']:
                print(f"Short-Einstieg ausgelöst: Bei {current['timestamp']} wurde das Tief des Kandidaten ({candidate_short_reversal['low']}) unterschritten.")
                order = submit_short_order(symbol, qty=10)
                if order:
                    current_position = {
                        'type': 'short',
                        'entry': current['open'],  # Einstieg zum Open des aktuellen Bars
                        'trailing_stop': candidate_short_reversal['high'],  # Initialer Stop: Hoch des Kandidaten
                        'pending_outside_bar': current,  # Für Trailing-Stop-Updates
                        'confirmed_outside_bar': None,
                        'inside_series': False,
                        'entry_bar': current
                    }
                candidate_short_reversal = None
                # Setze auch candidate_long_reversal zurück, falls vorhanden
                candidate_long_reversal = None
                prev_agg_bar = current
                return

        # Long-Einstieg (spiegelverkehrt):
        if candidate_long_reversal is None and prev_agg_bar is not None:
            # Kandidat setzen, wenn der vorherige Bar bärisch war und der aktuelle bullish ist:
            if prev_agg_bar['close'] < prev_agg_bar['open'] and current['close'] > current['open']:
                candidate_long_reversal = current
                print(f"Kandidat Long-Umkehrstab erkannt bei {current['timestamp']}. Warte auf Überschreitung des Hochs ({current['high']}).")
        
        # Prüfe, ob ein Kandidat für Long besteht und ob der aktuelle Bar das Hoch des Kandidaten überschreitet:
        if candidate_long_reversal is not None:
            if current['high'] > candidate_long_reversal['high']:
                print(f"Long-Einstieg ausgelöst: Bei {current['timestamp']} wurde das Hoch des Kandidaten ({candidate_long_reversal['high']}) überschritten.")
                order = submit_long_order(symbol, qty=10)
                if order:
                    current_position = {
                        'type': 'long',
                        'entry': current['open'],  # Einstieg zum Open des aktuellen Bars
                        'trailing_stop': candidate_long_reversal['low'],  # Initialer Stop: Tief des Kandidaten
                        'pending_outside_bar': current,
                        'confirmed_outside_bar': None,
                        'inside_series': False,
                        'entry_bar': current
                    }
                candidate_long_reversal = None
                # Setze auch candidate_short_reversal zurück, falls vorhanden
                candidate_short_reversal = None
                prev_agg_bar = current
                return

        # Kein Einstieg: Aktualisiere prev_agg_bar und beende Verarbeitung
        prev_agg_bar = current
        return

    # --- Trade-Management, wenn ein Trade aktiv ist ---
    pos = current_position

    # Ausstiegsbedingungen:
    if pos['type'] == 'long':
        # Long: Exit, wenn entweder
        # (a) das aktuelle Low unter den Trailing Stop fällt
        # oder (b) – falls vorhanden – der Schlusskurs unter das Tief des bestätigten Aussenstabs fällt.
        if current['low'] < pos['trailing_stop']: # or (pos['confirmed_outside_bar'] is not None and current['close'] < pos['confirmed_outside_bar']['low']):
            print(f"Exit-Bedingung (Long) erfüllt bei {current['timestamp']}. Schließe Long-Position.")
            submit_exit_order(symbol, qty=10, side='sell')
            current_position = None
            prev_agg_bar = current
            return
    else:  # Short
        if current['high'] > pos['trailing_stop']: # or (pos['confirmed_outside_bar'] is not None and current['close'] > pos['confirmed_outside_bar']['high']):
            print(f"Exit-Bedingung (Short) erfüllt bei {current['timestamp']}. Schließe Short-Position.")
            submit_exit_order(symbol, qty=10, side='buy')
            current_position = None
            prev_agg_bar = current
            return
    
    # --- Trailing-Stop-Management und Aussenstab-Aktualisierung ---
    if pos['type'] == 'long':
        if is_inside_bar(current, pos['pending_outside_bar']):
        # Der aktuelle Bar gilt als Innenstab relativ zum pending Kandidaten.
            if pos['confirmed_outside_bar'] is None:
            # Erster Innenstab: Setze den Vorgängerstab (prev_agg_bar) als confirmed outside bar
            # und setze den Trailing Stop auf den low des Vorgängerstabs.
                pos['confirmed_outside_bar'] = prev_agg_bar
                alt_stop = prev_agg_bar['low']
                print(f"Erster Innenstab (Long) erkannt bei {current['timestamp']}.")
                print(f"Setze confirmed outside bar auf Vorgängerstab ({prev_agg_bar['timestamp']}) und Trailing Stop auf {alt_stop}.")
                pos['trailing_stop'] = alt_stop
                pos['inside_series'] = True
            else:
                # Bereits in Innenstab-Reihe: Keine Änderung des Trailing Stops.
                print(f"Innenstab-Reihe (Long) fortlaufend bei {current['timestamp']}. Trailing Stop bleibt bei {pos['trailing_stop']}.")
        else:
        # Aktueller Bar gilt nicht als Innenstab – also außerhalb der Range des bisherigen Kandidaten:
            pos['pending_outside_bar'] = current
            pos['confirmed_outside_bar'] = None  # Zurücksetzen, da neuer Kandidat vorliegt.
            pos['inside_series'] = False
        # Trailing Stop-Nachzug: Wenn der aktuelle Bar einen höheren low liefert als der bisherige Trailing Stop,
        # wird dieser nachgezogen.
            if current['low'] > pos['trailing_stop']:
                print(f"Neuer Kandidat (Long) bei {current['timestamp']}. Trailing Stop wird auf {current['low']} nachgezogen.")
                pos['trailing_stop'] = current['low']
            else:
                print(f"Neuer Kandidat (Long) bei {current['timestamp']} ohne Anpassung (Trailing Stop bleibt bei {pos['trailing_stop']}).")
    else:  # Short Trade (spiegelverkehrt)
        if is_inside_bar(current, pos['pending_outside_bar']):
            if pos['confirmed_outside_bar'] is None:
            # Beim ersten Innenstab: Setze den Vorgängerstab (prev_agg_bar) als confirmed outside bar
            # und setze den Trailing Stop auf den high des Vorgängerstabs.
                pos['confirmed_outside_bar'] = prev_agg_bar
                alt_stop = prev_agg_bar['high']
                print(f"Erster Innenstab (Short) erkannt bei {current['timestamp']}.")
                print(f"Setze confirmed outside bar auf Vorgängerstab ({prev_agg_bar['timestamp']}) und Trailing Stop auf {alt_stop}.")
                pos['trailing_stop'] = alt_stop
                pos['inside_series'] = True
            else:
                print(f"Innenstab-Reihe (Short) fortlaufend bei {current['timestamp']}. Trailing Stop bleibt bei {pos['trailing_stop']}.")
        else:
            pos['pending_outside_bar'] = current
            pos['confirmed_outside_bar'] = None
            pos['inside_series'] = False
        # Trailing Stop-Nachzug: Wenn der aktuelle Bar einen niedrigeren high liefert als der bisherige Trailing Stop,
        # wird dieser nachgezogen.
            if current['high'] < pos['trailing_stop']:
                print(f"Neuer Kandidat (Short) bei {current['timestamp']}. Trailing Stop wird auf {current['high']} nachgezogen.")
                pos['trailing_stop'] = current['high']
            else:
                print(f"Neuer Kandidat (Short) bei {current['timestamp']} ohne Anpassung (Trailing Stop bleibt bei {pos['trailing_stop']}).")


    prev_agg_bar = current

# --- Asynchrone Callback-Funktion für jeden empfangenen 1‑Minuten‑Bar ---
async def on_bar(bar):
    global minute_bars

    # Extrahiere die 1‑Minuten‑Bar-Daten als Dictionary
    current_min_bar = {
        'open': bar.open,
        'high': bar.high,
        'low': bar.low,
        'close': bar.close,
        'timestamp': bar.timestamp
    }
    print(f"[{bar.timestamp}] 1-Minuten-Bar: O={bar.open} H={bar.high} L={bar.low} C={bar.close}")

    # Füge den 1‑Minuten‑Bar der Liste hinzu
    minute_bars.append(current_min_bar)

    # Sobald 10 1‑Minuten‑Bars vorliegen, aggregiere sie zu einem 10‑Minuten‑Bar
    if len(minute_bars) >= 2:
        agg_bar = aggregate_bars(minute_bars[:2])
        minute_bars[:] = minute_bars[2:]
        process_10min_bar(agg_bar)

# --- Hauptprogramm: Websocket-Verbindung und Start des Streams ---
async def main():
    stream = Stream(API_KEY,
                    API_SECRET,
                    base_url=BASE_URL,
                    data_feed='iex')  # 'iex' für kostenlose Echtzeit-Daten, alternativ 'sip'
    stream.subscribe_bars(on_bar, symbol)
    print(f"Starte den Intraday-Stream für {symbol} ...")
    await stream._run_forever()

if __name__ == "__main__":
    asyncio.run(main())
