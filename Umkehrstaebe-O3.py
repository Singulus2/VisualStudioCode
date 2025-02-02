import asyncio
import alpaca_trade_api as tradeapi
from alpaca_trade_api.stream import Stream

# === API-Konfiguration ===
API_KEY = 'your_api_key'
API_SECRET = 'your_api_secret'
BASE_URL = 'https://paper-api.alpaca.markets'  # Paper Trading URL

# REST-Client für Ordermanagement
api = tradeapi.REST(API_KEY, API_SECRET, BASE_URL, api_version='v2')

symbol = "AAPL"

# Globaler Zustand für einen aktiven Trade (None, falls kein Trade aktiv)
# Die Trade-State enthält:
#   - type: 'long'
#   - entry: Einstiegspreis
#   - trailing_stop: Aktuell gesetzter Stop
#   - outside_bar: Der letzte Bar, der als Aussenstab gilt (für Stop-Anpassungen)
#   - outside_bar_prev: Der Bar direkt vor dem Aussenstab (wird für den ersten Innenstab benötigt)
#   - inside_series: Boolean, ob bereits ein Innenstab in der aktuellen Reihe registriert wurde
current_position = None

# Wir speichern den letzten empfangenen Bar je Symbol, falls noch benötigt
last_bar_by_symbol = {}

# --- Hilfsfunktionen zur Klassifikation der Bars ---

def is_inside_bar(current, reference):
    """
    Ein Bar (current) gilt als Innenstab, wenn sein Eröffnungs- und Schlusskurs
    innerhalb der Spanne (low, high) des Referenz-Bars liegen.
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

def submit_exit_order(symbol, qty, side):
    try:
        order = api.submit_order(
            symbol=symbol,
            qty=qty,
            side=side,  # Bei Long-Position: side='sell'
            type='market',
            time_in_force='day'
        )
        print("Exit-Order gesendet:", order)
        return order
    except Exception as e:
        print("Fehler beim Senden der Exit-Order:", e)
        return None

# --- Asynchrone Callback-Funktion für jeden empfangenen 1-Minuten-Bar ---

async def on_bar(bar):
    global current_position, last_bar_by_symbol

    # Aktuelle Bar-Daten extrahieren
    current = {
        'open': bar.open,
        'high': bar.high,
        'low': bar.low,
        'close': bar.close,
        'timestamp': bar.timestamp  # Für Debugging
    }
    print(f"[{bar.timestamp}] {bar.symbol} Bar: O={bar.open} H={bar.high} L={bar.low} C={bar.close}")

    # Speichere den zuletzt empfangenen Bar für das Symbol
    last_bar_by_symbol[bar.symbol] = current

    # Wenn noch kein Trade aktiv ist, suchen wir nach einem Einstiegssignal
    if current_position is None:
        # Hier könnte man z. B. einen Umkehrstab als Einstiegssignal definieren.
        # In diesem Beispiel nehmen wir an, dass wir den Einstieg manuell oder über ein anderes Signal erhalten.
        # Für Demonstrationszwecke: wenn der aktuelle Bar bullisch ist (Close > Open) und ein bestimmtes Kriterium erfüllt,
        # könnte man hier einen Einstieg setzen.
        # (Diese Einstiegskriterien sind frei wählbar – hier nur ein Beispiel:)
        if current['close'] > current['open'] and current['high'] - current['low'] > 0.5:
            print(f"Einstiegssignal erkannt bei {bar.symbol} um {bar.timestamp}. Starte Long-Trade.")
            order = submit_long_order(bar.symbol, qty=10)
            if order:
                # Beim Einstieg setzen wir:
                # - trailing_stop initial auf das Tief des Einstiegs-Bars
                # - Der aktuelle Bar wird als Aussenstab festgelegt.
                # - Als outside_bar_prev nehmen wir den letzten Bar vor dem aktuellen (falls vorhanden)
                outside_bar_prev = last_bar_by_symbol.get(bar.symbol, None)
                current_position = {
                    'type': 'long',
                    'entry': current['open'],  # Annahme: Einstieg erfolgt zum Open des Bars
                    'trailing_stop': current['low'],
                    'outside_bar': current,      # Dieser Bar gilt als erster Aussenstab
                    'outside_bar_prev': outside_bar_prev,  # Kann None sein, wenn kein Vorgänger vorhanden
                    'inside_series': False
                }
        return  # Ohne aktiven Trade ist nichts weiter zu tun

    # --- Trade-Management (bei aktivem Trade) ---
    pos = current_position

    # Zuerst prüfen wir die Ausstiegsbedingungen:
    # 1. Fällt der aktuelle Bar unter den gesetzten Trailing Stop
    # 2. Oder fällt der aktuelle Bar unter das Tief des Aussenstabs
    if current['low'] < pos['trailing_stop'] or current['low'] < pos['outside_bar']['low']:
        print(f"Exit-Bedingung erfüllt bei {bar.symbol} um {bar.timestamp}. Schließe Long-Position.")
        submit_exit_order(bar.symbol, qty=10, side='sell')
        current_position = None
        return

    # Jetzt die Aktualisierung der Stop-Logik:
    # Prüfe, ob der aktuelle Bar als Innenstab zu dem aktuellen Aussenstab gilt.
    if is_inside_bar(current, pos['outside_bar']):
        # Es ist ein Innenstab.
        if not pos['inside_series']:
            # Dies ist der erste Innenstab in der Reihe.
            if pos['outside_bar_prev'] is not None:
                # Setze den Trailing Stop zurück auf das Tief des Vorgängerstabs des aktuellen Aussenstabs.
                alt_stop = pos['outside_bar_prev']['low']
                print(f"Erster Innenstab erkannt bei {bar.symbol} um {bar.timestamp}. " +
                      f"Setze Trailing Stop von {pos['trailing_stop']} auf {alt_stop}.")
                pos['trailing_stop'] = alt_stop
            else:
                print("Erster Innenstab, aber kein Vorgänger für Aussenstab vorhanden.")
            pos['inside_series'] = True
        else:
            # Weitere Innenstäbe: Trailing Stop wird nicht verändert.
            print(f"Innenstab-Reihe (fortlaufend) bei {bar.symbol} um {bar.timestamp}. Kein Update des Stops.")
    else:
        # Kein Innenstab: Es handelt sich um einen neuen Aussenstab.
        # Wenn der aktuelle Bar oberhalb des Hochs des bisherigen Aussenstabs liegt,
        # wird der Trailing Stop auf das Tief des aktuellen Bars nachgezogen.
        if current['high'] > pos['outside_bar']['high']:
            alt_stop = current['low']
            print(f"Neuer Aussenstab bei {bar.symbol} um {bar.timestamp} (Hoch {current['high']} > " +
                  f"vorheriges Hoch {pos['outside_bar']['high']}). " +
                  f"Trailing Stop wird auf {alt_stop} nachgezogen.")
            # Aktualisiere den Trade-Zustand: Der aktuelle Bar wird neuer Aussenstab,
            # und der alte Aussenstab wird zum Vorgänger (outside_bar_prev)
            pos['outside_bar_prev'] = pos['outside_bar']
            pos['outside_bar'] = current
            pos['trailing_stop'] = alt_stop
            pos['inside_series'] = False  # Zurücksetzen, da keine Innenstabreihe vorliegt
        else:
            print(f"Kein Innenstab und auch kein neuer Aussenstab bei {bar.symbol} um {bar.timestamp}. " +
                  f"Kein Update des Trailing Stops (aktuell: {pos['trailing_stop']}).")

# --- Hauptprogramm: Websocket-Verbindung und Start des Streams ---

async def main():
    stream = Stream(API_KEY,
                    API_SECRET,
                    base_url=BASE_URL,
                    data_feed='iex')  # Verwende 'iex' für kostenlose Echtzeit-Daten (alternativ 'sip', falls verfügbar)

    stream.subscribe_bars(on_bar, symbol)

    print(f"Starte den Intraday-Stream für {symbol} ...")
    await stream._run_forever()

if __name__ == "__main__":
    asyncio.run(main())
