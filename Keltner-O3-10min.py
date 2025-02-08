import asyncio
import datetime
import pandas as pd
import numpy as np

import alpaca_trade_api as tradeapi
from alpaca_trade_api.stream import Stream

# === API-Konfiguration ===
API_KEY = 'PK6BCDFSK9I0CRHD2XCU'
API_SECRET = 'WNG1EKfH7WZfYggeCtirEIaIS1glCz2yUa3qyock'
BASE_URL = 'https://paper-api.alpaca.markets'  # Paper Trading URL

# REST-Client für Ordermanagement
api = tradeapi.REST(API_KEY, API_SECRET, BASE_URL, api_version='v2')
symbol = "SPY"  # Beispielsymbol – anpassen!

# === Globale Variablen ===
# Hier werden empfangene 1‑Minuten‑Bars gesammelt
minute_bars = []  
# Hier sammeln wir aggregierte 10‑Minuten‑Bars (als Liste von Dicts)
aggregated_bars = []  
# Anzahl der 1‑Minuten‑Bars, die zu einem Aggregat zusammengefasst werden sollen
AGGREGATION_COUNT = 10  

# Wir nutzen diese Funktion, um den aktuellen Status der Position zu erfragen.
def get_current_position(symbol):
    try:
        pos = api.get_position(symbol)
        return float(pos.qty)
    except Exception:
        return 0

# --- Indikatorfunktionen ---
SMA_PERIOD = 10      # Periode für den SMA und für den SMA des True Range
ATR_PERIOD = 10       # Periode für den ATR
MULTIPLIER = 2       # Multiplikator für den Keltner Channel
# ATR_THRESHOLD = 40   # Schwellenwert zur Filterung (im Pinescript nur für Seitwärtsbewegung)

def calculate_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """
    Berechnet die folgenden Indikatoren:
      - SMA des Schlusskurses (sma)
      - True Range (tr) und dessen SMA (rangema)
      - Keltner Channel: basis, upper und lower
      - ATR (als gleitender Durchschnitt der tr über ATR_PERIOD)
    """
    df = df.copy()
    # SMA des Schlusskurses
    df['sma'] = df['close'].rolling(window=AGGREGATION_COUNT).mean() #df['close'].swm(span=EMA_PERIOD, adjust=False).mean()
    
    # Berechnung des True Range
    df['prev_close'] = df['close'].shift(1)
    df['tr1'] = df['high'] - df['low']
    df['tr2'] = (df['high'] - df['prev_close']).abs()
    df['tr3'] = (df['low'] - df['prev_close']).abs()
    df['tr'] = df[['tr1','tr2','tr3']].max(axis=1)
    
    # SMA des True Range (rangema)
    df['rangema'] = df['tr'].rolling(window=AGGREGATION_COUNT).mean() #ewm(span=EMA_PERIOD, adjust=False).mean()
    
    # Keltner Channel
    df['basis'] = df['sma']
    df['upper'] = df['sma'] + df['rangema'] * MULTIPLIER
    df['lower'] = df['sma'] - df['rangema'] * MULTIPLIER
    
    # ATR als einfacher gleitender Durchschnitt der tr
    df['atr'] = df['tr'].rolling(window=ATR_PERIOD).mean()
    
    return df

def detect_cross(series_a: pd.Series, series_b: pd.Series) -> pd.Series:
    """
    Liefert eine Series, die anzeigt, ob es einen Crossover von series_a über series_b gibt.
    Crossover: Vorher (a < b) und aktuell (a >= b).
    """
    cross = (series_a.shift(1) < series_b.shift(1)) & (series_a >= series_b)
    return cross

def detect_crossunder(series_a: pd.Series, series_b: pd.Series) -> pd.Series:
    """
    Liefert eine Series, die anzeigt, ob es einen Crossunder von series_a unter series_b gibt.
    Crossunder: Vorher (a > b) und aktuell (a <= b).
    """
    crossunder = (series_a.shift(1) > series_b.shift(1)) & (series_a <= series_b)
    return crossunder

# --- Aggregation der 1‑Minuten‑Bars zu einem 10‑Minuten‑Bar ---
def aggregate_bars(bars: list) -> dict:
    """
    Aggregiert eine Liste von Bars (in chronologischer Reihenfolge) zu einem neuen Bar.
      - open: Erster Open
      - high: Maximaler High-Wert
      - low: Minimaler Low-Wert
      - close: Letzter Close
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
        print(f"Long-Order gesendet: {order}")
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
        print(f"Short-Order gesendet: {order}")
        return order
    except Exception as e:
        print("Fehler beim Senden der Short-Order:", e)
        return None

def submit_exit_order(symbol, qty, side):
    """
    Bei einem Exit: Bei einer long Position wird verkauft (side='sell'), 
    bei einer short Position gekauft (side='buy').
    """
    try:
        order = api.submit_order(
            symbol=symbol,
            qty=qty,
            side=side,
            type='market',
            time_in_force='day'
        )
        print(f"Exit-Order gesendet: {order}")
        return order
    except Exception as e:
        print("Fehler beim Senden der Exit-Order:", e)
        return None

# --- Strategie-Logik (basierend auf Deinem ursprünglichen Pinescript) ---
# Hier wird der Keltner-Channel genutzt und es werden Ein-/Ausstiege bestimmt.
# Diese Funktion wird auf die gesammelten aggregierten Bars angewendet.
def strategy_logic():
    global aggregated_bars

    if len(aggregated_bars) < 3:
        print("Nicht genügend aggregierte Bars vorhanden (mindestens 3 benötigt).")
        return

    # Erstelle ein DataFrame aus den aggregierten Bars
    df = pd.DataFrame(aggregated_bars)
    df.sort_values(by='timestamp', inplace=True)
    df.reset_index(drop=True, inplace=True)

    # Berechne die Indikatoren
    df = calculate_indicators(df)

    # Zur Signaldetektion benötigen wir mindestens 3 Bars:
    recent = df.iloc[-3:].copy()
    current = recent.iloc[-1]
    prev = recent.iloc[-2]
    prev2 = recent.iloc[-3]

    # Filter: Optional kann hier isRange (ATR < Schwellenwert) berücksichtigt werden.
    atrValue = current['atr']
    # isRange = atrValue < ATR_THRESHOLD

    # Berechne Signale:
    # goLong: Wenn (low - 5) den unteren Kanal (lower) von oben nach unten kreuzt.
    # goShort: Wenn (high + 5) den oberen Kanal (upper) von unten nach oben kreuzt.
    low_minus = df['low'] # - 5
    high_plus = df['high'] # + 5

    crossunder_series = detect_crossunder(low_minus, df['lower'])
    crossover_series = detect_cross(high_plus, df['upper'])

    goLong = crossunder_series.iloc[-1]  # Signal für Long-Einstieg
    goShort = crossover_series.iloc[-1]  # Signal für Short-Einstieg

    
    current_position = get_current_position(symbol)

     # Einstiegssignale:
    if goLong:
        if current_position != 0:
            print("Short-Position vorhanden. Schließe Position bevor Long eingegangen wird.")
            submit_exit_order(symbol, qty=10, side='buy')
        print("Go Long Signal erkannt.")
        submit_long_order(symbol, qty=10)
    elif goShort:
        if current_position != 0:
            print("Long-Position vorhanden. Schließe Position bevor Short eingegangen wird.")
            submit_exit_order(symbol, qty=10, side='sell')
        print("Go Short Signal erkannt.")
        submit_short_order(symbol, qty=10)
    else:
        print("Kein Handelssignal erkannt.")

# --- Asynchrone Callback-Funktion für empfangene 1‑Minuten‑Bars ---
async def on_bar(bar):
    global minute_bars, aggregated_bars

    # Extrahiere die 1‑Minuten‑Bar als Dictionary
    current_min_bar = {
        'open': float(bar.open),
        'high': float(bar.high),
        'low': float(bar.low),
        'close': float(bar.close),
        'timestamp': bar.timestamp  # Dies ist ein datetime-Objekt
    }
    print(f"[{bar.timestamp}] 1‑Minuten‑Bar: O={bar.open} H={bar.high} L={bar.low} C={bar.close}")

    # Füge den empfangenen Bar der Liste hinzu
    minute_bars.append(current_min_bar)

    # Sobald genügend 1‑Minuten‑Bars vorliegen, aggregiere sie zu einem 10‑Minuten‑Bar
    if len(minute_bars) >= AGGREGATION_COUNT:
        agg_bars = minute_bars[:AGGREGATION_COUNT]
        # Entferne die bereits aggregierten Bars
        minute_bars[:] = minute_bars[AGGREGATION_COUNT:]
        agg_bar = aggregate_bars(agg_bars)
        aggregated_bars.append(agg_bar)
        print(f"[{agg_bar['timestamp']}] Aggregierter 10‑Minuten‑Bar: O={agg_bar['open']} H={agg_bar['high']} L={agg_bar['low']} C={agg_bar['close']}")
        
        # Optional: Begrenze die Anzahl der aggregierten Bars (z. B. auf 50)
        if len(aggregated_bars) > 50:
            aggregated_bars = aggregated_bars[-50:]
        
        # Wende die Strategie-Logik auf den aktualisierten Datensatz an
        strategy_logic()

# --- Hauptprogramm: Asynchroner Stream über Websocket ---
async def main():
    stream = Stream(API_KEY,
                    API_SECRET,
                    base_url=BASE_URL,
                    data_feed='iex')  # oder 'sip' falls verfügbar
    stream.subscribe_bars(on_bar, symbol)
    print(f"Starte den Intraday-Stream für {symbol} ...")
    await stream._run_forever()

if __name__ == "__main__":
    asyncio.run(main())
