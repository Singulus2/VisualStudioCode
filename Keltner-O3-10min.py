import os
import time
import numpy as np
import pandas as pd
from datetime import datetime, timedelta

import alpaca_trade_api as tradeapi

# API-Konfiguration (ersetze die folgenden Werte durch Deine Alpaca-API-Schlüssel und Endpunkt)
ALPACA_API_KEY = "DEIN_API_KEY"
ALPACA_SECRET_KEY = "DEIN_SECRET_KEY"
APCA_API_BASE_URL = "https://paper-api.alpaca.markets"  # oder die Live-URL, falls Du live handelst

api = tradeapi.REST(ALPACA_API_KEY, ALPACA_SECRET_KEY, APCA_API_BASE_URL, api_version='v2')

# Parameter der Strategie
EMA_PERIOD = 10         # Periode für den EMA der Close-Preise
ATR_PERIOD = 5          # Periode für den ATR
ATR_THRESHOLD = 40      # Schwellenwert zur Bestimmung der Seitwärtsbewegung (wird hier zwar berechnet, aber in den Entry-Bedingungen ist der Filter auskommentiert)
MULTIPLIER = 2          # Multiplikator für den Keltner Channel
QUANTITY = 100          # Anzahl der zu handelnden Aktien
SYMBOL = "AAPL"         # Beispielwert – passe das Symbol an

# Hilfsfunktionen zur Berechnung von Indikatoren
def calculate_indicators(df):
    """
    Erwartet ein DataFrame mit mindestens den Spalten: 'open', 'high', 'low', 'close'
    Gibt das DataFrame mit den berechneten Indikatoren zurück:
      - ema: EMA des Schlusskurses über EMA_PERIOD
      - tr: True Range
      - rangema: EMA des True Range über EMA_PERIOD
      - basis, upper, lower: Keltner Channel Werte
      - atr: ATR über ATR_PERIOD (hier als einfacher gleitender Durchschnitt der True Range)
    """
    # EMA des Schlusskurses
    df['ema'] = df['close'].ewm(span=EMA_PERIOD, adjust=False).mean()
    
    # Berechnung des True Range:
    # TR = max( high - low, abs(high - prev_close), abs(low - prev_close) )
    df['prev_close'] = df['close'].shift(1)
    df['tr1'] = df['high'] - df['low']
    df['tr2'] = (df['high'] - df['prev_close']).abs()
    df['tr3'] = (df['low'] - df['prev_close']).abs()
    df['tr'] = df[['tr1','tr2','tr3']].max(axis=1)
    
    # EMA des True Range (wie im Pinescript)
    df['rangema'] = df['tr'].ewm(span=EMA_PERIOD, adjust=False).mean()
    
    # Keltner Channel Berechnung
    df['basis'] = df['ema']
    df['upper'] = df['ema'] + df['rangema'] * MULTIPLIER
    df['lower'] = df['ema'] - df['rangema'] * MULTIPLIER
    
    # ATR über ATR_PERIOD (hier als einfacher gleitender Durchschnitt; alternativ kann auch Wilder's Methode verwendet werden)
    df['atr'] = df['tr'].rolling(window=ATR_PERIOD).mean()
    
    return df

def detect_cross(series_a, series_b):
    """
    Liefert einen Pandas Series mit boolschen Werten, die anzeigen, ob an der jeweiligen Stelle ein Crossover von series_a über series_b stattfindet.
    Crossover: Vorher (a < b) und aktuell (a >= b)
    """
    cross = (series_a.shift(1) < series_b.shift(1)) & (series_a >= series_b)
    return cross

def detect_crossunder(series_a, series_b):
    """
    Liefert einen Pandas Series mit boolschen Werten, die anzeigen, ob an der jeweiligen Stelle ein Crossunder von series_a unter series_b stattfindet.
    Crossunder: Vorher (a > b) und aktuell (a <= b)
    """
    crossunder = (series_a.shift(1) > series_b.shift(1)) & (series_a <= series_b)
    return crossunder

def get_latest_bars(symbol, timeframe="1Min", limit=50):
    """
    Ruft die letzten 'limit' Bars (Kerzen) für das angegebene Symbol ab.
    Du kannst diesen Teil anpassen, um Deine bevorzugte Datenquelle zu nutzen.
    """
    end_dt = datetime.utcnow()
    start_dt = end_dt - timedelta(minutes=limit*2)  # ein großzügiger Zeitraum, damit wir genug Bars bekommen
    bars = api.get_bars(symbol, timeframe, start_dt.isoformat(), end_dt.isoformat()).df
    # Alpaca liefert alle Symbole in einem DataFrame, filtere nach dem gewünschten Symbol:
    if symbol in bars.index.levels[0]:
        bars = bars.loc[symbol]
    else:
        raise ValueError(f"Keine Bars für Symbol {symbol} erhalten.")
    bars = bars.sort_index()  # sicherstellen, dass die Daten zeitlich sortiert sind
    return bars.tail(limit)

def get_current_position(symbol):
    """
    Liefert die aktuelle Position für das Symbol.
    Gibt 0 zurück, falls keine Position existiert.
    """
    try:
        position = api.get_position(symbol)
        # position.qty ist ein String – konvertieren wir ihn in float
        return float(position.qty)
    except Exception:
        return 0

def close_position(symbol):
    """
    Versucht, eine offene Position für das Symbol zu schließen.
    """
    try:
        api.close_position(symbol)
        print(f"Position für {symbol} geschlossen.")
    except Exception as e:
        print(f"Fehler beim Schließen der Position: {e}")

def submit_order(symbol, side, qty):
    """
    Schickt einen Market Order für das Symbol.
    side: "buy" oder "sell"
    """
    try:
        order = api.submit_order(
            symbol=symbol,
            qty=qty,
            side=side,
            type="market",
            time_in_force="gtc"
        )
        print(f"Order abgeschickt: {side} {qty} {symbol}")
    except Exception as e:
        print(f"Fehler beim Absenden der Order: {e}")

def strategy_logic():
    """
    Ruft die neuesten Bars ab, berechnet Indikatoren, detektiert Signale und führt ggf. Orders aus.
    """
    # Hole die aktuellen Kerzendaten
    try:
        df = get_latest_bars(SYMBOL, timeframe="1Min", limit=50)
    except Exception as e:
        print("Fehler beim Abrufen der Bars:", e)
        return

    # Berechne Indikatoren
    df = calculate_indicators(df)

    # Um die Strategiebedingungen zu prüfen, benötigen wir mindestens 3 Kerzen.
    if len(df) < 3:
        print("Nicht genügend Daten, um Signale zu berechnen.")
        return

    # Nimm die letzten drei Zeilen
    recent = df.iloc[-3:].copy()
    # Letzte Zeile: aktuelle Kerze
    current = recent.iloc[-1]
    prev = recent.iloc[-2]
    prev2 = recent.iloc[-3]

    # Berechnung der Signale (analog zum Pinescript)
    # Wir berechnen hier, ob an der aktuellen Bar ein Cross unter bzw. Cross over vorliegt.
    # Signal für Long: low - 5 kreuzt unter den unteren Kanal
    # Signal für Short: high + 5 kreuzt über den oberen Kanal
    # Wir berechnen den Wert (low - 5) und (high + 5) für die aktuelle und vorherige Bar.
    low_minus = df['low'] - 5
    high_plus = df['high'] + 5

    crossunder_series = detect_crossunder(low_minus, df['lower'])
    crossover_series  = detect_cross(high_plus, df['upper'])

    # Die Signale beziehen sich auf die letzte Kerze:
    goLong = crossunder_series.iloc[-1]
    goShort = crossover_series.iloc[-1]

    # Stop-Loss / Take-Profit Logik:
    # Falls wir short sind (Position < 0) und der Close steigt (close > prev_close und prev_close > prev2_close), dann schließen und Long gehen.
    # Falls wir long sind (Position > 0) und der Close fällt (close < prev_close und prev_close < prev2_close), dann schließen und Short gehen.
    current_position = get_current_position(SYMBOL)

    stopLongCondition = (current_position < 0) and (current['close'] > prev['close'] and prev['close'] > prev2['close'])
    stopShortCondition = (current_position > 0) and (current['close'] < prev['close'] and prev['close'] < prev2['close'])

    if stopLongCondition:
        print("Stopbedingung für Short-Position erfüllt. Wechsel zu Long.")
        close_position(SYMBOL)
        submit_order(SYMBOL, "buy", QUANTITY)
        return

    if stopShortCondition:
        print("Stopbedingung für Long-Position erfüllt. Wechsel zu Short.")
        close_position(SYMBOL)
        submit_order(SYMBOL, "sell", QUANTITY)
        return

    # Handelslogik: Nur handeln, wenn kein bestehender Trade in gleicher Richtung vorliegt.
    if goLong and current_position <= 0:
        # Falls bereits short positioniert, zuerst schließen
        if current_position < 0:
            print("Short-Position vorhanden. Schließe Position bevor Long eingegangen wird.")
            close_position(SYMBOL)
            time.sleep(1)  # Kurze Wartezeit, um die Orderausführung zu ermöglichen
        print("Go Long Signal erkannt.")
        submit_order(SYMBOL, "buy", QUANTITY)

    elif goShort and current_position >= 0:
        if current_position > 0:
            print("Long-Position vorhanden. Schließe Position bevor Short eingegangen wird.")
            close_position(SYMBOL)
            time.sleep(1)
        print("Go Short Signal erkannt.")
        submit_order(SYMBOL, "sell", QUANTITY)
    else:
        print("Kein Handelssignal erkannt.")

if __name__ == "__main__":
    # Beispielhafter Loop: Prüfe jede Minute (oder in Deinem gewünschten Intervall) die Strategie
    while True:
        print(f"\n[{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}] Überprüfe Handelssignale für {SYMBOL} ...")
        strategy_logic()
        # Warte bis zur nächsten Kerze (z. B. 60 Sekunden)
        time.sleep(60)
