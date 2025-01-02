import alpaca_trade_api as tradeapi
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import time
import logging

class MarkttechnikStrategy:
    def __init__(self, api_key, api_secret, base_url, symbol):
        self.setup_logging()
        self.logger.info(f"Initialisiere Strategie für {symbol}")
        
        self.api = tradeapi.REST(api_key, api_secret, base_url, api_version='v2')
        self.symbol = symbol
        
        # Status-Variablen
        self.reversal_high = None
        self.reversal_low = None
        self.outside_high = None
        self.outside_low = None
        self.previous_bar_low = None
        self.previous_bar_high = None
        self.is_first_inside_bar = False
        self.trailing_stop_long = None
        self.trailing_stop_short = None
        self.is_inside_bar = False

    def setup_logging(self):
        self.logger = logging.getLogger('MarkttechnikStrategy')
        self.logger.setLevel(logging.INFO)
        
        # Datei Handler
        fh = logging.FileHandler('strategy.log')
        fh.setLevel(logging.INFO)
        
        # Console Handler
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        
        # Format
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)
        ch.setFormatter(formatter)
        
        self.logger.addHandler(fh)
        self.logger.addHandler(ch)

    def check_inside_bar(self, df, i):
        is_inside = (df['open'].iloc[i] <= df['high'].iloc[i-1] and 
                    df['open'].iloc[i] >= df['low'].iloc[i-1] and 
                    df['close'].iloc[i] <= df['high'].iloc[i-1] and 
                    df['close'].iloc[i] >= df['low'].iloc[i-1])

        if is_inside and not self.is_first_inside_bar:
            self.is_first_inside_bar = True
            self.is_inside_bar = True
            self.outside_high = df['high'].iloc[i-1]
            self.outside_low = df['low'].iloc[i-1]
            self.previous_bar_high = df['high'].iloc[i-2]
            self.previous_bar_low = df['low'].iloc[i-2]
            
            position_size = self.get_position_size()
            if position_size > 0:
                self.trailing_stop_long = self.previous_bar_low
                self.logger.info(f"Erster Innenstab erkannt - Setze Trailing Stop Long auf {self.trailing_stop_long}")
            elif position_size < 0:
                self.trailing_stop_short = self.previous_bar_high
                self.logger.info(f"Erster Innenstab erkannt - Setze Trailing Stop Short auf {self.trailing_stop_short}")
        
        elif (df['close'].iloc[i] > self.outside_high or 
              df['close'].iloc[i] < self.outside_low or 
              df['low'].iloc[i] < self.previous_bar_low or 
              df['high'].iloc[i] > self.previous_bar_high):
            if self.is_inside_bar:
                self.logger.info("Innenstab-Sequenz beendet - Zurück zu normaler Trailing Stop Logik")
            self.reset_variables()
            //self.is_inside_bar = False
            //self.is_first_inside_bar = False
            //self.outside_high = None
            //self.outside_low = None
            //self.previous_bar_high = None
            //self.previous_bar_low = None
            
    def update_trailing_stop(self, df, i, position_size):
        if not self.is_inside_bar:
            if position_size > 0:
                old_stop = self.trailing_stop_long
                self.trailing_stop_long = df['low'].iloc[i]
                if old_stop != self.trailing_stop_long:
                    self.logger.info(f"Update Trailing Stop Long: {old_stop} -> {self.trailing_stop_long}")
            elif position_size < 0:
                old_stop = self.trailing_stop_short
                self.trailing_stop_short = df['high'].iloc[i]
                if old_stop != self.trailing_stop_short:
                    self.logger.info(f"Update Trailing Stop Short: {old_stop} -> {self.trailing_stop_short}")

    def check_trailing_stop(self, current_price, position_size):
        if position_size > 0 and self.trailing_stop_long is not None:
            if current_price <= self.trailing_stop_long:
                self.logger.info(f"Trailing Stop Long ausgelöst bei {self.trailing_stop_long}")
                self.close_position()
                self.reset_variables()
        elif position_size < 0 and self.trailing_stop_short is not None:
            if current_price >= self.trailing_stop_short:
                self.logger.info(f"Trailing Stop Short ausgelöst bei {self.trailing_stop_short}")
                self.close_position()
                self.reset_variables()
                
    def check_trailing_stop(self, df, i, position_size):
    """
    Prüft beide Stop-Bedingungen bei Innenstäben
    """
    if self.is_inside_bar:
        # Bedingung 1: Bewegung außerhalb des Vorgängerstab-Bereichs
        if (position_size > 0 and df['low'].iloc[i] < self.previous_bar_low) or \
           (position_size < 0 and df['high'].iloc[i] > self.previous_bar_high):
            self.logger.info("Stop ausgelöst - Bewegung außerhalb Vorgängerstab-Bereich")
            self.close_position()
            self.reset_variables()
            
        # Bedingung 2: Schließen außerhalb des Außenstab-Bereichs
        if (position_size > 0 and df['close'].iloc[i] < self.outside_low) or \
           (position_size < 0 and df['close'].iloc[i] > self.outside_high):
            self.logger.info("Stop ausgelöst - Schluss außerhalb Außenstab-Bereich")
            self.close_position()
            self.reset_variables()

         
    def close_position(self):
        self.logger.info("Schließe Position")
        self.api.close_position(self.symbol)

    def reset_variables(self):
        self.logger.info("Reset aller Variablen")
        self.trailing_stop_long = None
        self.trailing_stop_short = None
        self.outside_high = None
        self.outside_low = None
        self.previous_bar_high = None
        self.previous_bar_low = None
        self.is_first_inside_bar = False
        self.is_inside_bar = False

    def run(self):
        self.logger.info("Starte Trading Strategie")
        while True:
            try:
                bars = self.api.get_bars(self.symbol, '1Min', limit=3)
                df = pd.DataFrame([bar.__dict__ for bar in bars])
                
                current_price = float(self.api.get_latest_trade(self.symbol).price)
                position_size = self.get_position_size()
                
                self.logger.debug(f"Aktueller Preis: {current_price}, Position: {position_size}")
                
                if self.is_bullish_reversal(df, -1):
                    self.logger.info("Bullischer Umkehrstab erkannt")
                    self.reversal_high = df['high'].iloc[-1]
                    self.reversal_low = df['low'].iloc[-1]
                elif self.is_bearish_reversal(df, -1):
                    self.logger.info("Bärischer Umkehrstab erkannt")
                    self.reversal_high = df['high'].iloc[-1]
                    self.reversal_low = df['low'].iloc[-1]

                if position_size != 0
                    self.check_inside_bar(df, -1)
                    self.update_trailing_stop(df, -1, position_size)
                    self.check_trailing_stop(current_price, position_size)

                if position_size <= 0 and self.reversal_high is not None:
                    if current_price > self.reversal_high:
                        self.logger.info(f"Long Entry Signal bei {current_price}")
                        if position_size < 0:
                            self.close_position()
                        self.api.submit_order(
                            symbol=self.symbol,
                            qty=1,
                            side='buy',
                            type='market',
                            time_in_force='gtc'
                        )

                elif position_size >= 0 and self.reversal_low is not None:
                    if current_price < self.reversal_low:
                        self.logger.info(f"Short Entry Signal bei {current_price}")
                        if position_size > 0:
                            self.close_position()
                        self.api.submit_order(
                            symbol=self.symbol,
                            qty=1,
                            side='sell',
                            type='market',
                            time_in_force='gtc'
                        )

                time.sleep(60)

            except Exception as e:
                self.logger.error(f"Fehler: {str(e)}")
                time.sleep(60)

if __name__ == "__main__":
    API_KEY = "IHRE_API_KEY"
    API_SECRET = "IHRE_API_SECRET"
    BASE_URL = "https://paper-api.alpaca.markets"
    SYMBOL = "AAPL"

    strategy = MarkttechnikStrategy(API_KEY, API_SECRET, BASE_URL, SYMBOL)
    strategy.run()
