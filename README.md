Was in diesem Code enthalten ist:
Zeithorizont: Der Agent nutzt Stundendaten (1h). Durch n_features=24 blickt er immer genau einen Tag (24 Stunden) zurück, bevor er entscheidet.

Short-Selling: Durch die Berechnung pos * market_ret erzielt der Agent bei action=0 (Short) einen Gewinn, wenn die Kurse fallen.

Risk-Awareness: In der Bellman-Gleichung wird np.std(q_next) abgezogen. Das bestraft Zustände, in denen sich die KI unsicher ist (wenn die Q-Werte für Long und Short weit auseinanderliegen oder beide stark schwanken).

Sicherung: Das System bricht das Training einer Episode sofort ab, wenn ein Drawdown von 15% erreicht wird. Das lehrt die KI, "kontrolliert" zu traden.

Deployment-Ready: Das Modell wird als .h5 gespeichert und die Normalisierungswerte (Scaler) als .pkl. Beides benötigst du für spätere Live-Vorhersagen.
