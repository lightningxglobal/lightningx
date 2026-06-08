#!/usr/bin/env python3
"""Minimal Lightning Exchange swap SDK example (testnet).

Covers the full lifecycle: register/login → place → query position →
reduce-only close → WebSocket subscribe. Stdlib + `requests` +
`websocket-client` only.

    pip install requests websocket-client
    python example.py
"""
import json
import requests
import websocket  # websocket-client

BASE = "http://localhost:4003"
WS = "ws://localhost:4003/ws"


class Client:
    def __init__(self, base=BASE):
        self.base = base
        self.token = None

    def _auth(self):
        return {"Authorization": f"Bearer {self.token}"} if self.token else {}

    def register_login(self, email, password):
        requests.post(f"{self.base}/api/auth/register",
                      json={"email": email, "password": password})
        r = requests.post(f"{self.base}/api/auth/login",
                          json={"email": email, "password": password})
        r.raise_for_status()
        self.token = r.json()["token"]
        return self.token

    def place(self, symbol, side, order_type, quantity,
              price=None, reduce_only=False):
        body = {"symbol": symbol, "side": side, "order_type": order_type,
                "quantity": quantity, "reduce_only": reduce_only}
        if price is not None:
            body["price"] = price
        return requests.post(f"{self.base}/api/orders",
                             headers=self._auth(), json=body).json()

    def positions(self):
        return requests.get(f"{self.base}/api/positions",
                            headers=self._auth()).json()

    def funding(self, symbol):
        return requests.get(f"{self.base}/api/funding",
                            params={"symbol": symbol}).json()


def main():
    c = Client()
    c.register_login("sdk_demo@test", "demo_pw_123456")
    print("placed:", c.place("BTC_USDT", "buy", "limit", 0.01, price=50000))
    print("positions:", c.positions())
    print("funding:", c.funding("BTC_USDT"))
    # reduce-only close
    print("close:", c.place("BTC_USDT", "sell", "market", 0.01, reduce_only=True))

    # WebSocket: subscribe to trades + push, print 5 messages.
    ws = websocket.create_connection(WS)
    ws.send(json.dumps({"type": "subscribe",
                        "channels": ["trades.BTC_USDT", "depth.BTC_USDT"]}))
    for _ in range(5):
        print("ws:", ws.recv())
    ws.close()


if __name__ == "__main__":
    main()
