# -*- coding: utf-8 -*-

"""Watch websocket tickers across multiple exchanges and display cross-exchange spreads."""

import asyncio
import ccxt.pro as ccxtpro


async def watch_ticker(exchange_id, symbol, queue):
    exchange = getattr(ccxtpro, exchange_id)()
    await exchange.load_markets()
    print('Starting', exchange.id, 'for', symbol)
    try:
        while True:
            ticker = await exchange.watch_ticker(symbol)
            await queue.put((exchange.id, ticker))
    except asyncio.CancelledError:
        raise
    except Exception as e:
        print(exchange.id, type(e).__name__, str(e))
    finally:
        await exchange.close()
        await queue.put((exchange.id, None))


def best_bid_ask(latest):
    bids = [(exchange_id, ticker) for exchange_id, ticker in latest.items() if ticker.get('bid') is not None]
    asks = [(exchange_id, ticker) for exchange_id, ticker in latest.items() if ticker.get('ask') is not None]

    if not bids or not asks:
        return None, None, None, None

    bid_exchange, bid_ticker = max(bids, key=lambda item: item[1]['bid'])
    ask_exchange, ask_ticker = min(asks, key=lambda item: item[1]['ask'])
    return bid_exchange, bid_ticker, ask_exchange, ask_ticker


def print_spread(bid_exchange, bid_ticker, ask_exchange, ask_ticker):
    spread = bid_ticker['bid'] - ask_ticker['ask']
    spread_pct = spread / ask_ticker['ask'] * 100 if ask_ticker['ask'] else None
    timestamp = bid_ticker.get('datetime') or ask_ticker.get('datetime')
    print(
        timestamp,
        bid_ticker['symbol'],
        'best bid',
        bid_exchange,
        bid_ticker['bid'],
        'best ask',
        ask_exchange,
        ask_ticker['ask'],
        'spread',
        spread,
        'spread %',
        spread_pct,
    )


async def log_spreads(queue, exchange_ids):
    latest = {}
    finished = set()

    while len(finished) < len(exchange_ids):
        exchange_id, ticker = await queue.get()
        if ticker is None:
            finished.add(exchange_id)
            continue

        latest[exchange_id] = ticker
        bid_exchange, bid_ticker, ask_exchange, ask_ticker = best_bid_ask(latest)
        if bid_ticker is None or ask_ticker is None:
            continue

        print_spread(bid_exchange, bid_ticker, ask_exchange, ask_ticker)


async def main():
    symbols = {
        'binance': 'BTC/USDT',
        'okx': 'BTC/USDT',
        'bybit': 'BTC/USDT',
    }

    queue = asyncio.Queue()
    watchers = [asyncio.create_task(watch_ticker(exchange_id, symbol, queue)) for exchange_id, symbol in symbols.items()]
    logger = asyncio.create_task(log_spreads(queue, list(symbols.keys())))
    tasks = watchers + [logger]

    try:
        await asyncio.gather(*tasks)
    except KeyboardInterrupt:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


if __name__ == '__main__':
    asyncio.run(main())
