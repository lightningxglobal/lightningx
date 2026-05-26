-- Composite index for 24h ticker queries: WHERE symbol=$1 AND created_at > NOW() - INTERVAL '24 hours'
-- The separate idx_trades_symbol and idx_trades_created indexes force a bitmap AND;
-- this composite index makes the filter a single range scan.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_trades_symbol_created
    ON trades(symbol, created_at DESC);
