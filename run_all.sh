#!/bin/bash

set -u  # fail if an unset variable is used

# --- DB connection (set via env or export above this script) ---
# export PGHOST="stockdata.postgres.database.azure.com"
# export PGDATABASE="postgres"
# export PGUSER="lilbraveh"
# export PGPASSWORD="password"
# export PGPORT="5432"
# export PGSSLMODE="require"

echo "Starting parallel execution of Python scripts..."
echo "========================================="

scripts=(
  "stoch_data.py"
  "adx_data.py"
  "atr_data.py"
  "bbands_data.py"
  "company_overview.py"
  "daily_adjusted_data.py"
  "ema_data.py"
  "income_statments.py"
  "intraday_data.py"
  "macd_data.py"
  "news_sentiment.py"
  "rsi_updater.py"
  "sma_data.py"
  "mfi_data.py"
  "willr_data.py"

)

# Function that keeps rerunning a script until it succeeds
run_script() {
  script=$1
  echo "Starting $script..."
  until python "$script"; do
    echo "❌ $script failed. Retrying in 5 seconds..."
    sleep 5
  done
  echo "✅ $script completed."
}

# Run all scripts in parallel
for script in "${scripts[@]}"; do
  run_script "$script" &
done

wait  # Wait for all background jobs to finish

echo "========================================="
echo "✅ All scripts completed successfully!"

# --- Run SQL refresh step ---
SQL_FILE="refresh_trading_signals.sql"
echo "🔄 Running post-load SQL refresh: $SQL_FILE"

retries=5
attempt=1
until psql -v ON_ERROR_STOP=1 -f "$SQL_FILE"; do
  echo "❌ SQL refresh failed (attempt $attempt/$retries). Retrying in 10s..."
  attempt=$((attempt+1))
  if (( attempt > retries )); then
    echo "⛔ SQL refresh failed after $retries attempts. Exiting 1."
    exit 1
  fi
  sleep 10
done

echo "✅ SQL refresh completed."
echo "🎉 Full pipeline complete."
