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
  "FIN_IND_AROON_WEEKLY_HISTORICAL_ALPHA_ALL_STOCKS.py"
  "FIN_IND_daily_adjusted_PRODUCTION_INCREMENTAL.py"
  "FIN_IND_intraday_PRODUCTION_INCREMENTAL.py"
  "FIN_IND_income_statement_INCREMENTAL_ALL_STOCKS.py"
  "FIN_IND_news_sentiment_INCREMENTAL.py"
  "FIN_IND_ADX_WEEKLY_INCREMENTAL_FROM_DAILY_ALL_STOCKS.py"
  "FIN_IND_ATR_WEEKLY_INCREMENTAL_FROM_DAILY_ALL_STOCKS_v2.py"
  "FIN_IND_AROON_WEEKLY_INCREMENTAL_FROM_DAILY_ALL_STOCKS.py"
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

# Wait for all background jobs to finish
wait

echo "========================================="
echo "✅ All Python scripts completed successfully!"

# --- Run SQL refresh step ---
# This section is currently commented out.
# Uncomment this section when you want to run refresh_trading_signals.sql after Python scripts finish.

# SQL_FILE="refresh_trading_signals.sql"
# echo "🔄 Running post-load SQL refresh: $SQL_FILE"

# retries=5
# attempt=1

# until psql -v ON_ERROR_STOP=1 -f "$SQL_FILE"; do
#   echo "❌ SQL refresh failed (attempt $attempt/$retries). Retrying in 10s..."
#   attempt=$((attempt+1))

#   if (( attempt > retries )); then
#     echo "⛔ SQL refresh failed after $retries attempts. Exiting 1."
#     exit 1
#   fi

#   sleep 10
# done

# echo "✅ SQL refresh completed."

echo "🎉 Python pipeline complete. SQL refresh skipped."
