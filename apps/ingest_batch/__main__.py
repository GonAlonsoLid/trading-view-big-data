"""
Batch Ingestion CLI.

Entry point for batch data ingestion operations.

Usage:
    python -m apps.ingest_batch backfill --timeframe 1h --out data_lake
    python -m apps.ingest_batch backfill --timeframe 1d --start 2023-01-01 --end 2023-12-31
    python -m apps.ingest_batch backfill --timeframe all --overwrite
"""

import sys
from datetime import datetime, timezone
from typing import Optional

import click

from packages.utils.logging import setup_logging, get_logger
from pipelines.batch.ohlcv_backfill import run_backfill

logger = get_logger(__name__)


def parse_date(date_str: str) -> datetime:
    """Parse date string to datetime with UTC timezone."""
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        return dt.replace(tzinfo=timezone.utc)
    except ValueError as e:
        raise click.BadParameter(f"Invalid date format: {date_str}. Use YYYY-MM-DD") from e


@click.group()
@click.option(
    "--log-level",
    type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR"], case_sensitive=False),
    default="INFO",
    help="Set logging level",
)
def cli(log_level: str):
    """
    OHLCV Batch Ingestion CLI.
    
    Commands for batch data ingestion from exchanges.
    """
    setup_logging(level=log_level)


@cli.command()
@click.option(
    "--symbol",
    type=str,
    default="BTCUSDT",
    help="Trading pair symbol (default: BTCUSDT)",
)
@click.option(
    "--timeframe",
    type=click.Choice(["1h", "1d", "all"], case_sensitive=False),
    default="1h",
    help="Timeframe to fetch: 1h, 1d, or all (default: 1h)",
)
@click.option(
    "--start",
    type=str,
    default="2022-01-01",
    help="Start date YYYY-MM-DD (default: 2022-01-01)",
)
@click.option(
    "--end",
    type=str,
    default="2025-12-31",
    help="End date YYYY-MM-DD (default: 2025-12-31)",
)
@click.option(
    "--out",
    type=click.Path(),
    default="data_lake",
    help="Output path for data lake (default: data_lake)",
)
@click.option(
    "--max-rows-per-file",
    type=int,
    default=200000,
    help="Maximum rows per Parquet file (default: 200000)",
)
@click.option(
    "--overwrite",
    is_flag=True,
    default=False,
    help="Overwrite existing files (default: False)",
)
@click.option(
    "--no-progress",
    is_flag=True,
    default=False,
    help="Disable progress bar",
)
def backfill(
    symbol: str,
    timeframe: str,
    start: str,
    end: str,
    out: str,
    max_rows_per_file: int,
    overwrite: bool,
    no_progress: bool,
):
    """
    Backfill historical OHLCV data from Binance.
    
    Downloads candlestick data for the specified symbol and timeframe,
    validates, and writes to partitioned Parquet files.
    
    Examples:
    
        # Default: BTCUSDT 1h from 2022-01-01 to 2025-12-31
        
        python -m apps.ingest_batch backfill
        
        # Custom date range
        
        python -m apps.ingest_batch backfill --start 2023-01-01 --end 2023-12-31
        
        # Daily timeframe
        
        python -m apps.ingest_batch backfill --timeframe 1d
        
        # Both timeframes
        
        python -m apps.ingest_batch backfill --timeframe all
    """
    start_date = parse_date(start)
    end_date = parse_date(end)

    if start_date >= end_date:
        raise click.BadParameter("Start date must be before end date")

    click.echo(f"{'='*60}")
    click.echo("OHLCV Batch Backfill")
    click.echo(f"{'='*60}")
    click.echo(f"Symbol:           {symbol}")
    click.echo(f"Timeframe:        {timeframe}")
    click.echo(f"Date range:       {start} to {end}")
    click.echo(f"Output path:      {out}")
    click.echo(f"Max rows/file:    {max_rows_per_file}")
    click.echo(f"Overwrite:        {overwrite}")
    click.echo(f"{'='*60}")

    # Determine timeframes to process
    if timeframe == "all":
        timeframes = ["1h", "1d"]
    else:
        timeframes = [timeframe]

    all_stats = []

    for tf in timeframes:
        click.echo(f"\n>>> Processing timeframe: {tf}")

        try:
            stats = run_backfill(
                symbol=symbol,
                timeframe=tf,
                start_date=start_date,
                end_date=end_date,
                output_path=out,
                max_rows_per_file=max_rows_per_file,
                overwrite=overwrite,
                show_progress=not no_progress,
            )
            all_stats.append(stats)

            # Print summary
            click.echo(f"\n--- Summary for {tf} ---")
            click.echo(f"Total records:    {stats['total_records']}")
            click.echo(f"Valid records:    {stats['valid_records']}")
            click.echo(f"Invalid records:  {stats['invalid_records']}")
            click.echo(f"Files written:    {len(stats['files_written'])}")
            click.echo(f"Duration:         {stats['duration_seconds']:.1f}s")

            if stats["validation_errors"]:
                click.echo("\nValidation warnings:")
                for error in stats["validation_errors"]:
                    click.echo(f"  - {error}")

        except Exception as e:
            logger.exception(f"Error processing timeframe {tf}")
            click.echo(f"\nERROR: {e}", err=True)
            sys.exit(1)

    click.echo(f"\n{'='*60}")
    click.echo("Backfill completed successfully!")
    click.echo(f"{'='*60}")

    # Print file locations
    click.echo("\nOutput files:")
    for stats in all_stats:
        for f in stats["files_written"]:
            click.echo(f"  {f}")


@cli.command()
@click.option(
    "--path",
    type=click.Path(exists=True),
    default="data_lake",
    help="Path to data lake",
)
@click.option(
    "--symbol",
    type=str,
    default="BTCUSDT",
    help="Trading pair symbol",
)
@click.option(
    "--timeframe",
    type=str,
    default="1h",
    help="Timeframe",
)
def info(path: str, symbol: str, timeframe: str):
    """
    Show information about existing data.
    
    Displays statistics about data already stored in the data lake.
    """
    from pathlib import Path
    import pyarrow.parquet as pq

    base_path = Path(path) / "raw" / "ohlcv" / "source=binance" / f"symbol={symbol}" / f"timeframe={timeframe}"

    if not base_path.exists():
        click.echo(f"No data found at {base_path}")
        return

    parquet_files = list(base_path.glob("year=*/month=*/part-*.parquet"))

    if not parquet_files:
        click.echo(f"No Parquet files found at {base_path}")
        return

    click.echo(f"Data path: {base_path}")
    click.echo(f"Files found: {len(parquet_files)}")

    total_rows = 0
    min_time = None
    max_time = None

    for f in parquet_files:
        table = pq.read_table(f, columns=["event_time"])
        total_rows += len(table)

        times = table["event_time"].to_pandas()
        file_min = times.min()
        file_max = times.max()

        if min_time is None or file_min < min_time:
            min_time = file_min
        if max_time is None or file_max > max_time:
            max_time = file_max

    click.echo(f"Total records: {total_rows:,}")
    click.echo(f"Date range: {min_time} to {max_time}")


def main():
    """Main entry point."""
    cli()


if __name__ == "__main__":
    main()

