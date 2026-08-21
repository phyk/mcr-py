import datetime
import pathlib

from mcr_py.gtfs import clean, crop
from mcr_py.gtfs.index import gtfs_to_indexed_parquet
from mcr_py.utils import logger
from mcr_py.utils.geometa import GeoMeta


def prepare_gtfs(
    gtfs_raw_zip: pathlib.Path,
    gtfs_crop_zip: pathlib.Path,
    geometa: GeoMeta,
    gtfs_out_dir: pathlib.Path,
    time_start: datetime.datetime,
    time_end: datetime.datetime,
) -> None:
    """Crop, clean, and write the indexed GTFS parquets for one city."""
    gtfs_crop_zip.parent.mkdir(parents=True, exist_ok=True)
    with logger.Timed.info("Cropping GTFS to city boundary"):
        crop.crop(gtfs_raw_zip, gtfs_crop_zip, geometa, time_start=time_start, time_end=time_end)
    with logger.Timed.info("Cleaning GTFS data"):
        dfs = clean.clean(gtfs_crop_zip)
    with logger.Timed.info("Writing indexed GTFS parquets"):
        gtfs_to_indexed_parquet(dfs, gtfs_out_dir)
