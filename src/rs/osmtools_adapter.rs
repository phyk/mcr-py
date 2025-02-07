use osmtools::extractor::{_load_osm_cycling, _load_osm_driving, _load_osm_walking};
use pyo3::prelude::*;

#[pyfunction]
pub fn load_osm_cycling(
    city_name: &str,
    geometry_vec: Vec<(f64, f64)>,
    archive_path: &str,
    outpath: &str,
    download: bool,
) {
    println!("Starting load of cycling");
    _load_osm_cycling(city_name, geometry_vec, archive_path, outpath, download);
}

#[pyfunction]
pub fn load_osm_driving(
    city_name: &str,
    geometry_vec: Vec<(f64, f64)>,
    archive_path: &str,
    outpath: &str,
    download: bool,
) {
    println!("Starting load of driving");
    _load_osm_driving(city_name, geometry_vec, archive_path, outpath, download);
}

#[pyfunction]
pub fn load_osm_walking(
    city_name: &str,
    geometry_vec: Vec<(f64, f64)>,
    archive_path: &str,
    outpath: &str,
    download: bool,
) {
    println!("Starting load of walking");
    _load_osm_walking(city_name, geometry_vec, archive_path, outpath, download);
}
