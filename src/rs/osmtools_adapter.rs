use osmtools::extractor::{_load_osm_cycling, _load_osm_driving, _load_osm_walking};
use osmtools::download::download;
use pyo3::prelude::*;

#[pyfunction]
pub fn download_osm_data(py: Python, city_name: &str, archive_path: &str) -> String {
    py.allow_threads(|| {
        let result = download(&city_name.into(), &archive_path.into()).expect("Download process errored");
        return result.to_str().expect("Path not convertible to string").into();
    })
}


#[pyfunction]
pub fn load_osm_cycling(
    py: Python,
    city_name: &str,
    geometry_vec: Vec<(f64, f64)>,
    archive_path: &str,
    outpath: &str,
    download: bool,
) {
    py.allow_threads(|| {
        _load_osm_cycling(city_name, geometry_vec, archive_path, outpath, download);
    })
}

#[pyfunction]
pub fn load_osm_driving(
    py: Python,
    city_name: &str,
    geometry_vec: Vec<(f64, f64)>,
    archive_path: &str,
    outpath: &str,
    download: bool,
) {
    py.allow_threads(|| {
        _load_osm_driving(city_name, geometry_vec, archive_path, outpath, download);
    })
}

#[pyfunction]
pub fn load_osm_walking(
    py: Python,
    city_name: &str,
    geometry_vec: Vec<(f64, f64)>,
    archive_path: &str,
    outpath: &str,
    download: bool,
) {
    py.allow_threads(|| {
        _load_osm_walking(city_name, geometry_vec, archive_path, outpath, download);
    })
}
