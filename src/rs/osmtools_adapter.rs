use log::info;
use osmtools::download::download;
use osmtools::extractor::{
    _load_osm_cycling, _load_osm_driving, _load_osm_pois, _load_osm_walking,
};
use osmtools::nearest_node::add_nearest_node_to_geo_df;
use polars::prelude::DataFrame;
use pyo3::prelude::*;
use pyo3_polars::PyDataFrame;

#[pyfunction]
pub fn add_nearest_node_to_df(
    py: Python,
    geo_df: PyDataFrame,
    nodes_to_match: PyDataFrame,
    target_crs: i64,
) -> PyDataFrame {
    let result = py.allow_threads(|| {
        add_nearest_node_to_geo_df(
            geo_df.into(),
            &nodes_to_match.into(),
            target_crs.try_into().unwrap(),
        )
        .unwrap()
    });
    PyDataFrame(result)
}

#[pyfunction]
pub fn download_osm_data(py: Python, city_name: &str, archive_path: &str) -> String {
    py.allow_threads(|| {
        let result =
            download(&city_name.into(), &archive_path.into()).expect("Download process errored");
        return result
            .to_str()
            .expect("Path not convertible to string")
            .into();
    })
}

#[pyfunction]
pub fn load_osm_cycling(
    py: Python,
    city_name: &str,
    geometry_vec: Vec<(f64, f64)>,
    reverse_edges: bool,
    archive_path: &str,
    outpath: &str,
    download: bool,
) -> (PyDataFrame, PyDataFrame) {
    let result = py.allow_threads(|| {
        info!("Loading cycling network from {}", city_name);
        _load_osm_cycling(
            city_name,
            geometry_vec,
            &reverse_edges,
            archive_path,
            outpath,
            download,
        )
    });
    (PyDataFrame(result.0), PyDataFrame(result.1))
}

#[pyfunction]
pub fn load_osm_driving(
    py: Python,
    city_name: &str,
    geometry_vec: Vec<(f64, f64)>,
    archive_path: &str,
    outpath: &str,
    download: bool,
) -> (PyDataFrame, PyDataFrame) {
    let result = py.allow_threads(|| {
        info!("Loading driving network from {}", city_name);
        _load_osm_driving(city_name, geometry_vec, archive_path, outpath, download)
    });
    (PyDataFrame(result.0), PyDataFrame(result.1))
}

#[pyfunction]
pub fn load_osm_walking(
    py: Python,
    city_name: &str,
    geometry_vec: Vec<(f64, f64)>,
    archive_path: &str,
    outpath: &str,
    download: bool,
) -> (PyDataFrame, PyDataFrame) {
    let result = py.allow_threads(|| {
        info!("Loading walking network from {}", city_name);
        _load_osm_walking(city_name, geometry_vec, archive_path, outpath, download)
    });
    (PyDataFrame(result.0), PyDataFrame(result.1))
}

#[pyfunction]
#[pyo3(signature = (city_name, geometry_vec, archive_path, outpath, download, nodes_to_match_df=None, nodes_to_match_path=None))]
pub fn load_osm_pois(
    py: Python,
    city_name: &str,
    geometry_vec: Vec<(f64, f64)>,
    archive_path: &str,
    outpath: &str,
    download: bool,
    nodes_to_match_df: Option<PyDataFrame>,
    nodes_to_match_path: Option<&str>,
) -> PyDataFrame {
    let result = py.allow_threads(|| {
        info!("Loading POIs from {}", city_name);
        let val_df: DataFrame;
        let df: Option<&DataFrame> = match nodes_to_match_df {
            Some(val) => {
                val_df = val.into();
                Some(&val_df)
            }
            None => None,
        };
        _load_osm_pois(
            city_name,
            geometry_vec,
            archive_path,
            nodes_to_match_path,
            df,
            outpath,
            download,
        )
    });
    PyDataFrame(result)
}
