use log::info;
use pyo3::prelude::*;
use pyo3_log::{Caching, Logger};
use rs::graph_cache::GraphCache;
use rs::mlc_adapter::{run_mlc, run_mlc_with_bags, run_mlc_with_node_and_time, PyLabel};
use rs::osmtools_adapter::{
    add_nearest_node_to_df, download_osm_data, load_osm_cycling, load_osm_driving, load_osm_pois,
    load_osm_walking,
};

mod rs;

#[pyfunction]
fn log_something() {
    println!("Something print");
    info!("Something!");
}

#[pymodule]
fn _mcr_py(_py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    pyo3_log::init();
    let _ = Logger::new(_py, Caching::LoggersAndLevels)?.install();

    m.add_function(wrap_pyfunction!(run_mlc, m)?)?;
    m.add_function(wrap_pyfunction!(log_something, m)?)?;
    m.add_function(wrap_pyfunction!(run_mlc_with_bags, m)?)?;
    m.add_function(wrap_pyfunction!(run_mlc_with_node_and_time, m)?)?;

    //Osmtools mapping
    m.add_function(wrap_pyfunction!(load_osm_cycling, m)?)?;
    m.add_function(wrap_pyfunction!(load_osm_driving, m)?)?;
    m.add_function(wrap_pyfunction!(load_osm_walking, m)?)?;
    m.add_function(wrap_pyfunction!(download_osm_data, m)?)?;
    m.add_function(wrap_pyfunction!(load_osm_pois, m)?)?;
    m.add_function(wrap_pyfunction!(add_nearest_node_to_df, m)?)?;
    m.add_class::<GraphCache>()?;
    m.add_class::<PyLabel>()?;
    Ok(())
}
