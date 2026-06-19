use log::info;
use pyo3::prelude::*;
use pyo3_log::{Caching, Logger};
use rs::mcr_adapter::{
    MCRConfig, MCRGraph, MCRGraphBuilder, PriceFunction, PrivateModeConfig, PublicTransportConfig,
    SharedMicromobileConfig, WalkingConfig, run_mcr5,
};
use rs::osmtools_adapter::{
    add_nearest_node_to_df, download_osm_data, load_osm_boundary, load_osm_cycling,
    load_osm_driving, load_osm_pois, load_osm_walking,
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

    m.add_function(wrap_pyfunction!(log_something, m)?)?;

    // mcr-rust orchestration
    m.add_function(wrap_pyfunction!(run_mcr5, m)?)?;
    m.add_class::<MCRGraph>()?;
    m.add_class::<MCRGraphBuilder>()?;
    m.add_class::<MCRConfig>()?;
    m.add_class::<WalkingConfig>()?;
    m.add_class::<PrivateModeConfig>()?;
    m.add_class::<PublicTransportConfig>()?;
    m.add_class::<SharedMicromobileConfig>()?;
    m.add_class::<PriceFunction>()?;

    // Osmtools mapping
    m.add_function(wrap_pyfunction!(load_osm_cycling, m)?)?;
    m.add_function(wrap_pyfunction!(load_osm_driving, m)?)?;
    m.add_function(wrap_pyfunction!(load_osm_walking, m)?)?;
    m.add_function(wrap_pyfunction!(download_osm_data, m)?)?;
    m.add_function(wrap_pyfunction!(load_osm_pois, m)?)?;
    m.add_function(wrap_pyfunction!(load_osm_boundary, m)?)?;
    m.add_function(wrap_pyfunction!(add_nearest_node_to_df, m)?)?;
    Ok(())
}
