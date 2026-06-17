use std::sync::Arc;

use mcr_rust::mcr5::{self, MCRConfig as RustMCRConfig};
use mcr_rust::mlc_interface::{
    private_mode::PrivateModeConfig as RustPrivateModeConfig,
    public_transport::PublicTransportConfig as RustPublicTransportConfig,
    shared_micromobile::{
        PriceFunction as RustPriceFunction, SharedMicromobileConfig as RustSharedMicromobileConfig,
    },
    walking::WalkingConfig as RustWalkingConfig,
};
use mcr_rust::network::{MCRGraph as RustMCRGraph, MCRGraphBuilder as RustMCRGraphBuilder};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

#[pyclass]
#[derive(Clone)]
pub struct WalkingConfig {
    inner: RustWalkingConfig,
}

#[pymethods]
impl WalkingConfig {
    #[new]
    #[pyo3(signature = (speed_kmh, time_only_dominance=false))]
    fn new(speed_kmh: u64, time_only_dominance: bool) -> Self {
        WalkingConfig {
            inner: RustWalkingConfig::new(speed_kmh).with_time_only_dominance(time_only_dominance),
        }
    }
}

#[pyclass]
#[derive(Clone)]
pub struct PrivateModeConfig {
    inner: RustPrivateModeConfig,
}

#[pymethods]
impl PrivateModeConfig {
    #[new]
    fn new(speed_kmh: u64, switch_time_s: u64, price_function_base: u64) -> Self {
        PrivateModeConfig {
            inner: RustPrivateModeConfig::new(speed_kmh, switch_time_s, price_function_base),
        }
    }
}

#[pyclass]
#[derive(Clone)]
pub struct PriceFunction {
    inner: RustPriceFunction,
}

#[pymethods]
impl PriceFunction {
    #[new]
    fn new(
        unlock_fee: u64,
        interval_minutes: u64,
        price_per_interval: u64,
        first_interval_free: bool,
    ) -> Self {
        PriceFunction {
            inner: RustPriceFunction {
                unlock_fee,
                interval_minutes,
                price_per_interval,
                first_interval_free,
            },
        }
    }
}

#[pyclass]
#[derive(Clone)]
pub struct SharedMicromobileConfig {
    inner: RustSharedMicromobileConfig,
}

#[pymethods]
impl SharedMicromobileConfig {
    #[new]
    fn new(speed_kmh: u64, switch_time_s: u64, price_function: PriceFunction) -> Self {
        SharedMicromobileConfig {
            inner: RustSharedMicromobileConfig::new(
                speed_kmh,
                switch_time_s,
                price_function.inner.clone(),
            ),
        }
    }
}

#[pyclass]
#[derive(Clone)]
pub struct PublicTransportConfig {
    inner: RustPublicTransportConfig,
}

#[pymethods]
impl PublicTransportConfig {
    #[new]
    #[pyo3(signature = (
        data_dir,
        max_snap_distance_m=20.0,
        anchor_time_secs=8 * 3600,
        flex_window_secs=600,
        short_trip_fare_cents=220,
        short_trip_max_stops=4,
        long_trip_fare_cents=320,
    ))]
    fn new(
        data_dir: String,
        max_snap_distance_m: f64,
        anchor_time_secs: u32,
        flex_window_secs: i32,
        short_trip_fare_cents: u64,
        short_trip_max_stops: usize,
        long_trip_fare_cents: u64,
    ) -> Self {
        PublicTransportConfig {
            inner: RustPublicTransportConfig {
                data_dir,
                max_snap_distance_m,
                anchor_time_secs,
                flex_window_secs,
                short_trip_fare_cents,
                short_trip_max_stops,
                long_trip_fare_cents,
            },
        }
    }
}

#[pyclass]
#[derive(Clone, Default)]
pub struct MCRConfig {
    enable_limit: bool,
    walking: Option<WalkingConfig>,
    cycling: Option<PrivateModeConfig>,
    car: Option<PrivateModeConfig>,
    shared_micromobile: Option<Vec<SharedMicromobileConfig>>,
    public_transport: Option<PublicTransportConfig>,
    out_dir: String,
}

#[pymethods]
impl MCRConfig {
    #[new]
    #[pyo3(signature = (
        out_dir,
        walking=None,
        cycling=None,
        car=None,
        shared_micromobile=None,
        public_transport=None,
        enable_limit=false,
    ))]
    fn new(
        out_dir: String,
        walking: Option<WalkingConfig>,
        cycling: Option<PrivateModeConfig>,
        car: Option<PrivateModeConfig>,
        shared_micromobile: Option<Vec<SharedMicromobileConfig>>,
        public_transport: Option<PublicTransportConfig>,
        enable_limit: bool,
    ) -> Self {
        MCRConfig {
            enable_limit,
            walking,
            cycling,
            car,
            shared_micromobile,
            public_transport,
            out_dir,
        }
    }
}

impl MCRConfig {
    fn to_rust(&self) -> RustMCRConfig {
        RustMCRConfig {
            enable_limit: self.enable_limit,
            walking: self.walking.as_ref().map(|w| w.inner.clone()),
            cycling: self.cycling.as_ref().map(|c| c.inner.clone()),
            car: self.car.as_ref().map(|c| c.inner.clone()),
            shared_micromobile: self
                .shared_micromobile
                .as_ref()
                .map(|v| v.iter().map(|s| s.inner.clone()).collect()),
            public_transport: self.public_transport.as_ref().map(|p| p.inner.clone()),
            out_dir: self.out_dir.clone(),
        }
    }
}

#[pyclass]
pub struct MCRGraph {
    inner: Arc<RustMCRGraph>,
}

#[pymethods]
impl MCRGraph {
    fn node_count(&self) -> usize {
        self.inner.external_to_internal.len()
    }
}

#[pyclass]
pub struct MCRGraphBuilder {
    inner: Option<RustMCRGraphBuilder>,
}

#[pymethods]
impl MCRGraphBuilder {
    #[new]
    fn new() -> Self {
        MCRGraphBuilder {
            inner: Some(RustMCRGraphBuilder::new()),
        }
    }

    fn add_walking_nodes(mut slf: PyRefMut<Self>, path: String) -> PyRefMut<Self> {
        slf.inner = Some(slf.inner.take().unwrap().add_walking_nodes(path));
        slf
    }

    fn add_walking_edges(mut slf: PyRefMut<Self>, path: String) -> PyRefMut<Self> {
        slf.inner = Some(slf.inner.take().unwrap().add_walking_edges(path));
        slf
    }

    fn add_cycling_nodes(mut slf: PyRefMut<Self>, path: String) -> PyRefMut<Self> {
        slf.inner = Some(slf.inner.take().unwrap().add_cycling_nodes(path));
        slf
    }

    fn add_cycling_edges(mut slf: PyRefMut<Self>, path: String) -> PyRefMut<Self> {
        slf.inner = Some(slf.inner.take().unwrap().add_cycling_edges(path));
        slf
    }

    fn add_car_nodes(mut slf: PyRefMut<Self>, path: String) -> PyRefMut<Self> {
        slf.inner = Some(slf.inner.take().unwrap().add_car_nodes(path));
        slf
    }

    fn add_car_edges(mut slf: PyRefMut<Self>, path: String) -> PyRefMut<Self> {
        slf.inner = Some(slf.inner.take().unwrap().add_car_edges(path));
        slf
    }

    fn add_poi_nodes(mut slf: PyRefMut<Self>, path: String) -> PyRefMut<Self> {
        slf.inner = Some(slf.inner.take().unwrap().add_poi_nodes(path));
        slf
    }

    fn add_shared_bike_stations(mut slf: PyRefMut<Self>, path: String) -> PyRefMut<Self> {
        slf.inner = Some(slf.inner.take().unwrap().add_shared_bike_stations(path));
        slf
    }

    fn add_shared_bike_dropoff_zones(mut slf: PyRefMut<Self>, path: String) -> PyRefMut<Self> {
        slf.inner = Some(slf.inner.take().unwrap().add_shared_bike_dropoff_zones(path));
        slf
    }

    fn add_shared_scooter_stations(mut slf: PyRefMut<Self>, path: String) -> PyRefMut<Self> {
        slf.inner = Some(slf.inner.take().unwrap().add_shared_scooter_stations(path));
        slf
    }

    fn add_shared_scooter_dropoff_zones(mut slf: PyRefMut<Self>, path: String) -> PyRefMut<Self> {
        slf.inner = Some(
            slf.inner
                .take()
                .unwrap()
                .add_shared_scooter_dropoff_zones(path),
        );
        slf
    }

    fn build(&mut self) -> PyResult<MCRGraph> {
        let builder = self
            .inner
            .take()
            .ok_or_else(|| PyRuntimeError::new_err("MCRGraphBuilder already built"))?;
        let graph = builder
            .build()
            .map_err(|e| PyRuntimeError::new_err(format!("MCRGraph build failed: {e}")))?;
        Ok(MCRGraph {
            inner: Arc::new(graph),
        })
    }
}

#[pyfunction]
pub fn run_mcr5(
    py: Python<'_>,
    start_nodes: String,
    config: MCRConfig,
    graph: &MCRGraph,
) -> PyResult<()> {
    let rust_config = config.to_rust();
    let graph_arc = Arc::clone(&graph.inner);
    py.allow_threads(|| {
        mcr5::run_mcr5(start_nodes, rust_config, graph_arc)
            .map_err(|e| PyRuntimeError::new_err(format!("run_mcr5 failed: {e}")))
    })
}
