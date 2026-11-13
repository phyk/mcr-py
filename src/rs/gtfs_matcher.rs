//! Matches GTFS stops to walking-network nodes.
//!
//! Runs in the data-load flow (not at routing time): the result is written to
//! `gtfs_clean/<city>/stop_nodes.parquet` and read back by mcr-rust's
//! `TransitData::load`, so the router and the visualization share one mapping.

use std::collections::HashMap;

use polars::prelude::*;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3_polars::PyDataFrame;

const EARTH_RADIUS_M: f64 = 6_371_000.0;
const METERS_PER_DEG_LAT: f64 = 111_320.0;

pub fn haversine_m(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    let dlat = (lat2 - lat1).to_radians();
    let dlon = (lon2 - lon1).to_radians();
    let a = (dlat / 2.0).sin().powi(2)
        + lat1.to_radians().cos() * lat2.to_radians().cos() * (dlon / 2.0).sin().powi(2);
    2.0 * EARTH_RADIUS_M * a.sqrt().asin()
}

/// A stop matched to its nearest node.
#[derive(Debug, PartialEq)]
pub struct StopMatch {
    pub stop_idx: u32,
    pub node_id: u64,
    pub dist_m: f64,
}

/// Uniform lat/lon grid with cell size >= `max_m`, so the nearest node within
/// `max_m` of a point is always in the point's cell or one of its 8 neighbours.
struct NodeGrid<'a> {
    nodes: &'a [(u64, f64, f64)],
    cell_deg_lat: f64,
    cell_deg_lon: f64,
    cells: HashMap<(i64, i64), Vec<usize>>,
}

impl<'a> NodeGrid<'a> {
    fn new(nodes: &'a [(u64, f64, f64)], max_m: f64, ref_lat: f64) -> Self {
        // Small safety factor keeps the 3x3 neighbourhood sufficient despite the
        // flat-earth cell size vs. haversine distance mismatch.
        let cell_m = (max_m * 1.1).max(1.0);
        let cell_deg_lat = cell_m / METERS_PER_DEG_LAT;
        let cell_deg_lon = cell_m / (METERS_PER_DEG_LAT * ref_lat.to_radians().cos().max(0.01));
        let mut grid = NodeGrid {
            nodes,
            cell_deg_lat,
            cell_deg_lon,
            cells: HashMap::new(),
        };
        for (i, &(_, lat, lon)) in nodes.iter().enumerate() {
            let key = grid.cell(lat, lon);
            grid.cells.entry(key).or_default().push(i);
        }
        grid
    }

    fn cell(&self, lat: f64, lon: f64) -> (i64, i64) {
        (
            (lat / self.cell_deg_lat).floor() as i64,
            (lon / self.cell_deg_lon).floor() as i64,
        )
    }

    /// Nearest node within `max_m` (first in input order wins ties).
    fn nearest(&self, lat: f64, lon: f64, max_m: f64) -> Option<(u64, f64)> {
        let (ci, cj) = self.cell(lat, lon);
        let mut best: Option<(usize, f64)> = None;
        for di in -1..=1 {
            for dj in -1..=1 {
                let Some(bucket) = self.cells.get(&(ci + di, cj + dj)) else {
                    continue;
                };
                for &i in bucket {
                    let (_, nlat, nlon) = self.nodes[i];
                    let d = haversine_m(lat, lon, nlat, nlon);
                    let better = match best {
                        None => true,
                        Some((bi, bd)) => d < bd || (d == bd && i < bi),
                    };
                    if better {
                        best = Some((i, d));
                    }
                }
            }
        }
        best.filter(|&(_, d)| d <= max_m)
            .map(|(i, d)| (self.nodes[i].0, d))
    }
}

/// Snap every stop to its nearest node within `max_m` metres; stops with no node
/// in range are omitted.
pub fn match_stops(
    stops: &[(u32, f64, f64)],
    nodes: &[(u64, f64, f64)],
    max_m: f64,
) -> Vec<StopMatch> {
    if nodes.is_empty() || stops.is_empty() {
        return vec![];
    }
    let ref_lat = nodes.iter().map(|n| n.1).sum::<f64>() / nodes.len() as f64;
    let grid = NodeGrid::new(nodes, max_m, ref_lat);
    stops
        .iter()
        .filter_map(|&(stop_idx, lat, lon)| {
            grid.nearest(lat, lon, max_m)
                .map(|(node_id, dist_m)| StopMatch {
                    stop_idx,
                    node_id,
                    dist_m,
                })
        })
        .collect()
}

fn column_err(e: PolarsError) -> PyErr {
    PyValueError::new_err(e.to_string())
}

/// `stops`: `stop_idx` (u32), `stop_lat`, `stop_lon` (f64).
/// `walking_nodes`: `osm_id` (u64), `lat`, `long` (f64) — the walking network only,
/// so stops never snap to POI-only / cycling-only / car-only nodes.
/// Returns `stop_idx` (u32), `osm_id` (u64), `snap_dist_m` (f64), one row per matched stop.
#[pyfunction]
pub fn match_gtfs_stops_to_nodes(
    py: Python,
    stops: PyDataFrame,
    walking_nodes: PyDataFrame,
    max_snap_distance_m: f64,
) -> PyResult<PyDataFrame> {
    let stops: DataFrame = stops.into();
    let walking_nodes: DataFrame = walking_nodes.into();

    let stop_idx = stops.column("stop_idx").map_err(column_err)?.u32().map_err(column_err)?;
    let stop_lat = stops.column("stop_lat").map_err(column_err)?.f64().map_err(column_err)?;
    let stop_lon = stops.column("stop_lon").map_err(column_err)?.f64().map_err(column_err)?;
    let stop_rows: Vec<(u32, f64, f64)> = stop_idx
        .into_iter()
        .zip(stop_lat)
        .zip(stop_lon)
        .filter_map(|((i, lat), lon)| Some((i?, lat?, lon?)))
        .collect();

    let osm_id = walking_nodes.column("osm_id").map_err(column_err)?.u64().map_err(column_err)?;
    let lat = walking_nodes.column("lat").map_err(column_err)?.f64().map_err(column_err)?;
    let long = walking_nodes.column("long").map_err(column_err)?.f64().map_err(column_err)?;
    let node_rows: Vec<(u64, f64, f64)> = osm_id
        .into_iter()
        .zip(lat)
        .zip(long)
        .filter_map(|((i, lat), lon)| Some((i?, lat?, lon?)))
        .collect();

    let matches = py.allow_threads(|| match_stops(&stop_rows, &node_rows, max_snap_distance_m));

    let out = df!(
        "stop_idx" => matches.iter().map(|m| m.stop_idx).collect::<Vec<_>>(),
        "osm_id" => matches.iter().map(|m| m.node_id).collect::<Vec<_>>(),
        "snap_dist_m" => matches.iter().map(|m| m.dist_m).collect::<Vec<_>>(),
    )
    .map_err(column_err)?;
    Ok(PyDataFrame(out))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn haversine_known_distance() {
        // Brussels to Bruges is ~88 km.
        let d = haversine_m(50.8503, 4.3517, 51.2093, 3.2247);
        assert!((d - 88_000.0).abs() < 3_000.0, "got {d:.0}m");
    }

    #[test]
    fn picks_nearest_node() {
        let nodes = vec![(10, 51.0, 3.0), (11, 51.01, 3.01), (12, 51.1, 3.1)];
        let m = match_stops(&[(0, 51.0099, 3.0099)], &nodes, 500.0);
        assert_eq!(m.len(), 1);
        assert_eq!(m[0].node_id, 11);
        assert!(m[0].dist_m < 20.0);
    }

    #[test]
    fn drops_stops_beyond_max_distance() {
        let nodes = vec![(10, 51.0, 3.0)];
        assert!(match_stops(&[(0, 52.0, 4.0)], &nodes, 300.0).is_empty());
        // ~35 m north of the node: outside 20 m, inside 50 m.
        let stop = [(0, 51.000315, 3.0)];
        assert!(match_stops(&stop, &nodes, 20.0).is_empty());
        assert_eq!(match_stops(&stop, &nodes, 50.0).len(), 1);
    }

    #[test]
    fn matches_across_grid_cell_borders() {
        // Nodes a few metres apart on either side of a cell boundary.
        let nodes: Vec<_> = (0..200)
            .map(|i| (i as u64, 51.0 + i as f64 * 0.00005, 3.0))
            .collect();
        for (idx, &(id, lat, lon)) in nodes.iter().enumerate() {
            let m = match_stops(&[(idx as u32, lat + 0.000001, lon)], &nodes, 20.0);
            assert_eq!(m[0].node_id, id);
        }
    }

    #[test]
    fn grid_agrees_with_brute_force() {
        let nodes: Vec<_> = (0..500)
            .map(|i| {
                let f = i as f64;
                (i as u64, 50.9 + (f * 0.37).sin() * 0.01, 6.9 + (f * 0.91).cos() * 0.01)
            })
            .collect();
        let stops: Vec<_> = (0..100)
            .map(|i| {
                let f = i as f64;
                (i as u32, 50.9 + (f * 0.53).sin() * 0.01, 6.9 + (f * 0.29).cos() * 0.01)
            })
            .collect();
        let fast = match_stops(&stops, &nodes, 60.0);
        for &(sidx, lat, lon) in &stops {
            let brute = nodes
                .iter()
                .map(|&(id, nlat, nlon)| (id, haversine_m(lat, lon, nlat, nlon)))
                .fold(None::<(u64, f64)>, |b, c| match b {
                    Some(x) if x.1 <= c.1 => Some(x),
                    _ => Some(c),
                })
                .filter(|&(_, d)| d <= 60.0);
            let got = fast.iter().find(|m| m.stop_idx == sidx).map(|m| m.node_id);
            assert_eq!(got, brute.map(|b| b.0), "stop {sidx}");
        }
    }
}
