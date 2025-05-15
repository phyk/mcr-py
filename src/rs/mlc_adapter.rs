use std::collections::{HashMap, HashSet};
use std::hash::Hash;

use log::debug;
use mlc::{
    bag::{Bag, Label},
    mlc::{Bags, MLC},
};
use pyo3::types::PyDict;
use pyo3::{
    prelude::*,
    types::{IntoPyDict, PyList},
};

use super::{
    graph_cache::GraphCache,
    label::{next_bike_tariff, next_bike_without_tariff, personal_car},
};

pub struct PyBags<T: Hash + Eq>(HashMap<T, Bag<T>>);

impl<'py> IntoPyDict<'py> for PyBags<usize> {
    fn into_py_dict(self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let dict = PyDict::new(py);
        for (key, value) in self.0 {
            let py_labels = PyList::empty(py);
            for label in value.labels.iter() {
                let py_label = PyLabel {
                    values: label.values.clone(),
                    hidden_values: label.hidden_values.clone(),
                    path: label.path.clone(),
                    node_id: label.node_id,
                };
                py_labels.append(py_label).unwrap();
            }
            dict.set_item(key, py_labels)?;
        }
        Ok(dict)
    }
}

#[pyclass]
#[derive(Clone)]
pub struct PyLabel {
    #[pyo3(get)]
    pub values: Vec<u64>,
    #[pyo3(get)]
    pub hidden_values: Vec<u64>,
    #[pyo3(get)]
    pub path: Vec<usize>,
    #[pyo3(get)]
    pub node_id: usize,
}

#[pyfunction]
pub fn run_mlc<'py>(
    _py: Python<'py>,
    graph_cache: &GraphCache,
    start_node_id: usize,
) -> Bound<'py, PyDict> {
    let g = graph_cache.graph.as_ref().unwrap();
    let mut mlc = MLC::new(g).unwrap();
    mlc.set_start_node(start_node_id);
    let bags = mlc.run().unwrap();

    PyBags(bags.clone()).into_py_dict(_py).unwrap()
}

#[pyfunction]
#[pyo3(signature = (graph_cache, start_node_id, time, disable_paths=None, update_label_func=None, enable_limit=None))]
pub fn run_mlc_with_node_and_time<'py>(
    _py: Python<'py>,
    graph_cache: &GraphCache,
    start_node_id: usize,
    time: usize,
    disable_paths: Option<bool>,
    update_label_func: Option<String>,
    enable_limit: Option<bool>,
) -> Bound<'py, PyDict> {
    let g = graph_cache.graph.as_ref().unwrap();
    let mut mlc = MLC::new(g).unwrap();
    if let Some(disable_paths) = disable_paths {
        mlc.set_disable_paths(disable_paths);
    }
    let update_label_func = update_label_func.unwrap_or_else(|| "none".to_string());
    let update_label_func = UpdateLabelFunc::from_str(&update_label_func).get_func();
    if let Some(func) = update_label_func {
        mlc.set_update_label_func(func);
    }
    if let Some(enable_limit) = enable_limit {
        mlc.set_enable_limit(enable_limit);
    }
    mlc.set_start_node_with_time(start_node_id, time);
    let bags = mlc.run().unwrap();

    PyBags(bags.clone()).into_py_dict(_py).unwrap()
}

#[derive(Debug)]
enum UpdateLabelFunc {
    NextBikeTariff,
    NextBikeNoTariff,
    PersonalCar,
    None,
}

impl UpdateLabelFunc {
    fn from_str(func_name: &str) -> Self {
        match func_name {
            "next_bike_tariff" => UpdateLabelFunc::NextBikeTariff,
            "next_bike_no_tariff" => UpdateLabelFunc::NextBikeNoTariff,
            "personal_car" => UpdateLabelFunc::PersonalCar,
            "none" => UpdateLabelFunc::None,
            _ => panic!("Unknown update label function: {}", func_name),
        }
    }

    fn get_func(&self) -> Option<fn(&Label<usize>, &Label<usize>) -> Label<usize>> {
        match self {
            UpdateLabelFunc::NextBikeTariff => Some(next_bike_tariff),
            UpdateLabelFunc::NextBikeNoTariff => Some(next_bike_without_tariff),
            UpdateLabelFunc::PersonalCar => Some(personal_car),
            UpdateLabelFunc::None => None,
        }
    }
}

#[pyfunction]
#[pyo3(signature = (graph_cache, bags, update_label_func=None, disable_paths=None, enable_limit=None))]
pub fn run_mlc_with_bags<'py>(
    _py: Python<'py>,
    graph_cache: &GraphCache,
    bags: Bound<'py, PyDict>, //HashMap<usize, Vec<&PyAny>>
    update_label_func: Option<String>,
    disable_paths: Option<bool>,
    enable_limit: Option<bool>,
) -> Bound<'py, PyDict> {
    // convert the PyAny's to Labels
    let mut converted_bags: Bags<usize> = HashMap::new();
    for (node_id, py_labels) in bags.iter() {
        let mut labels = HashSet::new();
        for py_label in  py_labels.try_iter() .unwrap(){
            let py_label_extract = py_label.unwrap();
            let values_result = py_label_extract
                .getattr("values")
                .unwrap() // assuming this unwrap does not panic
                .extract::<Vec<u64>>();

            let values = match values_result {
                Ok(v) => v,
                Err(e) => {
                    panic!("Failed to extract values: {:?} {:?}", e, py_label_extract);
                }
            };
            let hidden_values = py_label_extract
                .getattr("hidden_values")
                .unwrap()
                .extract::<Option<Vec<u64>>>()
                .unwrap();
            let path_result = py_label_extract.getattr("path").unwrap().extract::<Vec<usize>>();
            let path = match path_result {
                Ok(v) => v,
                Err(e) => {
                    panic!("Failed to extract path: {:?} {:?}", e, py_label_extract);
                }
            };

            let node_id = py_label_extract
                .getattr("node_id")
                .unwrap()
                .extract::<usize>()
                .unwrap();

            let label = Label {
                values,
                hidden_values: hidden_values.unwrap_or(vec![]),
                path,
                node_id,
            };
            labels.insert(label);
        }
        converted_bags.insert(node_id.extract::<usize>().unwrap(), Bag { labels });
    }

    let g = graph_cache.graph.as_ref().unwrap();
    let mut mlc = MLC::new(g).unwrap();
    if let Some(enable_limit) = enable_limit {
        mlc.set_enable_limit(enable_limit);
    }
    if let Some(disable_paths) = disable_paths {
        mlc.set_disable_paths(disable_paths);
    }
    let update_label_func = update_label_func.unwrap_or_else(|| "none".to_string());
    let update_label_func = UpdateLabelFunc::from_str(&update_label_func).get_func();
    if let Some(func) = update_label_func {
        mlc.set_update_label_func(func);
    }
    mlc.set_bags(converted_bags);

    let bags: &Bags<usize> = mlc.run().unwrap();

    PyBags(bags.clone()).into_py_dict(_py).unwrap()
}
