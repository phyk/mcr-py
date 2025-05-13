use log::info;
use mlc::bag::{Weight, WeightsTuple};
use mlc::read::MLCGraph;
use petgraph::graph::DiGraph;
use petgraph::{graph::NodeIndex, Directed, Graph};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use std::collections::HashMap;
use std::sync::Arc;
use std::usize;
use log::debug;

#[pyclass]
pub struct GraphCache {
    pub graph: Option<Arc<MLCGraph<u8>>>,
}

#[pymethods]
impl GraphCache {
    #[new]
    fn new() -> Self {
        GraphCache { graph: None }
    }

    fn set_graph<'py>(&mut self, py: Python, raw_edges: Bound<'py, PyAny>) {
        debug!("Parsing Graph from {} edges", raw_edges.len().unwrap());
        let graph = parse_graph(py, raw_edges);
        debug!("Parsing successfull");
        self.graph = Some(Arc::new(graph));
    }

    fn set_node_weights(&mut self, node_weights: HashMap<usize, Vec<u8>>) {
        let arc_graph = self
            .graph
            .as_ref()
            .expect("Graph must be set before modifying node weights");
        let cloned_arc_graph = Arc::clone(arc_graph);
        let mut new_graph = (*cloned_arc_graph).clone();

        for (node_id, weight) in node_weights {
            let current_weight = new_graph
                .node_weight_mut(NodeIndex::new(node_id))
                .expect(format!("Node id {} is not in graph", node_id).as_str());
            *current_weight = weight;
        }

        self.graph = Some(Arc::new(new_graph));
    }


    fn summary(&self) -> PyResult<()> {
        if let Some(graph) = &self.graph {
            info!("Nodes: {}", graph.node_count());
            info!("Edges: {}", graph.edge_count());
            Ok(())
        } else {
            Err(PyValueError::new_err("Graph not set"))
        }
    }

    fn validate_node_id(&self, node_id: usize) -> PyResult<()> {
        if let Some(graph) = &self.graph {
            if node_id < graph.node_count() {
                Ok(())
            } else {
                Err(PyValueError::new_err(format!(
                    "Node id {} is not in graph",
                    node_id
                )))
            }
        } else {
            Err(PyValueError::new_err("Graph not set"))
        }
    }

    fn get_edge_weights(&self, start_node_id: usize, end_node_id: usize) -> PyResult<Vec<u64>> {
        if let Some(graph) = &self.graph {
            let edge = graph
                .find_edge(NodeIndex::new(start_node_id), NodeIndex::new(end_node_id))
                .ok_or_else(|| {
                    PyValueError::new_err(format!(
                        "Edge ({}, {}) not found",
                        start_node_id, end_node_id
                    ))
                })?;
            let weights = graph.edge_weight(edge).unwrap().weights.clone();
            Ok(weights)
        } else {
            Err(PyValueError::new_err("Graph not set"))
        }
    }
}



fn parse_graph<'py>(py: Python, raw_edges: Bound<'py, PyAny>) -> MLCGraph<u8> {
    let mut edge_list = Vec::new();
    for py_obj in raw_edges.try_iter().unwrap() {
        let (u, v, weights_, hidden_weights_) = py_obj.unwrap().extract::<(usize, usize, String, String)>().unwrap();
        let weights: Vec<Weight> = parse_weights(&weights_
        )
        .unwrap();

        let hidden_weights: Vec<Weight> = parse_weights(&hidden_weights_).unwrap();
        let weights_tuple = WeightsTuple {
            weights,
            hidden_weights,
        };
        edge_list.push((NodeIndex::new(u), NodeIndex::new(v), weights_tuple));
    }
    debug!("Filled List, has now {} edges", edge_list.len());
    py.allow_threads(|| {
        DiGraph::<Vec<u8>, WeightsTuple>::from_edges(edge_list)
    })
}

fn parse_weights<'py>(raw_weights: &String) -> Result<Vec<u64>, String> {
    // remove first and last character (brackets)
    let raw_weights = &raw_weights[1..raw_weights.len() - 1];
    let weights: Result<Vec<u64>, _> = raw_weights
        .split(",")
        .map(|x| {
            x.parse::<u64>()
                .map_err(|_| format!("Failed to parse weight: {}", x))
        })
        .collect();

    weights.map_err(|e| e.to_string())
}
