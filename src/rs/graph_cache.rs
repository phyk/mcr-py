use log::{debug, info};
use mlc::bag::WeightsTuple;
use mlc::read::MLCGraph;
use petgraph::graph::{DiGraph, NodeIndex};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use std::collections::HashMap;
use std::sync::Arc;
use std::usize;

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

    fn set_graph<'py>(&mut self, raw_edges: Bound<'py, PyAny>) {
        debug!("Parsing Graph from {} edges", raw_edges.len().unwrap());
        let graph = parse_graph(raw_edges);
        self.graph = Some(Arc::new(graph));
        debug!("Parsing successfull");
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

fn parse_graph<'py>(raw_edges: Bound<'py, PyAny>) -> MLCGraph<u8> {
    let mut edge_list = Vec::new();
    for py_obj in raw_edges.try_iter().unwrap() {
        let (u, v, weights, hidden_weights) = py_obj
            .unwrap()
            .extract::<(usize, usize, Vec<u64>, Vec<u64>)>()
            .unwrap();

        let weights_tuple = WeightsTuple {
            weights,
            hidden_weights,
        };
        edge_list.push((NodeIndex::new(u), NodeIndex::new(v), weights_tuple));
    }
    DiGraph::<Vec<u8>, WeightsTuple>::from_edges(edge_list)
}
