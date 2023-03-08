use std::{
    collections::{HashMap, VecDeque},
    fmt::Debug,
    num::NonZeroU64,
    sync::atomic::AtomicU64,
};

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct NodeId(NonZeroU64);

impl NodeId {
    pub fn new(id: u64) -> Self {
        let id = NonZeroU64::new(id).unwrap();
        NodeId(id)
    }
}

impl Into<u64> for NodeId {
    fn into(self) -> u64 {
        self.0.get()
    }
}

#[derive(Debug)]
pub struct Node<T>
where
    T: Debug + Clone + Send + Sync,
{
    pub id: NodeId,
    pub parent_id: Option<NodeId>,
    pub children_ids: VecDeque<NodeId>,
    pub payload: T,
}

#[derive(Debug)]
pub struct Arena<T>
where
    T: Debug + Clone + Send + Sync,
{
    map: HashMap<NodeId, Node<T>>,
    counter: AtomicU64,
}

impl<T> Arena<T>
where
    T: Debug + Clone + Send + Sync,
{
    pub fn new() -> Self {
        Arena {
            map: HashMap::new(),
            counter: AtomicU64::new(1),
        }
    }

    pub fn add(&mut self, payload: T, parent_id: Option<NodeId>) -> NodeId {
        let id = self.generate_id();

        let node = Node {
            id: id.clone(),
            parent_id: parent_id.clone(),
            children_ids: VecDeque::new(),
            payload,
        };

        self.map.insert(id.clone(), node);

        if let Some(parent_id) = parent_id {
            if let Some(node) = self.map.get_mut(&parent_id) {
                node.children_ids.push_back(id.clone());
            }
        }

        id
    }

    // pub fn contains_children(&self, node_id: &NodeId) -> bool {
    //     if let Some(node) = self.get(node_id) {
    //         return node.children_ids.len() > 0;
    //     }
    //
    //     return false;
    // }

    pub fn contains(&self, node_id: &NodeId) -> bool {
        self.map.contains_key(&node_id)
    }

    pub fn get(&self, node_id: &NodeId) -> Option<&Node<T>> {
        let node = self.map.get(node_id);
        node
    }

    pub fn get_children(&self, parent: &NodeId) -> Option<Vec<&Node<T>>> {
        if !self.contains(&parent) {
            return None;
        }

        if let Some(node) = self.get(parent) {
            let result: Vec<&Node<T>> = node
                .children_ids
                .iter()
                .map(|c| self.get(c))
                .filter_map(|n| n)
                .collect();
            return Some(result);
        }

        None
    }

    fn generate_id(&self) -> NodeId {
        let id = self
            .counter
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        NodeId::new(id)
    }

    /// - [DFS graph walking](https://developerlife.com/2018/08/16/algorithms-in-kotlin-5/)
    /// - [DFS tree walking](https://stephenweiss.dev/algorithms-depth-first-search-dfs#handling-non-binary-trees)
    pub fn tree_walk_dfs(&self, node_id: &NodeId) -> Option<VecDeque<NodeId>> {
        if !self.contains(&node_id) {
            return None;
        }

        let mut stack = VecDeque::from([node_id.clone()]);

        let mut it = VecDeque::new();

        while let Some(node_id) = stack.pop_back() {
            // Question mark operator works below, since it returns a `Option` to `while let ...`.
            // Basically skip to the next item in the `stack` if `node_id` can't be found.
            let node = self.get(&node_id)?;

            it.push_back(node.id.clone());

            // Note that the children ordering has to be flipped! You want to perform the
            // traversal from RIGHT -> LEFT (not LEFT -> RIGHT).
            // PTAL: <https://developerlife.com/assets/algo-ts-2-images/depth-first-search.svg>
            for child_id in node.children_ids.iter().rev() {
                stack.push_back(child_id.clone());
            }
        }

        match it.len() {
            0 => None,
            _ => Some(it),
        }
    }

    pub fn delete_node(&mut self, node_id: NodeId) -> Option<VecDeque<NodeId>> {
        let node = self.get(&node_id)?;
        let parent_id = &node.parent_id.clone();

        let deletion_list = self.tree_walk_dfs(&node_id)?;

        // Note - this lambda expects that `parent_id` exists.
        let mut remove_node_id_from_parent = |parent_id: &NodeId| {
            if let Some(parent_node) = self.map.get_mut(parent_id) {
                parent_node
                    .children_ids
                    .retain(|child_id| *child_id != node_id);
            }
        };

        // If `node_id` has a parent, remove `node_id` its children, otherwise skip this
        // step.
        if let Some(id) = parent_id {
            remove_node_id_from_parent(id);
        }

        // Actually delete the nodes in the deletion list.
        for node_id in &deletion_list {
            self.map.remove(node_id);
        }

        // Pass the deletion list back.
        deletion_list.into()
    }
}
