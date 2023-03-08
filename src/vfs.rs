use std::{collections::HashMap, fmt::Debug, time::SystemTime};

use fuser::{FileAttr, FileType};
use kube::{
    core::DynamicObject,
    discovery::{verbs, ApiCapabilities, ApiResource, Scope},
    ResourceExt,
};

use crate::client::KubeClient;
use crate::tree::{Arena, Node, NodeId};

#[derive(Debug, Clone)]
pub enum KubeManifestType {
    Json,
    Yaml,
}

#[derive(Debug, Clone)]
pub struct KubeApiResourceDirectory {
    pub name: String,
    pub namespace: String,
    pub alias: Option<String>,
    pub api: ApiResource,
}

impl KubeApiResourceDirectory {
    pub fn get_name(&self) -> String {
        self.alias.clone().unwrap_or(self.name.clone())
    }
}

#[derive(Debug, Clone)]
pub struct KubeManifestFile {
    pub name: String,
    pub file_type: KubeManifestType,
    pub data: DynamicObject,
}

impl KubeManifestFile {
    // TODO: Let's not serialize each time I need lookup the size
    pub fn get_size(&self) -> u64 {
        self.to_string().len() as u64
    }

    pub fn to_string(&self) -> String {
        let mut obj = self.data.clone();
        obj.metadata.managed_fields = None;
        match self.file_type {
            KubeManifestType::Yaml => serde_yaml::to_string(&obj).unwrap_or_default(),
            KubeManifestType::Json => serde_json::to_string(&obj).unwrap_or_default(),
        }
    }
}

#[derive(Debug, Clone)]
struct KubeApiResourceNode {
    pub namespace: Option<String>,
    pub group: String,
    pub version: String,
    /// Singular PascalCase name of the resource
    pub kind: String,
    /// Plural name of the resource
    pub plural: String,
}

impl KubeApiResourceNode {
    fn name(&self) -> String {
        if !self.plural.is_empty() {
            return self.plural.clone();
        } else {
            return self.kind.clone();
        }
    }
}

#[derive(Debug, Clone)]
struct KubeResourceNode {
    namespace: Option<String>,
    uuid: String,
    name: String,
    kind: String,
}

impl KubeResourceNode {
    fn new(uuid: &str, name: &str, kind: &str) -> Self {
        KubeResourceNode {
            namespace: None,
            uuid: uuid.into(),
            name: name.into(),
            kind: kind.into(),
        }
    }

    fn from(obj: &DynamicObject, kind: &str) -> Self {
        KubeResourceNode {
            namespace: obj.namespace(),
            uuid: obj.uid().unwrap(),
            name: obj.name_any(),
            kind: kind.into(),
        }
    }
}

#[derive(Debug, Clone)]
enum KubeFileNode {
    Virtual(String),
    Context(String),
    ClusterInfoFile,
    ApiResourceDirectory(KubeApiResourceNode),
    ResourceDirectory(KubeResourceNode),
    ResourceFile(KubeResourceNode),
    LogFile(KubeResourceNode),
}

impl KubeFileNode {
    pub fn get_file_name(&self) -> String {
        match self {
            KubeFileNode::Context(name) | KubeFileNode::Virtual(name) => name.clone(),
            KubeFileNode::ClusterInfoFile => "cluster_info".into(),
            KubeFileNode::ApiResourceDirectory(api) => api.name(),
            KubeFileNode::ResourceDirectory(r) => r.name.clone(),
            KubeFileNode::ResourceFile(r) => format!("{}.yml", r.name),
            KubeFileNode::LogFile(_) => "logs".into(),
        }
    }
}

impl PartialEq<KubeFileNode> for KubeFileNode {
    fn eq(&self, other: &KubeFileNode) -> bool {
        let this = self;
        match this {
            KubeFileNode::Virtual(l) => match other {
                KubeFileNode::Virtual(r) => l == r,
                _ => false,
            },
            KubeFileNode::Context(l) => match other {
                KubeFileNode::Context(r) => l == r,
                _ => false,
            },
            KubeFileNode::ClusterInfoFile => match other {
                KubeFileNode::ClusterInfoFile => true,
                _ => false,
            },
            KubeFileNode::ApiResourceDirectory(l) => match other {
                KubeFileNode::ApiResourceDirectory(r) => {
                    l.kind == r.kind && l.group == r.group && l.version == r.version
                }
                _ => false,
            },
            KubeFileNode::ResourceDirectory(l) => match other {
                KubeFileNode::ResourceDirectory(r) => l.uuid == r.uuid,
                _ => false,
            },
            KubeFileNode::ResourceFile(l) => match other {
                KubeFileNode::ResourceFile(r) => l.uuid == r.uuid,
                _ => false,
            },
            KubeFileNode::LogFile(l) => match other {
                KubeFileNode::LogFile(r) => l.uuid == r.uuid,
                _ => false,
            },
        }
    }
}

pub struct KubeVirtualFs {
    kube_client: KubeClient,
    aliases: HashMap<String, String>,
    api_resources: Vec<(ApiResource, ApiCapabilities)>,
    arena_two: Arena<KubeFileNode>,
    startup: SystemTime,
}

impl KubeVirtualFs {
    pub fn new(kube_client: KubeClient) -> Self {
        let mut arena_two = Arena::new();
        arena_two.add(KubeFileNode::Context("default".into()), None);

        let aliases = HashMap::from([
            ("service".into(), "svc".into()),
            ("deployment".into(), "deploy".into()),
        ]);

        let filter = vec![
            "",
            "apps",
            "batch",
            "networking.k8s.io",
            "rbac.authorization.k8s.iol",
        ];
        let ops = vec![verbs::LIST];

        let api_resources = kube_client
            .discover_api_resources(Some(&filter), Some(&ops))
            .unwrap();

        KubeVirtualFs {
            kube_client,
            aliases,
            arena_two,
            api_resources,
            startup: SystemTime::now(),
        }
    }

    pub fn get_file_from_parent_by_name_two(
        &mut self,
        parent: u64,
        name: &str,
    ) -> Option<(String, FileAttr)> {
        self.sync_leafs_for_inode(parent);

        let id = NodeId::new(parent);

        println!(
            "Found {} for {} and it is {:?}",
            name,
            parent,
            self.arena_two
                .get_children(&id)
                .map(|nodes| {
                    nodes
                        .iter()
                        .map(|n| (n.payload.get_file_name(), self.map_kube_file_to_attr(n)))
                        .find(|f| f.0 == name)
                })
                .flatten()
        );

        self.arena_two
            .get_children(&id)
            .map(|nodes| {
                nodes
                    .iter()
                    .map(|n| (n.payload.get_file_name(), self.map_kube_file_to_attr(n)))
                    .find(|f| f.0 == name)
            })
            .flatten()
    }

    pub fn get_file(&self, inode: u64) -> Option<(String, FileAttr)> {
        let id = NodeId::new(inode);
        match self.arena_two.get(&id) {
            Some(node) => Some((
                node.payload.get_file_name(),
                self.map_kube_file_to_attr(node),
            )),
            _ => None,
        }
    }

    pub fn get_kube_manifest(&self, inode: u64) -> anyhow::Result<String> {
        let id = NodeId::new(inode);

        if let Some(node) = self.arena_two.get(&id) {
            if let KubeFileNode::ResourceFile(m) = &node.payload {
                // let uuid = m.uuid;
                // TODO: Grab the yaml or json contents of the k8s resource
                return Ok("".into());
            } else {
                return Err(anyhow::Error::msg("Not a manifest file!"));
            }
        }

        Err(anyhow::Error::msg("Inode not found!"))
    }

    pub fn list_files_two(&mut self, inode: u64) -> Option<Vec<(String, FileAttr)>> {
        self.sync_leafs_for_inode(inode);

        let result: Option<Vec<(String, FileAttr)>> = self
            .arena_two
            .get_children(&NodeId::new(inode))
            .map(|nodes| {
                nodes
                    .iter()
                    .map(|n| (n.payload.get_file_name(), self.map_kube_file_to_attr(n)))
                    .collect()
            });

        println!("Files for {} are {:?}", inode, result);
        result
    }

    fn map_kube_file_to_attr(&self, node: &Node<KubeFileNode>) -> FileAttr {
        match &node.payload {
            KubeFileNode::Virtual(_)
            | KubeFileNode::Context(_)
            | KubeFileNode::ApiResourceDirectory(_)
            | KubeFileNode::ResourceDirectory(_) => FileAttr {
                ino: node.id.clone().into(),
                size: 0,
                blocks: 0,
                atime: self.startup,
                mtime: self.startup,
                ctime: self.startup,
                crtime: self.startup,
                kind: FileType::Directory,
                perm: 0o755,
                nlink: 1,
                uid: 1000,
                gid: 1000,
                rdev: 0,
                blksize: 512,
                flags: 0,
            },
            KubeFileNode::ResourceFile(file) => FileAttr {
                ino: node.id.clone().into(),
                size: 10000,
                blocks: 0,
                atime: self.startup,
                mtime: self.startup,
                ctime: self.startup,
                crtime: self.startup,
                kind: FileType::RegularFile,
                perm: 0o655,
                nlink: 1,
                uid: 1000,
                gid: 1000,
                rdev: 0,
                blksize: 512,
                flags: 0,
            },
            KubeFileNode::ClusterInfoFile | KubeFileNode::LogFile(_) => FileAttr {
                ino: node.id.clone().into(),
                size: 10000,
                blocks: 0,
                atime: self.startup,
                mtime: self.startup,
                ctime: self.startup,
                crtime: self.startup,
                kind: FileType::RegularFile,
                perm: 0o655,
                nlink: 1,
                uid: 10000,
                gid: 1000,
                rdev: 0,
                blksize: 512,
                flags: 0,
            },
        }
    }

    fn get_leafs_for_node(&self, node: &Node<KubeFileNode>) -> Vec<KubeFileNode> {
        match &node.payload {
            KubeFileNode::Context(_) => {
                let mut items = Vec::new();
                items.push(KubeFileNode::Virtual(String::from(".")));
                items.push(KubeFileNode::Virtual(String::from("..")));

                items.push(KubeFileNode::ClusterInfoFile);

                let namespaces = self.kube_client.list_namespaces().unwrap();

                for namespace in namespaces {
                    let uuid = namespace.uid().unwrap();
                    let name = namespace.name_any();
                    let n = KubeResourceNode::new(uuid.as_str(), name.as_str(), "Namespace".into());
                    items.push(KubeFileNode::ResourceDirectory(n.clone()));
                    items.push(KubeFileNode::ResourceFile(n));
                }

                items
            }
            KubeFileNode::ResourceDirectory(dir) => {
                let mut items = Vec::new();
                items.push(KubeFileNode::Virtual(String::from(".")));
                items.push(KubeFileNode::Virtual(String::from("..")));

                println!("Rendering Resource Directory {:?}", dir);

                match dir.kind.as_str() {
                    "Namespace" => {
                        let scoped: Vec<&ApiResource> = self
                            .api_resources
                            .iter()
                            .filter(|(_, c)| c.scope == Scope::Namespaced)
                            .map(|(a, _)| a)
                            .collect();

                        for api in scoped {
                            let n = KubeApiResourceNode {
                                namespace: Some(dir.name.clone()),
                                group: api.group.clone(),
                                kind: api.kind.clone(),
                                version: api.version.clone(),
                                plural: api.plural.clone(),
                            };
                            items.push(KubeFileNode::ApiResourceDirectory(n));
                        }
                    }
                    _ => {}
                }
                items
            }
            KubeFileNode::ApiResourceDirectory(api) => {
                let mut items = Vec::new();
                items.push(KubeFileNode::Virtual(String::from(".")));
                items.push(KubeFileNode::Virtual(String::from("..")));

                let (resource, _) = self
                    .api_resources
                    .iter()
                    .find(|(a, _)| {
                        a.group == api.group && a.kind == api.kind && a.version == api.version
                    })
                    .unwrap();

                let objs = self
                    .kube_client
                    .list_resources(api.namespace.clone().unwrap().as_str(), resource)
                    .unwrap();

                for obj in &objs {
                    items.push(KubeFileNode::ResourceFile(KubeResourceNode::from(
                        obj, &api.kind,
                    )));
                }

                items
            }
            _ => Vec::new(),
        }
    }

    fn sync_leafs_for_inode(&mut self, inode: u64) {
        println!("syncing leafs for node {}", inode);
        let id = NodeId::new(inode);
        let node = self.arena_two.get(&id);

        if node.is_none() {
            return;
        }

        let node = node.unwrap();

        let new_leaf = self.get_leafs_for_node(node);

        let old_leaf: Vec<(NodeId, KubeFileNode)> = self
            .arena_two
            .get_children(&id)
            .unwrap_or_default()
            .iter()
            .map(|n| (n.id.clone(), n.payload.clone()))
            .collect();

        let add_nodes: Vec<KubeFileNode> = new_leaf
            .iter()
            .filter(|n| !old_leaf.iter().any(|(_, o)| o == *n))
            .map(|n| n.clone())
            .collect();

        let remove_inodes: Vec<NodeId> = old_leaf
            .iter()
            .filter(|(_, n)| !new_leaf.iter().any(|o| o == n))
            .map(|(i, _)| i.clone())
            .collect();

        for inode in remove_inodes {
            self.arena_two.delete_node(inode);
        }

        for node in add_nodes {
            self.arena_two.add(node, Some(id.clone()));
        }
    }
}
