use k8s_openapi::apimachinery::pkg::version::Info;
use kube::{
    api::ListParams,
    core::{DynamicObject, GroupVersionKind, TypeMeta},
    discovery::{ApiCapabilities, ApiResource},
    Api, Client, Discovery,
};
use mini_moka::sync::Cache;
use tokio::runtime::Runtime;

pub struct KubeClient {
    runtime: Runtime,
    client: Client,

    cache: Cache<String, Vec<DynamicObject>>, // cache: Cell<HashMap<String, Vec<DynamicObject>>>,
}

impl KubeClient {
    pub fn new() -> Self {
        let runtime = tokio::runtime::Runtime::new().expect("Unable to create a runtime");
        let client = runtime
            .block_on(async { Client::try_default().await })
            .unwrap();
        KubeClient {
            runtime,
            client,
            cache: Cache::builder().build(),
        }
    }

    pub fn cluster_info(&self) -> anyhow::Result<Info> {
        let info = self
            .runtime
            .block_on(async { self.client.apiserver_version().await })?;

        Ok(info)
    }

    pub fn discover_api_resources(
        &self,
        filter: Option<&Vec<&str>>,
        operations: Option<&Vec<&str>>,
    ) -> anyhow::Result<Vec<(ApiResource, ApiCapabilities)>> {
        let default = Vec::new();
        let filter = filter.unwrap_or(&default);
        let discovery = self.runtime.block_on(async {
            let discovery = Discovery::new(self.client.clone())
                .filter(filter)
                .run()
                .await;
            discovery
        })?;

        let operations = operations.unwrap_or(&default);

        Ok(discovery
            .groups()
            .flat_map(|g| g.recommended_resources())
            .filter(|(_, c)| operations.iter().all(|o| c.supports_operation(o)))
            .collect())
    }

    pub fn list_namespaces(&self) -> anyhow::Result<Vec<DynamicObject>> {
        let key: String = "namespaces".into();
        if let Some(ns) = self.cache.get(&key) {
            return Ok(ns);
        }

        self.runtime.block_on(async {
            let resource = ApiResource::from_gvk(&GroupVersionKind {
                group: String::from(""),
                version: String::from("v1"),
                kind: String::from("Namespace"),
            });
            let namespace: Api<DynamicObject> = Api::all_with(self.client.clone(), &resource);

            let all = namespace.list(&ListParams::default()).await?;

            let namespaces: Vec<DynamicObject> = all
                .items
                .iter()
                .map(|n| {
                    let mut obj = n.clone();
                    obj.types = Some(TypeMeta {
                        api_version: resource.version.clone(),
                        kind: resource.kind.clone(),
                    });
                    obj
                })
                .collect();

            self.cache.insert(key, namespaces.clone());
            Ok(namespaces)
        })
    }

    pub fn list_resources(
        &self,
        namespace: &str,
        resource: &ApiResource,
    ) -> anyhow::Result<Vec<DynamicObject>> {
        let key = format!("{}/{}", namespace, resource.kind.to_lowercase());

        if let Some(objs) = self.cache.get(&key) {
            return Ok(objs);
        }

        let resources = self.runtime.block_on(async {
            let api: Api<DynamicObject> =
                Api::namespaced_with(self.client.clone(), namespace, resource);
            api.list(&ListParams::default()).await
        })?;

        let objs: Vec<DynamicObject> = resources.items.iter().map(|p| p.clone()).collect();

        self.cache.insert(key, objs.clone());

        Ok(objs)
    }
}
