//! Kubernetes Event recording.
//!
//! Records `events.k8s.io/v1` Events against any resource so that
//! `kubectl describe` shows operator activity under the **Events** section.
//!
//! # Quick start
//!
//! ```no_run
//! use koprs::error::KubeGenericError;
//! use koprs::events::{record_event, EventType};
//! use kube::Client;
//! use k8s_openapi::api::core::v1::ConfigMap;
//!
//! # async fn example(client: Client, resource: &ConfigMap) -> Result<(), KubeGenericError> {
//! record_event(
//!     client,
//!     resource,
//!     EventType::Normal,
//!     "Sync",
//!     "Synced",
//!     "All child resources are up to date",
//!     "my-operator",
//! ).await?;
//! # Ok(())
//! # }
//! ```

use k8s_openapi::api::core::v1::ObjectReference;
use k8s_openapi::api::events::v1::Event;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{MicroTime, ObjectMeta};
use kube::{Api, Client};
use tracing::info;

use crate::error::Result;
use crate::traits::KubeResource;

// ---------------------------------------------------------------------------
// EventType
// ---------------------------------------------------------------------------

/// Kubernetes event severity.
///
/// Maps directly to the Kubernetes `type` field on `events.k8s.io/v1` Events.
/// `Normal` is for routine operator activity; `Warning` is for degraded or
/// unexpected conditions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventType {
    /// Routine informational event (e.g. resource synced, deployment updated).
    Normal,
    /// Something unexpected occurred and the operator may not be able to
    /// proceed without intervention.
    Warning,
}

impl EventType {
    fn as_str(self) -> &'static str {
        match self {
            Self::Normal => "Normal",
            Self::Warning => "Warning",
        }
    }
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Record a Kubernetes `events.k8s.io/v1` Event against `resource`.
///
/// The event is created in the same namespace as `resource`, or `default` for
/// cluster-scoped resources.
///
/// | Parameter | Kubernetes field | Guidance |
/// |---|---|---|
/// | `event_type` | `type` | `Normal` or `Warning` |
/// | `action` | `action` | what the operator did, e.g. `"Sync"` |
/// | `reason` | `reason` | camelCase cause, e.g. `"Synced"`, `"Failed"` |
/// | `note` | `note` | human-readable sentence shown by `kubectl describe` |
/// | `reporting_controller` | `reportingController` | operator name, e.g. `"my-operator"` |
///
/// # Examples
///
/// ```no_run
/// use koprs::error::KubeGenericError;
/// use koprs::events::{record_event, EventType};
/// use kube::Client;
/// use k8s_openapi::api::core::v1::ConfigMap;
///
/// # async fn example(client: Client, resource: &ConfigMap) -> Result<(), KubeGenericError> {
/// record_event(
///     client,
///     resource,
///     EventType::Warning,
///     "Sync",
///     "SyncFailed",
///     "Failed to apply child ConfigMap: permission denied",
///     "my-operator",
/// ).await?;
/// # Ok(())
/// # }
/// ```
pub async fn record_event<T>(
    client: Client,
    resource: &T,
    event_type: EventType,
    action: impl Into<String>,
    reason: impl Into<String>,
    note: impl Into<String>,
    reporting_controller: impl Into<String>,
) -> Result<()>
where
    T: KubeResource,
{
    let meta = resource.meta();
    let resource_name = meta.name.as_deref().unwrap_or("unknown");
    let namespace = meta.namespace.as_deref().unwrap_or("default");

    let now = k8s_openapi::jiff::Timestamp::now();
    let event_name = format!("{}.{:x}", resource_name, now.as_nanosecond() as u64);

    let reporting_instance = std::env::var("POD_NAME")
        .or_else(|_| std::env::var("HOSTNAME"))
        .unwrap_or_else(|_| "unknown".to_string());

    let action = action.into();
    let reason = reason.into();
    let type_str = event_type.as_str();

    info!(
        resource = %resource_name,
        %namespace,
        %reason,
        event_type = type_str,
        "Recording event"
    );

    let regarding = ObjectReference {
        api_version: Some(T::api_version(&()).to_string()),
        kind: Some(T::kind(&()).to_string()),
        name: meta.name.clone(),
        namespace: meta.namespace.clone(),
        uid: meta.uid.clone(),
        resource_version: meta.resource_version.clone(),
        ..Default::default()
    };

    let event = Event {
        metadata: ObjectMeta {
            name: Some(event_name),
            namespace: Some(namespace.to_string()),
            ..Default::default()
        },
        event_time: Some(MicroTime(now)),
        regarding: Some(regarding),
        action: Some(action),
        reason: Some(reason),
        note: Some(note.into()),
        type_: Some(type_str.to_string()),
        reporting_controller: Some(reporting_controller.into()),
        reporting_instance: Some(reporting_instance),
        ..Default::default()
    };

    let api: Api<Event> = Api::namespaced(client, namespace);
    api.create(&kube::api::PostParams::default(), &event)
        .await?;

    Ok(())
}
