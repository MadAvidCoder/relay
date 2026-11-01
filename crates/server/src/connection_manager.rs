use relay_common::Connection;
use std::collections::HashMap;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct ActiveConnection {
    pub connection: Connection,
    pub sender: tokio::sync::mpsc::Sender<relay_common::ServerProtocol>,
}

#[derive(Debug, Clone)]
pub struct ConnectionManager {
    connections: HashMap<Uuid, ActiveConnection>,
}
impl ConnectionManager {
    pub fn new() -> Self {
        ConnectionManager {
            connections: HashMap::new(),
        }
    }

    pub fn register(&mut self, user_id: Uuid, connection: ActiveConnection) -> Result<(), String> {
        if self.connections.contains_key(&user_id) {
            return Err(
                "User is already connected. Multiple connections are not yet supported.".into(),
            );
        }
        self.connections.insert(user_id, connection);
        Ok(())
    }

    pub fn remove(&mut self, user_id: Uuid, connection_id: Uuid) -> Result<(), String> {
        if self
            .connections
            .get(&user_id)
            .and_then(|f| Some(&f.connection.id))
            != Some(&connection_id)
        {
            return Err("Failed to remove user connection. Connection does not match.".into());
        }
        if self.connections.remove(&user_id).is_none() {
            return Err("Failed to remove user connection. User is not connected.".into());
        }
        Ok(())
    }

    pub fn get(&self, user_id: Uuid) -> Option<&ActiveConnection> {
        self.connections.get(&user_id)
    }

    pub fn is_online(&self, user_id: Uuid) -> bool {
        self.connections.contains_key(&user_id)
    }
}
