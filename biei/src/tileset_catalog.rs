//! Tileset URL resolution for request-local addlayer sources.

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TilesetCatalog {
    template: String,
}

impl TilesetCatalog {
    pub fn new(template: impl Into<String>) -> Self {
        Self {
            template: template.into(),
        }
    }

    pub fn resolve_url(&self, tileset_id: &str) -> String {
        self.template.replace("{tileset_id}", tileset_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolves_tileset_id_by_raw_replacement() {
        let catalog = TilesetCatalog::new("https://tiles.example.test/{tileset_id}/tileset.json");

        assert_eq!(
            catalog.resolve_url("analysis/hrnowc/sample"),
            "https://tiles.example.test/analysis/hrnowc/sample/tileset.json"
        );
    }
}
